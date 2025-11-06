// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package s3

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/endpoints"
)

const (
	awsTestRegion         = "us-west-2"
	awsTestBucket         = "nasa-power"
	minioTestEndpointURL  = "http://localhost:9000"
	minioTestAccessKey    = "minioadmin"
	minioTestSecretKey    = "minioadmin"
	minioTestSessionToken = ""
	minioTestRegion       = "us-east-1"
	minioTestUsePathStyle = true
)

var minioTestBuckets = []string{"dts-test-source-bucket", "dts-test-dest-bucket"}

// connect to the Minio test server, create a test bucket, and populate it with some
// test data
func setup(t *testing.T) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		t.Fatalf("unable to load SDK config, %v", err)
	}

	// override config for Minio
	s3Client := awsS3.NewFromConfig(cfg, func(o *awsS3.Options) {
		baseEndpoint := minioTestEndpointURL
		o.BaseEndpoint = &baseEndpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			minioTestAccessKey,
			minioTestSecretKey,
			minioTestSessionToken,
		)
		o.Region = minioTestRegion
		o.UsePathStyle = minioTestUsePathStyle
	})

	// create the test buckets if they don't exist
	existingBuckets, err := s3Client.ListBuckets(context.TODO(), &awsS3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("unable to list buckets, %v", err)
	}

	for _, minioTestBucket := range minioTestBuckets {
		bucketExists := false
		for _, bucket := range existingBuckets.Buckets {
			if aws.ToString(bucket.Name) == minioTestBucket {
				bucketExists = true
				break
			}
		}

		if !bucketExists {
			_, err = s3Client.CreateBucket(context.TODO(), &awsS3.CreateBucketInput{
				Bucket: aws.String(minioTestBucket),
			})
			if err != nil {
				t.Fatalf("unable to create test bucket, %v", err)
			}
		}
	}

	// put a couple test files into the source bucket
	testFiles := map[string]string{
		"testfile1.txt":      "This is the content of test file 1.",
		"testfile2.txt":      "This is the content of test file 2.",
		"dir1/testfile3.txt": "This is the content of test file 3 in dir1.",
	}

	for key, content := range testFiles {
		fileLength := int64(len(content))
		_, err = s3Client.PutObject(context.TODO(), &awsS3.PutObjectInput{
			Bucket:        aws.String(minioTestBuckets[0]),
			Key:           aws.String(key),
			Body:          bytes.NewReader([]byte(content)),
			ContentLength: &fileLength,
		})
		if err != nil {
			t.Fatalf("unable to put test file %s, %v", key, err)
		}
	}
}

func TestNewAWSS3Endpoint(t *testing.T) {
	assert := assert.New(t)
	cfg := EndpointConfig{
		Region: awsTestRegion,
	}
	awsEndpoint, err := NewEndpoint(awsTestBucket, uuid.New(), cfg)
	assert.NotNil(awsEndpoint)
	assert.Nil(err)
	assert.Equal(awsTestBucket+"/", awsEndpoint.RootDir())
	assert.Equal("s3: "+awsTestBucket+".s3."+awsTestRegion+".amazonaws.com", awsEndpoint.Provider())
	staged, err := awsEndpoint.FilesStaged([]any{})
	assert.True(staged)
	assert.Nil(err)
	staged, err = awsEndpoint.FilesStaged([]any{map[string]any{
		"id":   "example",
		"path": "some/nonexistent/file.txt",
	}})
	assert.False(staged)
	assert.Nil(err)
	staged, err = awsEndpoint.FilesStaged([]any{map[string]any{
		"id":   "NASA POWER License",
		"path": "LICENSE.txt",
	}})
	assert.True(staged)
	assert.Nil(err)
}

func TestNewMinioS3Endpoint(t *testing.T) {
	assert := assert.New(t)
	cfg := EndpointConfig{
		BaseURL:      minioTestEndpointURL,
		AccessKeyID:  minioTestAccessKey,
		SecretKey:    minioTestSecretKey,
		SessionToken: minioTestSessionToken,
		Region:       minioTestRegion,
		PathStyle:    minioTestUsePathStyle,
	}
	minioEndpoint, err := NewEndpoint(minioTestBuckets[0], uuid.New(), cfg)
	assert.NotNil(minioEndpoint)
	assert.Nil(err)
	assert.Equal(minioTestBuckets[0]+"/", minioEndpoint.RootDir())
	expectedProvider := "s3: " + minioTestEndpointURL + "/" + minioTestBuckets[0]
	assert.Equal(expectedProvider, minioEndpoint.Provider())

	// test FilesStaged with existing files
	descriptors := make([]any, 0)
	testFiles := []string{
		"testfile1.txt",
		"testfile2.txt",
		"dir1/testfile3.txt",
	}
	for _, filePath := range testFiles {
		d := map[string]any{ // descriptor
			"id":   filePath,
			"path": filePath,
		}
		descriptors = append(descriptors, d)
	}
	staged, err := minioEndpoint.FilesStaged(descriptors)
	assert.True(staged)
	assert.Nil(err)

	// test FilesStaged with one nonexistent file
	nonexistent := map[string]any{ // descriptor
		"id":   "nonexistent",
		"path": "nonexistent/file.txt",
	}
	assert.Nil(err)
	descriptors = append(descriptors, nonexistent)
	staged, err = minioEndpoint.FilesStaged(descriptors)
	assert.False(staged)
	assert.Nil(err)

	// test FilesStaged with an empty list
	staged, err = minioEndpoint.FilesStaged([]any{})
	assert.True(staged)
	assert.Nil(err)

	// test FilesStaged with invalid descriptor
	invalidDescriptor := "this is not a valid descriptor"
	staged, err = minioEndpoint.FilesStaged([]any{invalidDescriptor})
	assert.False(staged)
	assert.NotNil(err)

	// test FilesStaged with missing path in descriptor
	missingPathDescriptor := map[string]any{
		"id": "missing-path",
	}
	staged, err = minioEndpoint.FilesStaged([]any{missingPathDescriptor})
	assert.False(staged)
	assert.NotNil(err)

	// test FilesStaged with non-string path in descriptor
	nonStringPathDescriptor := map[string]any{
		"id":   "non-string-path",
		"path": 12345,
	}
	staged, err = minioEndpoint.FilesStaged([]any{nonStringPathDescriptor})
	assert.False(staged)
	assert.NotNil(err)

	// cancels are not supported
	err = minioEndpoint.Cancel(uuid.New())
	assert.NotNil(err)

	// status for unknown transfer ID
	_, err = minioEndpoint.Status(uuid.New())
	assert.NotNil(err)
}

// Test transfer from AWS to Minio
func TestAWSToMinioTransfer(t *testing.T) {
	assert := assert.New(t)
	// create AWS endpoint
	awsCfg := EndpointConfig{
		Region: awsTestRegion,
	}
	awsEndpoint, err := NewEndpoint(awsTestBucket, uuid.New(), awsCfg)
	assert.NotNil(awsEndpoint)
	assert.Nil(err)

	// create Minio endpoint
	minioCfg := EndpointConfig{
		BaseURL:      minioTestEndpointURL,
		AccessKeyID:  minioTestAccessKey,
		SecretKey:    minioTestSecretKey,
		SessionToken: minioTestSessionToken,
		Region:       minioTestRegion,
		PathStyle:    minioTestUsePathStyle,
	}
	minioEndpoint, err := NewEndpoint(minioTestBuckets[1], uuid.New(), minioCfg)
	assert.NotNil(minioEndpoint)
	assert.Nil(err)

	// perform transfer of LICENSE.txt file
	filesToTransfer := []endpoints.FileTransfer{
		{
			SourcePath:      "LICENSE.txt",
			DestinationPath: "LICENSE_copied.txt",
		},
	}
	transferID, err := awsEndpoint.Transfer(*minioEndpoint, filesToTransfer)
	assert.NotEqual(uuid.Nil, transferID)
	assert.Nil(err)

	// check transfer status
	status, err := awsEndpoint.Status(transferID)
	assert.Nil(err)
	assert.Equal(endpoints.TransferStatusSucceeded, status.Code)

	// verify that the file exists in the Minio bucket
	exists, err := minioEndpoint.fileExists("LICENSE_copied.txt")
	assert.True(exists)
	assert.Nil(err)

	// check that there is one transfer in the AWS endpoint
	transfers, err := awsEndpoint.Transfers()
	assert.Nil(err)
	assert.Equal(1, len(transfers))
	assert.Equal(transferID, transfers[0])
}

// Test transfer from Minio to Minio
func TestMinioToMinioTransfer(t *testing.T) {
	assert := assert.New(t)
	// create Minio endpoint configuration
	minioCfg := EndpointConfig{
		BaseURL:      minioTestEndpointURL,
		AccessKeyID:  minioTestAccessKey,
		SecretKey:    minioTestSecretKey,
		SessionToken: minioTestSessionToken,
		Region:       minioTestRegion,
		PathStyle:    minioTestUsePathStyle,
	}
	minioSrcEndpoint, err := NewEndpoint(minioTestBuckets[0], uuid.New(), minioCfg)
	assert.NotNil(minioSrcEndpoint)
	assert.Nil(err)

	minioDestEndpoint, err := NewEndpoint(minioTestBuckets[1], uuid.New(), minioCfg)
	assert.NotNil(minioDestEndpoint)
	assert.Nil(err)

	// perform transfer of testfile1.txt and testfile2.txt
	filesToTransfer := []endpoints.FileTransfer{
		{
			SourcePath:      "testfile1.txt",
			DestinationPath: "testfile1_copied.txt",
		},
		{
			SourcePath:      "testfile2.txt",
			DestinationPath: "testfile2_copied.txt",
		},
	}
	transferID, err := minioSrcEndpoint.Transfer(*minioDestEndpoint, filesToTransfer)
	assert.NotEqual(uuid.Nil, transferID)
	assert.Nil(err)

	// check transfer status
	status, err := minioSrcEndpoint.Status(transferID)
	assert.Nil(err)
	assert.Equal(endpoints.TransferStatusSucceeded, status.Code)
	assert.Equal(2, status.NumFiles)
	assert.Equal(2, status.NumFilesTransferred)
	assert.Equal(0, status.NumFilesSkipped)

	// verify that the files exist in the destination Minio bucket
	exists, err := minioDestEndpoint.fileExists("testfile1_copied.txt")
	assert.True(exists)
	assert.Nil(err)

	exists, err = minioDestEndpoint.fileExists("testfile2_copied.txt")
	assert.True(exists)
	assert.Nil(err)

	// try to transfer a nonexistent file
	nonexistentFileTransfer := []endpoints.FileTransfer{
		{
			SourcePath:      "nonexistent.txt",
			DestinationPath: "nonexistent_copied.txt",
		},
		{
			SourcePath:      "testfile1.txt",
			DestinationPath: "testfile1_copied_again.txt",
		},
	}
	failedTransferID, err := minioSrcEndpoint.Transfer(*minioDestEndpoint, nonexistentFileTransfer)
	assert.NotEqual(uuid.Nil, failedTransferID)
	assert.Nil(err)

	// check transfer status
	failedStatus, err := minioSrcEndpoint.Status(failedTransferID)
	assert.Nil(err)
	assert.Equal(endpoints.TransferStatusFailed, failedStatus.Code)
	assert.Equal(2, failedStatus.NumFiles)
	assert.Equal(1, failedStatus.NumFilesTransferred)
	assert.Equal(1, failedStatus.NumFilesSkipped)

	// verify that the existing file was transferred
	exists, err = minioDestEndpoint.fileExists("testfile1_copied_again.txt")
	assert.True(exists)
	assert.Nil(err)

	// verify that the nonexistent file was not transferred
	exists, err = minioDestEndpoint.fileExists("nonexistent_copied.txt")
	assert.False(exists)
	assert.Nil(err)

	// check that there are two transfers in the source Minio endpoint
	transfers, err := minioSrcEndpoint.Transfers()
	assert.Nil(err)
	assert.Equal(2, len(transfers))
	assert.Equal(transferID, transfers[0])
	assert.Equal(failedTransferID, transfers[1])
}

func TestMain(m *testing.M) {
	setup(&testing.T{})
	os.Exit(m.Run())
}
