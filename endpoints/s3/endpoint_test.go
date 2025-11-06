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
)

const (
	awsTestRegion 		    = "us-west-2"
	awsTestBucket 		    = "nasa-power"
	minioTestEndpointURL    = "http://localhost:9000"
	minioTestAccessKey	    = "minioadmin"
	minioTestSecretKey	    = "minioadmin"
	minioTestSessionToken	= ""
	minioTestRegion		    = "us-east-1"
	minioTestUsePathStyle	= true
	minioTestBucket		    = "dts-test-bucket"
)

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

	// create the test bucket if it doesn't exist
	existingBuckets, err := s3Client.ListBuckets(context.TODO(), &awsS3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("unable to list buckets, %v", err)
	}

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

	// put a couple test files into the bucket
	testFiles := map[string]string{
		"testfile1.txt": "This is the content of test file 1.",
		"testfile2.txt": "This is the content of test file 2.",
	}

	for key, content := range testFiles {
		fileLength := int64(len(content))
		_, err = s3Client.PutObject(context.TODO(), &awsS3.PutObjectInput{
			Bucket: aws.String(minioTestBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
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
}

func TestNewMinioS3Endpoint(t *testing.T) {
	assert := assert.New(t)
	cfg := EndpointConfig{
		BaseURL:    minioTestEndpointURL,
		AccessKeyID: minioTestAccessKey,
		SecretKey:   minioTestSecretKey,
		SessionToken: minioTestSessionToken,
		Region:      minioTestRegion,
		PathStyle:   minioTestUsePathStyle,
	}
	minioEndpoint, err := NewEndpoint(minioTestBucket, uuid.New(), cfg)
	assert.NotNil(minioEndpoint)
	assert.Nil(err)
	assert.Equal(minioTestBucket+"/", minioEndpoint.RootDir())
	expectedProvider := "s3: " + minioTestEndpointURL + "/" + minioTestBucket
	assert.Equal(expectedProvider, minioEndpoint.Provider())
	staged, err := minioEndpoint.FilesStaged([]any{})
	assert.True(staged)
	assert.Nil(err)
}
	
func TestMain(m *testing.M) {
	setup(&testing.T{})
	os.Exit(m.Run())
}
