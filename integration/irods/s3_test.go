// Copyright (c) 2025 The KBase Project and its Contributors
// Copyright (c) 2025 Cohere Consulting, LLC
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
package integration

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	minioTestEndpointURL  = "http://localhost:9000"
	minioTestAccessKey    = "minioadmin" // must match MinIO server config
	minioTestSecretKey    = "minioadmin" // must match MinIO server config
	minioTestSessionToken = ""
	minioTestRegion       = "us-east-1"
	minioTestUsePathStyle = true
	minioDisableHTTPS     = false
	irodsTestEndpointURL  = "http://localhost:9010"
	irodsTestAccessKey    = "s3_access_key" // must match iRODS S3 config
	irodsTestSecretKey    = "s3_secret_key" // must match iRODS S3 config
	irodsTestSessionToken = ""
	irodsTestRegion       = "us-east-1"
	irodsTestUsePathStyle = true
	irodsDisableHTTPS     = true
)

var minioTestBuckets = []string{
	"test-bucket-integration-irods-foo",
}

var irodsTestBuckets = []string{
	"test-bucket-integration-irods-bar",
	"test-bucket-integration-irods-baz",
}

var testFiles = map[string]map[string]string{
	"test-bucket-integration-irods-foo": {
		"file1.txt":              "This is file 1 in bucket foo.",
		"file2.txt":              "This is file 2 in bucket foo.",
		"dir1/file3.txt":         "This is file 3 in dir1 of bucket foo.",
		"dir1/file4.txt":         "This is file 4 in dir1 of bucket foo.",
		"dir2/file5.txt":         "This is file 5 in dir2 of bucket foo.",
		"dir2/subdir1/file6.txt": "This is file 6 in subdir1 of dir2 of bucket foo.",
	},
	"test-bucket-integration-irods-bar": {
		"file3.txt":      "This is file 3 in bucket bar.",
		"file4.txt":      "This is file 4 in bucket bar.",
		"dir1/file5.txt": "This is file 5 in dir1 of bucket bar.",
		"dir1/file6.txt": "This is file 6 in dir1 of bucket bar.",
	},
	"test-bucket-integration-irods-baz": {
		"file5.txt":               "This is file 5 in bucket baz.",
		"file6.txt":               "This is file 6 in bucket baz.",
		"dir1/file7.txt":          "This is file 7 in dir1 of bucket baz.",
		"dir1/file8.txt":          "This is file 8 in dir1 of bucket baz.",
		"dir2/file9.txt":          "This is file 9 in dir2 of bucket baz.",
		"dir2/subdir1/file10.txt": "This is file 10 in subdir1 of dir2 of bucket baz.",
	},
}

func ResetMinioTestBuckets() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}

	// setup S3 client for MinIO
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		baseEndpoint := minioTestEndpointURL
		o.BaseEndpoint = &baseEndpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			minioTestAccessKey,
			minioTestSecretKey,
			minioTestSessionToken,
		)
		o.Region = minioTestRegion
		o.UsePathStyle = minioTestUsePathStyle
		o.EndpointOptions.DisableHTTPS = minioDisableHTTPS
	})

	// create the test buckets if they don't exist
	existingBuckets, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		panic(fmt.Sprintf("unable to list buckets, %v", err))
	}
	for _, bucketName := range minioTestBuckets {
		bucketExists := false
		for _, existingBucket := range existingBuckets.Buckets {
			if aws.ToString(existingBucket.Name) == bucketName {
				bucketExists = true
				break
			}
		}
		if !bucketExists {
			_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				panic(fmt.Sprintf("unable to create bucket %s, %v", bucketName, err))
			}
		} else {
			// empty the bucket
			listOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				panic(fmt.Sprintf("unable to list objects in bucket %s, %v", bucketName, err))
			}
			for _, object := range listOutput.Contents {
				_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    object.Key,
				})
				if err != nil {
					panic(fmt.Sprintf("unable to delete object %s in bucket %s, %v", aws.ToString(object.Key), bucketName, err))
				}
			}
		}
		// upload the test files
		for filePath, fileContent := range testFiles[bucketName] {
			fileLength := int64(len(fileContent))
			_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:        aws.String(bucketName),
				Key:           aws.String(filePath),
				Body:          bytes.NewReader([]byte(fileContent)),
				ContentLength: &fileLength,
			})
			if err != nil {
				panic(fmt.Sprintf("unable to upload file %s to bucket %s, %v", filePath, bucketName, err))
			}
		}
	}
}

func ResetIrodsTestBuckets() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}

	// setup S3 client for iRODS
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		baseEndpoint := irodsTestEndpointURL
		o.BaseEndpoint = &baseEndpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			irodsTestAccessKey,
			irodsTestSecretKey,
			irodsTestSessionToken,
		)
		o.Region = irodsTestRegion
		o.UsePathStyle = irodsTestUsePathStyle
		o.EndpointOptions.DisableHTTPS = irodsDisableHTTPS
	})

	// create the test buckets
	for _, bucketName := range irodsTestBuckets {
		// ignore error if bucket already exists
		s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})

		// empty the bucket
		listOutput, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			panic(fmt.Sprintf("unable to list objects in bucket %s, %v", bucketName, err))
		}
		for _, object := range listOutput.Contents {
			_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    object.Key,
			})
			if err != nil {
				panic(fmt.Sprintf("unable to delete object %s in bucket %s, %v", aws.ToString(object.Key), bucketName, err))
			}
		}

		// upload the test files
		for filePath, fileContent := range testFiles[bucketName] {
			fileLength := int64(len(fileContent))
			_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:        aws.String(bucketName),
				Key:           aws.String(filePath),
				Body:          bytes.NewReader([]byte(fileContent)),
				ContentLength: &fileLength,
			})
			if err != nil {
				panic(fmt.Sprintf("unable to upload file %s to bucket %s, %v", filePath, bucketName, err))
			}
		}
	}
}
