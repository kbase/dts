// Example Go code to create an AWS S3 client using the AWS SDK for Go v2
// and list objects in the NASA POWER public dataset bucket.
package main

import (
	"bytes"
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func AwsClient() {

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-west-2"),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	// Example operation: List objects in a bucket
	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("nasa-power"),
	})
	if err != nil {
		log.Fatalf("unable to list objects, %v", err)
	}

	for _, item := range output.Contents {
		log.Printf("Name: %s, Size: %d", aws.ToString(item.Key), aws.ToInt64(item.Size))
	}

	// check for the presence of a LICENSE.txt file
	output, err = s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("nasa-power"),
		Prefix: aws.String("LICENSE.txt"),
	})
	if err != nil {
		log.Fatalf("unable to list objects, %v", err)
	}
	if len(output.Contents) == 0 {
		log.Fatalf("LICENSE.txt file not found in bucket")
	}
	log.Println("LICENSE.txt file found in bucket")

	// Copy out the LICENSE file
	getObjOutput, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("nasa-power"),
		Key:    aws.String("LICENSE.txt"),
	})
	if err != nil {
		log.Fatalf("unable to get object, %v", err)
	}
	defer getObjOutput.Body.Close()

	// Print the contents of the LICENSE file
	buf := make([]byte, 1024)
	for {
		n, err := getObjOutput.Body.Read(buf)
		if err != nil && err.Error() != "EOF" {
			log.Fatalf("unable to read object body, %v", err)
		}
		if n == 0 {
			break
		}
		log.Print(string(buf[:n]))
	}
}

func MinioClient() {
	// Example Go code to create a MinIO client using the AWS SDK for Go v2
	// and list objects in a MinIO bucket.
	baseEndpoint := "http://localhost:9000"

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Override the endpoint to point to the local MinIO server
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &baseEndpoint
		o.Credentials = credentials.NewStaticCredentialsProvider(
			"minioadmin", // Access Key
			"minioadmin", // Secret Key
			"",           // Session Token
		)
		o.Region = "us-east-1"
		o.UsePathStyle = true
	})

    // Create a bucket if it doesn't exist
	buckets, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("unable to list buckets, %v", err)
	}

	bucketExists := false
	for _, bucket := range buckets.Buckets {
		if aws.ToString(bucket.Name) == "my-bucket" {
			bucketExists = true
			break
		}
	}

	if !bucketExists {
		log.Println("Bucket 'my-bucket' does not exist. Creating it.")
		_, err = s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket: aws.String("my-bucket"),
		})
		if err != nil {
			log.Fatalf("unable to create bucket, %v", err)
		}
	}

	// put foo.bar file into the bucket
	fileContent := "This is a test file for MinIO."
	fileLength := int64(len(fileContent))
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        aws.String("my-bucket"),
		Key:           aws.String("foo.bar"),
		Body:          bytes.NewReader([]byte(fileContent)),
		ContentLength: &fileLength,
	})
	if err != nil {
		log.Fatalf("unable to put object, %v", err)
	}

	// Example operation: List objects in a bucket
	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("my-bucket"),
	})
	if err != nil {
		log.Fatalf("unable to list objects, %v", err)
	}

	for _, item := range output.Contents {
		log.Printf("Name: %s, Size: %d", aws.ToString(item.Key), aws.ToInt64(item.Size))
	}

	// read the foo.bar file
	getObjOutput, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String("my-bucket"),
		Key:    aws.String("foo.bar"),
	})
	if err != nil {
		log.Fatalf("unable to get object, %v", err)
	}
	defer getObjOutput.Body.Close()

	// Print the contents of the foo.bar file
	buf := make([]byte, 1024)
	for {
		n, err := getObjOutput.Body.Read(buf)
		if err != nil && err.Error() != "EOF" {
			log.Fatalf("unable to read object body, %v", err)
		}
		if n == 0 {
			break
		}
		log.Print(string(buf[:n]))
	}
}

// For the Minio client, there needs to be a running Minio server
// Start one from a Docker image with:
// ```
// docker run -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"
// ```
func main() {
	AwsClient()
	MinioClient()
}