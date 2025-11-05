// Example Go code to create an AWS S3 client using the AWS SDK for Go v2
// and list objects in the NASA POWER public dataset bucket.
package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {

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