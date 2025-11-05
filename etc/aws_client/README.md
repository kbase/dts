# AWS Client Demo

This directory contains a simple Go application that demonstrates how to interact with AWS S3 using the AWS SDK for Go v2. It includes examples of listing buckets, creating a bucket, and uploading an object.

The example runs using AWS and a local Minio instance, which must be running locally. To start the Minio instance run:

```bash
docker run -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"
```
(Note that MinIO is no longer publishing built images, and there is a [lot of drama](https://github.com/minio/minio/issues/21647#issuecomment-3418675115)) 

In a separate bash terminal, from this foler run:
```bash
go run ./...
```

You should first see output from querying the NASA POWER AWS S3 bucket (a list of files, and the contents of the LICENSE.txt file), and then see output from creating a reading a simple text file in Minio.
