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

package s3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/kbase/dts/endpoints"
)

// This file implements an AWS S3 endpoint. It should be usable with any S3-compatible
// storage system, such as Minio.

// this type satisfies the endpoints.Endpoint interface for AWS S3 endpoints
type Endpoint struct {
	// bucket identifier
	Bucket string
	// AWS S3 client
	Client *awsS3.Client
	// AWS S3 downloader
	Downloader *manager.Downloader
	// AWS S3 uploader
	Uploader *manager.Uploader
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// Map of completed transfers
	TransfersMap map[uuid.UUID]endpoints.TransferStatus
}

type EndpointConfig struct {
	// AWS region
	Region string `yaml:"region"`
	// AWS access key ID (optional)
	AccessKeyID string `yaml:"access_key_id,omitempty"`
	// AWS secret key (optional)
	SecretKey string `yaml:"secret_key,omitempty"`
	// Session token (optional)
	SessionToken string `yaml:"session_token,omitempty"`
	// Base endpoint URL (optional)
	BaseURL string `yaml:"base_url,omitempty"`
	// Whether to use path-style addressing
	PathStyle bool `yaml:"path_style,omitempty"`
}

// creates a new S3 endpoint from the provided configuration information
func NewEndpoint(bucket string, id uuid.UUID, ecfg EndpointConfig) (*Endpoint, error) {

	var newEndpoint Endpoint

	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	// Create a client, overriding config values as needed
	newEndpoint.Client = awsS3.NewFromConfig(cfg, func(o *awsS3.Options) {
		if ecfg.BaseURL != "" {
			o.BaseEndpoint = &ecfg.BaseURL
		}
		if ecfg.AccessKeyID != "" || ecfg.SecretKey != "" || ecfg.SessionToken != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				ecfg.AccessKeyID,
				ecfg.SecretKey,
				ecfg.SessionToken,
			)
		} else {
			o.Credentials = aws.AnonymousCredentials{}
		}
		if ecfg.Region != "" {
			o.Region = ecfg.Region
		}
		o.UsePathStyle = ecfg.PathStyle
	})
	newEndpoint.Downloader = manager.NewDownloader(newEndpoint.Client)
	newEndpoint.Uploader = manager.NewUploader(newEndpoint.Client)
	newEndpoint.Bucket = bucket
	newEndpoint.Id = id
	newEndpoint.TransfersMap = make(map[uuid.UUID]endpoints.TransferStatus)

	return &newEndpoint, nil
}

func (e *Endpoint) Provider() string {
	region := ""
	if e.Client.Options().Region != "" {
		region = "." + e.Client.Options().Region
	}
	baseUrl := "s3" + region + ".amazonaws.com"
	if e.Client.Options().BaseEndpoint != nil {
		baseUrl = *e.Client.Options().BaseEndpoint
	}
	if e.Client.Options().UsePathStyle {
		return "s3: " + baseUrl + "/" + e.Bucket
	}
	return "s3: " + e.Bucket + "." + baseUrl
}

func (e *Endpoint) RootDir() string {
	return e.Bucket + "/"
}

func (e *Endpoint) FilesStaged(files []any) (bool, error) {
	staged := true
	for _, f := range files {
		descriptor, ok := f.(map[string]any)
		if !ok {
			return false, fmt.Errorf("invalid descriptor format")
		}
		pathVal, found := descriptor["path"]
		if !found {
			return false, fmt.Errorf("descriptor missing 'path' field")
		}
		pathStr, ok := pathVal.(string)
		if !ok {
			return false, fmt.Errorf("'path' field is not a string")
		}
		exists, err := e.fileExists(pathStr)
		if err != nil {
			return false, err
		}
		if !exists {
			staged = false
			break
		}
	}
	return staged, nil
}

func (e *Endpoint) Transfers() ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0)
	for id := range e.TransfersMap {
		ids = append(ids, id)
	}
	return ids, nil
}

func (e *Endpoint) Transfer(dst Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	numFiles := len(files)
	numTransferred := 0

	for _, file := range files {
		// get the object from the source endpoint
		buffer, _, err := e.getObject(file.SourcePath)
		if err != nil {
			// skip this file
			continue
		}

		// put the object into the destination endpoint
		err = dst.putObject(file.DestinationPath, bytes.NewReader(buffer.Bytes()))
		if err != nil {
			// skip this file
			continue
		}

		numTransferred++
	}

	// generate a transfer task ID
	taskId := uuid.New()

	if numTransferred != numFiles {
		e.TransfersMap[taskId] = endpoints.TransferStatus{
			Code:                endpoints.TransferStatusFailed,
			Message:             fmt.Sprintf("Transferred %d out of %d files", numTransferred, numFiles),
			NumFiles:            numFiles,
			NumFilesTransferred: numTransferred,
			NumFilesSkipped:     numFiles - numTransferred,
		}
	} else {
		e.TransfersMap[taskId] = endpoints.TransferStatus{
			Code:                endpoints.TransferStatusSucceeded,
			Message:             "All files transferred successfully",
			NumFiles:            numFiles,
			NumFilesTransferred: numTransferred,
			NumFilesSkipped:     0,
		}
	}

	return taskId, nil
}

func (e *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	status, found := e.TransfersMap[id]
	if found {
		return status, nil
	}
	return endpoints.TransferStatus{}, fmt.Errorf("unknown transfer ID %s", id.String())
}

func (e *Endpoint) Cancel(id uuid.UUID) error {
	return fmt.Errorf("transfer cancellation not supported for S3 endpoints")
}

//-----------
// Internals
//-----------

// returns whether the given file exists in the bucket
func (e *Endpoint) fileExists(key string) (bool, error) {
	contents, err := e.Client.ListObjectsV2(context.TODO(), &awsS3.ListObjectsV2Input{
		Bucket: aws.String(e.Bucket),
		Prefix: aws.String(key),
	})
	if err != nil {
		return false, err
	}
	if len(contents.Contents) == 0 {
		return false, nil
	}
	return true, nil
}

// gets an object from the bucket
func (e *Endpoint) getObject(key string) (*manager.WriteAtBuffer, int64, error) {
	buffer := manager.NewWriteAtBuffer([]byte{})
	numBytes, err := e.Downloader.Download(context.TODO(), buffer, &awsS3.GetObjectInput{
		Bucket: aws.String(e.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, err
	}
	return buffer, numBytes, nil
}

// puts an object into the bucket
func (e *Endpoint) putObject(key string, buffer *bytes.Reader) error {
	_, err := e.Uploader.Upload(context.TODO(), &awsS3.PutObjectInput{
		Bucket: aws.String(e.Bucket),
		Key:    aws.String(key),
		Body:   buffer,
	})
	return err
}
