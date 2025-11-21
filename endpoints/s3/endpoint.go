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
	"log/slog"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/endpoints"
)

// This file implements an AWS S3 endpoint. It should be usable with any S3-compatible
// storage system, such as Minio.
//
// NOTE: This hasn't been tested with large files. The Upload and Download Managers
// used here should support multipart transfers, but this needs to be verified.
//
// NOTE: If we expect multiple simultaneous transfer requests, we may need to set
// up a queue system and limit the number of concurrent transfers. Each goroutine
// could have concurrent multipart transfers happening internally.

// S3 transfer status
type TransferStatus struct {
	mu sync.Mutex
	// transfer status information
	endpoints.TransferStatus
	// flag to cancel the transfer
	cancelRequested bool
}

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
	TransfersMap map[uuid.UUID]*TransferStatus
}

type Config struct {
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
func NewEndpoint(bucket string, id uuid.UUID, ecfg Config) (endpoints.Endpoint, error) {

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
	newEndpoint.TransfersMap = make(map[uuid.UUID]*TransferStatus)

	return &newEndpoint, nil
}

// constructs an S3 endpoint from a configuration map
func EndpointConstructor(conf map[string]any) (endpoints.Endpoint, error) {
	var config struct {
		Bucket string    `yaml:"bucket"`
		Id     string    `yaml:"id"`
		Config Config    `yaml:",inline" mapstructure:",squash"`
	}
	if err := mapstructure.Decode(conf, &config); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(config.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID specified for S3 endpoint: %s", config.Id)
	}
	return NewEndpoint(config.Bucket, id, config.Config)
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

func (e *Endpoint) Root() string {
	return e.Bucket + "/"
}

func (e *Endpoint) FilesStaged(descriptors []map[string]any) (bool, error) {
	staged := true
	for _, d := range descriptors {
		pathVal, found := d["path"]
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

func (e *Endpoint) Transfer(dst endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	s3Dest, ok := dst.(*Endpoint)
	if !ok {
		return uuid.Nil, fmt.Errorf("destination endpoint is not an S3 endpoint")
	}
	taskId := uuid.New()
	e.TransfersMap[taskId] = &TransferStatus{
		TransferStatus: endpoints.TransferStatus{
			Code:                endpoints.TransferStatusInactive,
			Message:             "Transfer pending",
			NumFiles:            len(files),
			NumFilesTransferred: 0,
			NumFilesSkipped:     0,
		},
	}
	go e.transferFiles(e.TransfersMap[taskId], *s3Dest, files)
	return taskId, nil
}

func (e *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	status, found := e.TransfersMap[id]
	if found && status != nil {
		return status.TransferStatus, nil
	}
	return endpoints.TransferStatus{}, endpoints.TransferNotFoundError{Id: id}
}

func (e *Endpoint) Cancel(id uuid.UUID) error {
	status, found := e.TransfersMap[id]
	if found && status != nil {
		status.mu.Lock()
		status.cancelRequested = true
		status.mu.Unlock()
		return nil
	}
	return endpoints.TransferNotFoundError{Id: id}
}

//-----------
// Internals
//-----------

// returns whether the given file exists in the bucket
func (e *Endpoint) fileExists(key string) (bool, error) {
	_, err := e.Client.HeadObject(context.TODO(), &awsS3.HeadObjectInput{
		Bucket: aws.String(e.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "Not Found") {
			return false, nil
		} else {
			return false, fmt.Errorf("error checking existence of S3 object %s: %v", key, err)
		}
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

func (t *TransferStatus) handleCancel() bool {
	if t.cancelRequested {
		t.mu.Lock()
		t.TransferStatus = endpoints.TransferStatus{
			Code:                endpoints.TransferStatusFailed,
			Message:             "Transfer canceled",
			NumFiles:            t.NumFiles,
			NumFilesTransferred: t.NumFilesTransferred,
			NumFilesSkipped:     t.NumFiles - t.NumFilesTransferred,
		}
		t.mu.Unlock()
	}
	return t.cancelRequested
}

// transfers a set of files from this endpoint to the given destination endpoint.
// typically invoked as a goroutine
func (e *Endpoint) transferFiles(t *TransferStatus, dst Endpoint, files []endpoints.FileTransfer) error {
	t.mu.Lock()
	t.TransferStatus = endpoints.TransferStatus{
		Code:                endpoints.TransferStatusActive,
		Message:             "Transfer in progress",
		NumFiles:            len(files),
		NumFilesTransferred: 0,
		NumFilesSkipped:     0,
	}
	t.mu.Unlock()

	for _, file := range files {
		// get the object from the source endpoint
		if t.handleCancel() {
			return nil
		}
		buffer, _, err := e.getObject(file.SourcePath)
		if err != nil {
			// skip this file
			t.mu.Lock()
			t.NumFilesSkipped++
			slog.Warn("S3 transfer: error getting object",
				slog.String("source_path", file.SourcePath),
				slog.String("error", err.Error()))
			t.mu.Unlock()
			continue
		}

		// put the object into the destination endpoint
		if t.handleCancel() {
			return nil
		}
		err = dst.putObject(file.DestinationPath, bytes.NewReader(buffer.Bytes()))
		if err != nil {
			// skip this file
			t.mu.Lock()
			t.NumFilesSkipped++
			slog.Warn("S3 transfer: error putting object",
				slog.String("destination_path", file.DestinationPath),
				slog.String("error", err.Error()))
			t.mu.Unlock()
			continue
		}

		t.mu.Lock()
		t.NumFilesTransferred++
		t.mu.Unlock()
	}
	if t.NumFilesTransferred < t.NumFiles {
		t.mu.Lock()
		t.TransferStatus = endpoints.TransferStatus{
			Code:                endpoints.TransferStatusFailed,
			Message:             "Some files failed to transfer",
			NumFiles:            t.NumFiles,
			NumFilesTransferred: t.NumFilesTransferred,
			NumFilesSkipped:     t.NumFilesSkipped,
		}
		t.mu.Unlock()
	} else {
		t.mu.Lock()
		t.TransferStatus = endpoints.TransferStatus{
			Code:                endpoints.TransferStatusSucceeded,
			Message:             "Transfer completed successfully",
			NumFiles:            t.NumFiles,
			NumFilesTransferred: t.NumFilesTransferred,
			NumFilesSkipped:     t.NumFilesSkipped,
		}
		t.mu.Unlock()
	}

	return nil
}
