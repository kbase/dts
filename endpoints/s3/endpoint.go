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
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	Client *s3.Client
	// endpoint UUID (obtained from config)
	Id uuid.UUID
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
	newEndpoint.Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
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

	newEndpoint.Bucket = bucket
	newEndpoint.Id = id

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
	// No staging for S3 endpoints
	return true, nil
}

func (e *Endpoint) Transfers() ([]uuid.UUID, error) {
	// not implemented yet
	return nil, fmt.Errorf("S3 endpoint transfers not implemented yet")
}

func (e *Endpoint) Transfer(dst Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	// not implemented yet
	return uuid.Nil, fmt.Errorf("S3 endpoint transfer not implemented yet")
}

func (e *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	// not implemented yet
	return endpoints.TransferStatus{Code: endpoints.TransferStatusUnknown}, fmt.Errorf("S3 endpoint status not implemented yet")
}

func (e *Endpoint) Cancel(id uuid.UUID) error {
	// not implemented yet
	return fmt.Errorf("S3 endpoint cancel not implemented yet")
}


