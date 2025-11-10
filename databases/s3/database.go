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
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/kbase/dts/databases"
)

// S3 object store database
// (implements the databases.Database interface)
type Database struct {
	// S3 bucket name
	Bucket string
	// S3 client
	Client *awsS3.Client
	// S3 downloader
	Downloader *manager.Downloader
}

// S3 database configuration
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
	BaseUrl string `yaml:"endpoint,omitempty"`
	// Whether to use path-style addressing (optional; default: false)
	UsePathStyle bool `yaml:"use_path_style,omitempty"`
}

// creates a new S3 database using the given configuration and bucket name
func NewDatabase(bucket string, cfg Config) (databases.Database, error) {

	var newDb Database

	awsCfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error loading AWS config: %v", err)
	}

	// create a client, overriding config values as needed
	newDb.Client = awsS3.NewFromConfig(awsCfg, func(o *awsS3.Options) {
		if cfg.BaseUrl != "" {
			o.BaseEndpoint = &cfg.BaseUrl
		}
		if cfg.AccessKeyID != "" || cfg.SecretKey != "" || cfg.SessionToken != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretKey,
				cfg.SessionToken,
			)
		} else {
			o.Credentials = aws.AnonymousCredentials{}
		}
		if cfg.Region != "" {
			o.Region = cfg.Region
		}
		o.UsePathStyle = cfg.UsePathStyle
	})
	newDb.Bucket = bucket
	newDb.Downloader = manager.NewDownloader(newDb.Client)

	return &newDb, nil
}

func DatabaseConstructor(bucket string, config Config) func() (databases.Database, error) {
	return func() (databases.Database, error) {
		return NewDatabase(bucket, config)
	}
}

func (db *Database) SpecificSearchParameters() map[string]any {
	return map[string]any{}
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
	return databases.SearchResults{}, fmt.Errorf("search not implemented for S3 database")
}

func (db *Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	return []map[string]any{}, fmt.Errorf("descriptors not implemented for S3 database")
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	return uuid.UUID{}, fmt.Errorf("staging not implemented for S3 database")
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	return databases.StagingStatusUnknown, fmt.Errorf("staging status not implemented for S3 database")
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	return "", fmt.Errorf("local user lookup not implemented for S3 database")
}

func (db *Database) Save() (databases.DatabaseSaveState, error) {
	return databases.DatabaseSaveState{}, fmt.Errorf("save not implemented for S3 database")
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	return fmt.Errorf("load not implemented for S3 database")
}
