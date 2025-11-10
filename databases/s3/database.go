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
	"encoding/gob"
	"fmt"
	"strings"
	"time"

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
	// Staging requests
	StagingRequests map[uuid.UUID]StagingRequest
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

type StagingRequest struct {
	// File paths to stage
	Paths []string `json:"paths"`
	// ORCID of user requesting staging
	Orcid string `json:"orcid"`
	// Time of staging request
	RequestTime string `json:"request_time"`
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
	newDb.StagingRequests = make(map[uuid.UUID]StagingRequest)

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
	files, err := db.listFilesWithPrefix(params.Query)
	if err != nil {
		return databases.SearchResults{}, fmt.Errorf("error listing files: %v", err)
	}
	descriptors, err := db.Descriptors(orcid, files)
	if err != nil {
		return databases.SearchResults{}, fmt.Errorf("error retrieving descriptors: %v", err)
	}
	return databases.SearchResults{
		Descriptors: descriptors,
	}, nil
}

func (db *Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	var descriptors []map[string]any

	for _, fileId := range fileIds {
		descriptor, err := db.s3ObjectToDescriptor(fileId)
		if err != nil {
			return nil, fmt.Errorf("error retrieving descriptor for file %s: %v", fileId, err)
		}
		descriptors = append(descriptors, descriptor)
	}

	return descriptors, nil
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	// check that files exist
	for _, fileId := range fileIds {
		exists, err := db.fileExists(fileId)
		if err != nil {
			return uuid.UUID{}, fmt.Errorf("error checking existence of file %s: %v", fileId, err)
		}
		if !exists {
			return uuid.UUID{}, fmt.Errorf("file %s does not exist in S3 bucket %s", fileId, db.Bucket)
		}
	}
	// save staging request (though no actual staging is done)
	stagingID := uuid.New()
	db.StagingRequests[stagingID] = StagingRequest{
		Paths:       fileIds,
		Orcid:       orcid,
		RequestTime: time.Now().Format(time.RFC3339),
	}
	return stagingID, nil
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	// check if staging request exists
	_, ok := db.StagingRequests[id]
	if !ok {
		return databases.StagingStatusUnknown, fmt.Errorf("staging request %s not found", id.String())
	}
	// since no actual staging is done, just return completed status
	return databases.StagingStatusSucceeded, nil
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	return "", fmt.Errorf("local user lookup not implemented for S3 database")
}

func (db *Database) Save() (databases.DatabaseSaveState, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(db.StagingRequests)
	if err != nil {
		return databases.DatabaseSaveState{}, fmt.Errorf("error encoding S3 database state: %v", err)
	}
	return databases.DatabaseSaveState{
		Name: "s3",
		Data: buffer.Bytes(),
	}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	enc := gob.NewDecoder(bytes.NewReader(state.Data))
	return enc.Decode(&db.StagingRequests)
}

//////////////
// Internals
//////////////

// returns whether a file exists in the S3 bucket
func (db *Database) fileExists(key string) (bool, error) {
	_, err := db.Client.HeadObject(context.TODO(), &awsS3.HeadObjectInput{
		Bucket: aws.String(db.Bucket),
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

// returns all files that have the given prefix in the S3 bucket
func (db *Database) listFilesWithPrefix(prefix string) ([]string, error) {
	var files []string
	contents, err := db.Client.ListObjectsV2(context.TODO(), &awsS3.ListObjectsV2Input{
		Bucket: aws.String(db.Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return files, fmt.Errorf("error listing files with prefix %s in bucket %s: %v", prefix, db.Bucket, err)
	}
	for _, obj := range contents.Contents {
		files = append(files, aws.ToString(obj.Key))
	}
	return files, nil
}

// returns a frictionless file descriptor for the given S3 object key
func (db *Database) s3ObjectToDescriptor(key string) (map[string]any, error) {
	// get object head to retrieve metadata
	headOutput, err := db.Client.HeadObject(context.TODO(), &awsS3.HeadObjectInput{
		Bucket: aws.String(db.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("error retrieving metadata for S3 object %s: %v", key, err)
	}

	parts := strings.Split(key, "/")
	descriptor := map[string]any{
		"name":      parts[len(parts)-1],
		"path":      key,
		"mediatype": aws.ToString(headOutput.ContentType),
		"bytes":     aws.ToInt64(headOutput.ContentLength),
	}

	// add ETag as checksum if available
	if headOutput.ETag != nil {
		descriptor["hash"] = strings.Trim(aws.ToString(headOutput.ETag), `"`)
	}

	if headOutput.ContentEncoding != nil {
		descriptor["encoding"] = aws.ToString(headOutput.ContentEncoding)
	}
	return descriptor, nil
}
