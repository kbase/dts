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

package kbase_lakehouse

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// file database appropriate for handling KBase searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// HTTP client that caches queries
	Client http.Client
	// Name of Globus/S3 lakehouse endpoint
	EndpointName string
}

type Config struct {
	Endpoint string `yaml:"endpoint"`
}

func NewDatabase(conf Config) (databases.Database, error) {
	// make sure the endpoint is valid
	if !endpoints.EndpointExists(conf.Endpoint) {
		return nil, fmt.Errorf("invalid endpoint '%s' in kbase database configuration", conf.Endpoint)
	}
	db := Database{
		EndpointName: conf.Endpoint,
	}
	return &db, nil
}

func DatabaseConstructor(conf map[string]any) func() (databases.Database, error) {
	return func() (databases.Database, error) {
		var kbaseConf Config
		if err := mapstructure.Decode(conf, &kbaseConf); err != nil {
			return nil, err
		}
		return NewDatabase(kbaseConf)
	}
}

func (db *Database) SpecificSearchParameters() map[string]any {
	return nil
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
	err := fmt.Errorf("Search not implemented for kbase_lakehouse database")
	return databases.SearchResults{}, err
}

func (db *Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	err := fmt.Errorf("Descriptors not implemented for kbase_lakehouse database")
	return nil, err
}

func (db *Database) EndpointNames() []string {
	return []string{db.EndpointName}
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	err := fmt.Errorf("StageFiles not implemented for kbase_lakehouse database")
	return uuid.UUID{}, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	err := fmt.Errorf("StagingStatus not implemented for kbase_lakehouse database")
	return databases.StagingStatusUnknown, err
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	record, err := db.fetchMMSRecord(orcid)
	return record.Username, err
}

func (db Database) Save() (databases.DatabaseSaveState, error) {
	// so far, this database has no internal state
	return databases.DatabaseSaveState{
		Name: "kbase_lakehouse",
	}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	return nil // no internal state
}

func (db *Database) FinalizeDatabase() error {
	return nil
}

//-----------
// Internals
//-----------

type mmsRecord struct {
	Username            string `json:"username"`
	S3AccessKey         string `json:"s3_access_key"`
	S3SecretKey         string `json:"s3_secret_key"`
	PolarisClientId     string `json:"polaris_client_id"`
	PolarisClientSecret string `json:"polaris_client_secret"`
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(orcid string, request *http.Request) {
	request.Header.Add("Authorization", fmt.Sprintf("Token %s_%s", orcid, db.Secret))
}

// retrieves the MMS record for the given ORCID
// response body and/or error
func (db *Database) fetchMMSRecord(orcid string) (mmsRecord, error) {
	u.Path = resource
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	request, err := http.NewRequest(http.MethodGet, "http://mms.dev:8000/credentials/", http.NoBody)
	if err != nil {
		return mmsRecord{}, err
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s_%s", orcid, db.Secret))
	if values.Has("orcid") { // orcid stashed in URL parameters
		db.addAuthHeader(values.Get("orcid"), req)
	}
	resp, err := db.Client.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200:
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	case 503:
		return nil, &databases.UnavailableError{
			Database: "jdp",
		}
	default:
		return nil, fmt.Errorf("an error occurred with the JDP database (%d)",
			resp.StatusCode)
	}
}
