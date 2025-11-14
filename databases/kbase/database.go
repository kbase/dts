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

package kbase

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// file database appropriate for handling KBase searches and transfers
// (implements the databases.Database interface)
type Database struct {
	EndpointName string
	kbaseFed     KBaseUserFederation
}

type Config struct {
	Endpoint                  string `yaml:"endpoint"`
	KBaseUserFederationConfig `yaml:",inline"`
}

func NewDatabase(conf Config) (databases.Database, error) {
	// make sure the endpoint is valid
	if !endpoints.EndpointExists(conf.Endpoint) {
		return nil, fmt.Errorf("invalid endpoint '%s' in kbase database configuration", conf.Endpoint)
	}
	db := Database{
		EndpointName: conf.Endpoint,
	}
	var err error
	db.kbaseFed, err = newKBaseUserFederation(conf.KBaseUserFederationConfig)
	if err != nil {
		return nil, err
	}
	err = db.kbaseFed.Start()
	if err != nil {
		return nil, err
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
	err := fmt.Errorf("Search not implemented for kbase database")
	return databases.SearchResults{}, err
}

func (db *Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	err := fmt.Errorf("Descriptors not implemented for kbase database")
	return nil, err
}

func (db *Database) EndpointNames() []string {
	return []string{db.EndpointName}
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	err := fmt.Errorf("StageFiles not implemented for kbase database")
	return uuid.UUID{}, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	err := fmt.Errorf("StagingStatus not implemented for kbase database")
	return databases.StagingStatusUnknown, err
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	return db.kbaseFed.usernameForOrcid(orcid)
}

func (db Database) Save() (databases.DatabaseSaveState, error) {
	// so far, this database has no internal state
	return databases.DatabaseSaveState{
		Name: "kbase",
	}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	// no internal state -> nothing to do
	return nil
}

func (db *Database) FinalizeDatabase() error {
	return db.kbaseFed.Stop()
}
