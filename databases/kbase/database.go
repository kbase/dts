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

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// file database appropriate for handling KBase searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// database identifier
	Id string
}

func NewDatabase(orcid string) (databases.Database, error) {
	if orcid == "" {
		return nil, fmt.Errorf("No ORCID ID was given")
	}

	return &Database{
		Id: "kbase",
	}, nil
}

func (db *Database) SpecificSearchParameters() map[string]interface{} {
	return nil
}

func (db *Database) Search(params databases.SearchParameters) (databases.SearchResults, error) {
	err := fmt.Errorf("Search not implemented for kbase database!")
	return databases.SearchResults{}, err
}

func (db *Database) Resources(fileIds []string) ([]frictionless.DataResource, error) {
	err := fmt.Errorf("Resources not implemented for kbase database!")
	return nil, err
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
	err := fmt.Errorf("StageFiles not implemented for kbase database!")
	return uuid.UUID{}, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	err := fmt.Errorf("StagingStatus not implemented for kbase database!")
	return databases.StagingStatusUnknown, err
}

func (db *Database) Endpoint() (endpoints.Endpoint, error) {
	return endpoints.NewEndpoint(config.Databases[db.Id].Endpoint)
}

func (db *Database) LocalUser(orcid string) (string, error) {
	// for KBase user federation, we rely on a table maintained by our KBase
	// auth server proxy
	return auth.KBaseLocalUsernameForOrcid(orcid)
}
