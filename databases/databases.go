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

package databases

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

type SearchPaginationParameters struct {
	// number of search results to skip
	Offset int
	// maximum number of search results to include (0 indicates no max)
	MaxNum int
}

// parameters that define a search for files
type SearchParameters struct {
	// ElasticSearch query string
	Query string
	// pagination support
	Pagination SearchPaginationParameters
}

// results from a file query
type SearchResults struct {
	Resources []frictionless.DataResource `json:"resources"`
}

// This "enum" type identifies the status of a staging operation that moves
// files into place on a Database's endpoint.
type StagingStatus int

const (
	StagingStatusUnknown   StagingStatus = iota // unknown staging operation or status not available
	StagingStatusActive                         // staging in progress
	StagingStatusSucceeded                      // staging completed successfully
	StagingStatusFailed                         // staging failed
)

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// search for files using the given parameters
	Search(params SearchParameters) (SearchResults, error)
	// returns a slice of Frictionless DataResources for the files with the
	// given IDs
	Resources(fileIds []string) ([]frictionless.DataResource, error)
	// begins staging the files for a transfer, returning a UUID representing the
	// staging operation
	StageFiles(fileIds []string) (uuid.UUID, error)
	// returns the status of a given staging operation
	StagingStatus(id uuid.UUID) (StagingStatus, error)
	// returns the endpoint associated with this database
	Endpoint() (endpoints.Endpoint, error)
	// returns the local username associated with the given Orcid ID
	LocalUser(orcid string) (string, error)
}

// This error type is returned when a database is sought but not found.
type NotFoundError struct {
	dbName string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("The database %s was not found.", e.dbName)
}

// This error type is returned when a database is already registered and
// an attempt is made to register it again.
type AlreadyRegisteredError struct {
	dbName string
}

func (e AlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Cannot register database %s (already registered).", e.dbName)
}

// we maintain a table of database instances, identified by their names
var allDatabases = make(map[string]Database)

// here's a table of database creation functions
var createDatabaseFuncs = make(map[string]func(name string) (Database, error))

// registers a database creation function under the given database name
// to allow for e.g. test database implementations
func RegisterDatabase(dbName string, createDb func(orcid string) (Database, error)) error {
	if _, found := createDatabaseFuncs[dbName]; found {
		return AlreadyRegisteredError{dbName: dbName}
	} else {
		createDatabaseFuncs[dbName] = createDb
		return nil
	}
}

// creates a database proxy associated with the given ORCID ID, based on the
// configured type, or returns an existing instance
func NewDatabase(orcid, dbName string) (Database, error) {
	var err error

	// do we have one of these already?
	key := fmt.Sprintf("orcid: %s db: %s", orcid, dbName)
	db, found := allDatabases[key]
	if !found {
		// create the requested database
		if createDb, valid := createDatabaseFuncs[dbName]; valid {
			db, err = createDb(orcid)
		} else {
			err = NotFoundError{dbName}
		}
		if err == nil {
			allDatabases[key] = db // stash it
		}
	}
	return db, err
}
