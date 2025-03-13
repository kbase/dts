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
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/kbase/dts/credit"
)

// Database defines the interface for a database that is used to search for
// files and initiate file transfers
type Database interface {
	// returns a mapping of database-specific search parameters to zeroed values
	// of specific types accessible via type switches
	// * supported types: int, string, bool, float64, slices
	// * slices represent sets of accepted values of their respective types
	//   (useful for pulldown menus)
	// * databases with no specific search parameters should return nil
	SpecificSearchParameters() map[string]interface{}
	// search for files visible to the user with the given ORCID using the given
	// parameters
	Search(orcid string, params SearchParameters) (SearchResults, error)
	// returns a slice of Frictionless descriptors for the resources visible to
	// the user with the given ORCID that match the given IDs
	Descriptors(orcid string, fileIds []string) ([]interface{}, error)
	// begins staging the files visible to the user with the given ORCID for
	// transfer, returning a UUID representing the staging operation
	StageFiles(orcid string, fileIds []string) (uuid.UUID, error)
	// returns the status of a given staging operation
	StagingStatus(id uuid.UUID) (StagingStatus, error)
	// performs any work needed to finalize a transfer with the given UUID,
	// associated with the user with the given ORCID
	Finalize(orcid string, id uuid.UUID) error
	// returns the local username associated with the given ORCID
	LocalUser(orcid string) (string, error)
	// returns the saved state of the Database, loadable via Load
	Save() (DatabaseSaveState, error)
	// loads a previously saved state into the Database
	Load(state DatabaseSaveState) error
}

// represents a saved database state (for service restarts)
type DatabaseSaveState struct {
	// database name
	Name string
	// serialized database in bytes
	Data []byte
}

// represents a collection of saved database states
type DatabaseSaveStates struct {
	// mapping of orcid/database keys to database save states
	Data map[string]DatabaseSaveState
}

// parameters that define a search for files
type SearchParameters struct {
	// ElasticSearch query string
	Query string
	// file status, if requested
	Status SearchFileStatus
	// pagination support
	Pagination SearchPaginationParameters
	// database-specific search parameters with names matched to provided values
	// (validated by database)
	Specific map[string]json.RawMessage
}

// results from a file search
type SearchResults struct {
	// Frictionless data descriptors
	Descriptors []interface{} `json:"resources"`
}

type SearchPaginationParameters struct {
	// number of search results to skip
	Offset int
	// maximum number of search results to include (0 indicates no max)
	MaxNum int
}

// allows searching for files that are staged, not yet staged, etc
type SearchFileStatus int

const (
	SearchFileStatusAny SearchFileStatus = iota
	SearchFileStatusStaged
	SearchFileStatusUnstaged
)

// This enum identifies the status of a staging operation that moves
// files into place on a Database's endpoint.
type StagingStatus int

const (
	StagingStatusUnknown   StagingStatus = iota // unknown staging operation or status not available
	StagingStatusActive                         // staging in progress
	StagingStatusSucceeded                      // staging completed successfully
	StagingStatusFailed                         // staging failed
)

// registers a database creation function under the given database name
// to allow for e.g. test database implementations
func RegisterDatabase(dbName string, createDb func() (Database, error)) error {
	if firstTime {
		// register types that appear in Frictionless Descriptors (for manifests)
		gob.Register(credit.CreditMetadata{})
		gob.Register(json.RawMessage{})

		firstTime = false
	}

	if _, found := createDatabaseFuncs_[dbName]; found {
		return AlreadyRegisteredError{
			Database: dbName,
		}
	} else {
		createDatabaseFuncs_[dbName] = createDb
		return nil
	}
}

// creates a database proxy associated with the given ORCID, based on the
// configured type, or returns an existing instance
func NewDatabase(dbName string) (Database, error) {
	var err error

	// do we have one of these already?
	db, found := allDatabases_[dbName]
	if !found {
		// create the requested database
		if createDb, valid := createDatabaseFuncs_[dbName]; valid {
			db, err = createDb()
		} else {
			err = NotFoundError{dbName}
		}
		if err == nil {
			allDatabases_[dbName] = db // stash it
		}
	}
	return db, err
}

// saves the internal states of all resident databases, returning a map to
// their save states
func Save() (DatabaseSaveStates, error) {
	states := DatabaseSaveStates{
		Data: make(map[string]DatabaseSaveState),
	}
	for key, db := range allDatabases_ {
		saveState, err := db.Save()
		if err != nil {
			return states, err
		}
		states.Data[key] = saveState
	}
	return states, nil
}

// loads a previously saved map of save states for all databases, restoring
// their previous states
func Load(states DatabaseSaveStates) error {
	for dbName, state := range states.Data {
		if dbName != state.Name {
			return fmt.Errorf("Couldn't load saved state for database '%s'", state.Name)
		}
		db, err := NewDatabase(state.Name)
		if err != nil {
			return err
		}
		err = db.Load(state)
		if err != nil {
			return err
		}
	}
	return nil
}

//-----------
// Internals
//-----------

// set to false after the first database is registered
var firstTime = true

// we maintain a table of database instances, identified by their names
var allDatabases_ = make(map[string]Database)

// a table of database creation functions
var createDatabaseFuncs_ = make(map[string]func() (Database, error))
