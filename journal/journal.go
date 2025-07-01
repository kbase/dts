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

package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/kbase/dts/config"
)

// This is the DTS transfer journal, which logs all transfer activity. The journal is a table of
// transfer records (one per transfer).

// initialize the DTS transfer journal
func Init() error {
	dbPath := filepath.Join(config.Service.DataDirectory, fmt.Sprintf("%s-journal.db", config.Service.Name))

	var err error
	var flags sqlite.OpenFlags
	if _, err = os.Stat(dbPath); err == nil { // database exists, so open read-write
		flags = sqlite.OpenReadWrite
	} else if errors.Is(err, os.ErrNotExist) { // database does not exist, so create it
		flags = sqlite.OpenCreate
	} else { // database file may or may not exist? Only the error knows for certain
		return err
	}

	conn_, err = sqlite.OpenConn(dbPath, flags)
	if err != nil {
		return err
	}
	return createSchema()
}

// saves and closes the DTS transfer journal (if it's been opened)
func Finalize() error {
	if conn_ != nil {
		return conn_.Close()
	}
	return nil
}

// records a newly requested transfer with the given information
func LogNewTransfer(id uuid.UUID, source, destination, orcid string, payloadSize int64, numFiles int) error {
	return sqlitex.Execute(conn_, "INSERT dts.transfers (id, source, destination, orcid, start_time, payload_size, num_files) VALUES (?, ?, ?, ?, ?, ?, ?);", 
		&sqlitex.ExecOptions{
			Args: []any{id.String(), source, destination, orcid, time.Now().String(), payloadSize, numFiles},
		})
}

// records the successful completion of an existing transfer
func LogTransferCompletion(id uuid.UUID, manifestFile string) error {
	err := sqlitex.Execute(conn_, "UPDATE dts.transfers SET stop_time = ?, status = 'succeeded' WHERE id = ?;", 
		&sqlitex.ExecOptions{
			Args: []any{time.Now().String(), id.String()},
		})
	if err != nil {
		return err
	}

	// store the manifest for the completed transfer
	manifest, err := datapackage.Load(manifestFile, validator.InMemoryLoader())
	jsonManifest, err := json.Marshal(manifest.Descriptor())
	return sqlitex.Execute(conn_, "INSERT dts.manifests (id, manifest) VALUES (?, JSON(?));", 
		&sqlitex.ExecOptions{
			Args: []any{id.String(), string(jsonManifest)},
		})
}

// records the unsuccessful termination of an existing transfer
func LogTransferFailure(id uuid.UUID) error {
	return sqlitex.Execute(conn_, "UPDATE dts.transfers SET stop_time = ?, status = 'failed' WHERE id = ?;", 
		&sqlitex.ExecOptions{
			Args: []any{time.Now().String(), id.String()},
		})
}

// records the cancellation of an existing transfer
func LogTransferCancellation(id uuid.UUID) error {
	return sqlitex.Execute(conn_, "UPDATE dts.transfers SET stop_time = ?, status = 'canceled' WHERE id = ?;", 
		&sqlitex.ExecOptions{
			Args: []any{time.Now().String(), id.String()},
		})
}

//-----------
// Internals
//-----------

// transfer journal database connection
var conn_ *sqlite.Conn

type Record struct {
	// UUID associated with the transfer
	Id uuid.UUID
	// the source and destination associated with the transfer
	Source, Destination string
	// the ORCID associated with the transfer
	Orcid string
	// time at which the transfer was requested
	StartTime time.Time
	// time at which the transfer completed or was canceled (NULL if in progress)
	StopTime time.Time
	// status of the transfer (succeeded, failed, staging, transferring, canceled)
	Status string
	// size of the transfer's payload in bytes
	PayloadSize int64 
	// number of files in the transfer's payload
	NumFiles int
	// manifest containing metadata for the transfer's payload
	Manifest map[string]any
}

// creates the schema for the transfer journal when it's first created
func createSchema() error {
	script := 
`CREATE TABLE IF NOT EXISTS dts.transfers (
	id TEXT PRIMARY KEY,
	source TEXT NOT NULL,
	destination TEXT NOT NULL,
	orcid TEXT NOT NULL,
	start_time TEXT NOT NULL,
	stop_time TEXT,
	status TEXT,
	payload_size INTEGER NOT NULL,
	num_files INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dts.manifests (
	id TEXT,
	manifest TEXT NOT NULL
);
`
	return sqlitex.ExecuteScript(conn_, script, nil)
}

