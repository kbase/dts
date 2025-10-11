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

package transfers

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/databases/jdp"
	"github.com/kbase/dts/databases/kbase"
	"github.com/kbase/dts/databases/nmdc"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
	"github.com/kbase/dts/endpoints/local"
	"github.com/kbase/dts/journal"
)

// useful type aliases
type Database = databases.Database
type Endpoint = endpoints.Endpoint
type FileTransfer = endpoints.FileTransfer
type TransferStatus = endpoints.TransferStatus

// useful constants
const (
	TransferStatusUnknown    = endpoints.TransferStatusUnknown
	TransferStatusStaging    = endpoints.TransferStatusStaging
	TransferStatusActive     = endpoints.TransferStatusActive
	TransferStatusFailed     = endpoints.TransferStatusFailed
	TransferStatusFinalizing = endpoints.TransferStatusFinalizing
	TransferStatusInactive   = endpoints.TransferStatusInactive
	TransferStatusSucceeded  = endpoints.TransferStatusSucceeded
)

// starts processing transfers according to the given configuration, returning an
// informative error if anything prevents this
func Start() error {
	if global.Running {
		return &AlreadyRunningError{}
	}

	// if this is the first call to Start(), register our built-in endpoint and database providers
	if !global.Started {
		if err := registerEndpointProviders(); err != nil {
			return err
		}
		if err := registerDatabases(); err != nil {
			return err
		}
		global.Started = true
	}

	// do the necessary directories exist, and are they writable/readable?
	if err := validateDirectories(); err != nil {
		return err
	}

	// can we access the local endpoint?
	if _, err := endpoints.NewEndpoint(config.Service.Endpoint); err != nil {
		return err
	}

	// fire up the transfer journal
	if err := journal.Init(); err != nil {
		return err
	}

	if err := startOrchestration(); err != nil {
		return err
	}

	global.Running = true

	return nil
}

// Stops processing tasks. Adding new tasks and requesting task statuses are
// disallowed in a stopped state.
func Stop() error {
	var err error
	if global.Running {
		err := dispatcher.Stop()
		if err != nil {
			return err
		}
		err = journal.Finalize()
		if err != nil {
			return err
		}
		global.Running = false
	} else {
		err = &NotRunningError{}
	}
	return err
}

// Returns true if tasks are currently being processed, false if not.
func Running() bool {
	return global.Running
}

// this type holds a specification used to create a valid transfer task
type Specification struct {
	// a Markdown description of the transfer task
	Description string
	// the name of destination database to which files are transferred (as
	// specified in the DTS config file) OR a custom destination spec (<provider>:<id>:<credential>)
	Destination string
	// machine-readable instructions for processing the payload at its destination
	Instructions map[string]any
	// an array of identifiers for files to be transferred from Source to Destination
	FileIds []string
	// the name of source database from which files are transferred (as specified
	// in the DTS config file)
	Source string
	// information about the user requesting the task
	User auth.User
}

// Creates a new transfer task associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	var taskId uuid.UUID

	// have we requested files to be transferred?
	if len(spec.FileIds) == 0 {
		return taskId, &NoFilesRequestedError{}
	}

	// verify the source and destination strings
	_, err := databases.NewDatabase(spec.Source) // source must refer to a database
	if err != nil {
		return taskId, err
	}

	// destination can be a database OR a custom location
	if _, err = databases.NewDatabase(spec.Destination); err != nil {
		if _, err = endpoints.ParseCustomSpec(spec.Destination); err != nil {
			return taskId, err
		}
	}

	// create a new task and send it along for processing
	return dispatcher.CreateTransfer(spec)
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(transferId uuid.UUID) (TransferStatus, error) {
	return dispatcher.GetTransferStatus(transferId)
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	return dispatcher.CancelTransfer(transferId)
}

//===========
// Internals
//===========

// globals
var global struct {
	Running, Started bool
}

//-----------------------------------------------
// Provider Registration and Resource Validation
//-----------------------------------------------

func registerEndpointProviders() error {
	// NOTE: it's okay if these endpoint providers have already been registered,
	// NOTE: as they can be used in testing
	err := endpoints.RegisterEndpointProvider("globus", globus.NewEndpointFromConfig)
	if err == nil {
		err = endpoints.RegisterEndpointProvider("local", local.NewEndpoint)
	}
	if err != nil {
		if _, matches := err.(*endpoints.AlreadyRegisteredError); !matches {
			return err
		}
	}
	return nil
}

// registers databases; if at least one database is available, no error is propagated
func registerDatabases() error {
	numAvailable := 0
	if _, found := config.Databases["jdp"]; found {
		if err := databases.RegisterDatabase("jdp", jdp.NewDatabase); err != nil {
			slog.Error(err.Error())
		} else {
			numAvailable++
		}
	}
	if _, found := config.Databases["kbase"]; found {
		if err := databases.RegisterDatabase("kbase", kbase.NewDatabase); err != nil {
			slog.Error(err.Error())
		} else {
			numAvailable++
		}
	}
	if _, found := config.Databases["nmdc"]; found {
		if err := databases.RegisterDatabase("nmdc", nmdc.NewDatabase); err != nil {
			slog.Error(err.Error())
		} else {
			numAvailable++
		}
	}
	if numAvailable == 0 {
		return &NoDatabasesAvailable{}
	}
	return nil
}

func validateDirectories() error {
	err := validateDirectory("data", config.Service.DataDirectory)
	if err != nil {
		return err
	}
	return validateDirectory("manifest", config.Service.ManifestDirectory)
}

// checks for the existence of a directory and whether it is readable/writeable, returning an error
// if these conditions are not met
func validateDirectory(dirType, dir string) error {
	if dir == "" {
		return fmt.Errorf("no %s directory was specified!", dirType)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("%s is not a valid %s directory!", dir, dirType),
		}
	}

	// can we write a file and read it?
	testFile := filepath.Join(dir, "test.txt")
	writtenTestData := []byte("test")
	err = os.WriteFile(testFile, writtenTestData, 0644)
	if err != nil {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("Could not write to %s directory %s!", dirType, dir),
		}
	}
	readTestData, err := os.ReadFile(testFile)
	if err == nil {
		os.Remove(testFile)
	}
	if err != nil || !bytes.Equal(readTestData, writtenTestData) {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("Could not read from %s directory %s!", dirType, dir),
		}
	}
	return nil
}

//------------------------
// Transfer Orchestration
//------------------------

// The DTS orchestrates data transfer by requesting operations from service providers and monitoring
// their status. Transfers and status checks are handled by a family of goroutines that communicate
// with each other and the main goroutine via channels. These goroutines include:
//
// * dispatcher: handles all client requests, communicates with other goroutines as needed
// * stager: handles file staging by communicating with provider databases and endpoints
// * mover: handles file transfers by communicatig with provider databases and endpoints
// * manifestor: generates a transfer manifest after each transfer has completed and sends it to
//              the correct destination
// * store: maintains metadata records and status info for ongoing and completed transfers

func startOrchestration() error {
	if err := dispatcher.Start(); err != nil {
		return err
	}
	if err := stager.Start(); err != nil {
		return err
	}
	if err := mover.Start(); err != nil {
		return err
	}
	if err := manifestor.Start(); err != nil {
		return err
	}
	return store.Start()
}

func stopOrchestration() error {
	if err := dispatcher.Stop(); err != nil {
		return err
	}
	if err := stager.Stop(); err != nil {
		return err
	}
	if err := mover.Stop(); err != nil {
		return err
	}
	if err := manifestor.Stop(); err != nil {
		return err
	}
	return store.Stop()
}

// resolves the given destination (name) string, accounting for custom transfers
func destinationEndpoint(destination string) (endpoints.Endpoint, error) {
	// everything's been validated at this point, so no need to check for errors
	if strings.Contains(destination, ":") { // custom transfer spec
		customSpec, _ := endpoints.ParseCustomSpec(destination)
		endpointId, _ := uuid.Parse(customSpec.Id)
		credential := config.Credentials[customSpec.Credential]
		clientId, _ := uuid.Parse(credential.Id)
		return globus.NewEndpoint("Custom endpoint", endpointId, customSpec.Path, clientId, credential.Secret)
	}
	return endpoints.NewEndpoint(config.Databases[destination].Endpoint)
}

// resolves the folder at the given destination in which transferred files are deposited, with the
// given subfolder appended
func destinationFolder(destination, subfolder string) string {
	if customSpec, err := endpoints.ParseCustomSpec(destination); err == nil { // custom transfer?
		return filepath.Join(customSpec.Path, subfolder)
	}
	return subfolder
}
