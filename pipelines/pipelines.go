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

package pipelines

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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

// this type holds a specification used to create a valid transfer
type Specification struct {
	// a Markdown description of the transfer task
	Description string
	// the name of destination database to which files are transferred (as
	// specified in the DTS config file)
	Destination string
	// machine-readable instructions for processing the payload at its destination
	Instructions map[string]any
	// an array of identifiers for files to be transferred from Source to
	// Destination
	FileIds []string
	// the name of source database from which files are transferred (as specified
	// in the DTS config file)
	Source string
	// information about the user requesting the task
	User auth.User
}

// basic status information regarding a transfer
type TransferRecord struct {
	Cancelled      bool
	CompletionTime time.Time
	Id             uuid.UUID
	Pipeline       *Pipeline
	Specification  Specification
	Status         TransferStatus
}

// information about a specific transfer status update (dispatch <- pipeline)
type TransferStatusUpdate struct {
	Id     uuid.UUID
	Status TransferStatus
}

// channels for dispatching requests to specific pipelines
type DispatchChannels struct {
	// requests from host/client
	Create    chan Specification  // requests new transfers (host -> dispatch)
	Cancel    chan uuid.UUID      // requests transfer cancellations (host -> dispatch)
	GetStatus chan uuid.UUID      // requests transfer statuses (host -> dispatch)
	Status    chan TransferStatus // returns requested transfer statuses (host <- dispatch)

	// communication between pipelines and dispatch
	Halt          chan struct{}             // halts all pipelines (dispatch -> pipelines)
	NewTransfers  chan uuid.UUID            // accepts new transfers (dispatch <- pipeline)
	StatusUpdates chan TransferStatusUpdate // accepts transfer status updates (dispatch <- pipeline)

	// error reporting
	Error chan error
}

// starts processing pipelines, returning an informative error if anything
// prevents it
func Start() error {
	if running_ {
		return &AlreadyRunningError{}
	}

	// register our built-in endpoint and database providers
	if err := endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint); err != nil {
		return err
	}
	if err := endpoints.RegisterEndpointProvider("local", local.NewEndpoint); err != nil {
		return err
	}
	if _, found := config.Databases["jdp"]; found {
		if err := databases.RegisterDatabase("jdp", jdp.NewDatabase); err != nil {
			return err
		}
	}
	if _, found := config.Databases["kbase"]; found {
		if err := databases.RegisterDatabase("kbase", kbase.NewDatabase); err != nil {
			return err
		}
	}
	if _, found := config.Databases["nmdc"]; found {
		if err := databases.RegisterDatabase("nmdc", nmdc.NewDatabase); err != nil {
			return err
		}
	}

	// do the necessary directories exist, and are they writable/readable?
	if err := validateDirectory("data", config.Service.DataDirectory); err != nil {
		return err
	}
	if err := validateDirectory("manifest", config.Service.ManifestDirectory); err != nil {
		return err
	}

	// can we access the local endpoint?
	if _, err := endpoints.NewEndpoint(config.Service.Endpoint); err != nil {
		return err
	}

	// allocate dispatch channels
	channels_ = DispatchChannels{
		Create:    make(chan Specification, 32),
		Cancel:    make(chan uuid.UUID, 32),
		GetStatus: make(chan uuid.UUID, 32),
		Status:    make(chan TransferStatus, 32),

		Halt:          make(chan struct{}),
		NewTransfers:  make(chan uuid.UUID, 32),
		StatusUpdates: make(chan TransferStatusUpdate, 32),

		Error: make(chan error, 32),
	}

	go listen()
	running_ = true

	return nil
}

// Stops processing pipelines. Adding new pipelines and requesting statuses are
// disallowed in a stopped state.
func Halt() error {
	var err error
	if running_ {
		channels_.Halt <- struct{}{}
		err = <-channels_.Error
		running_ = false
	} else {
		err = &NotRunningError{}
	}
	return err
}

// Returns true if pipelines are currently being processed, false if not.
func Running() bool {
	return running_
}

// Creates a new transfer associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	var transferId uuid.UUID

	// have we requested files to be transferred?
	if len(spec.FileIds) == 0 {
		return transferId, &NoFilesRequestedError{}
	}

	// verify that we can fetch the task's source and destination databases
	// without incident
	_, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return transferId, err
	}
	_, err = databases.NewDatabase(spec.Destination)
	if err != nil {
		return transferId, err
	}

	// create a new task and send it along for processing
	channels_.Create <- spec
	select {
	case transferId = <-channels_.NewTransfers:
	case err = <-channels_.Error:
	}
	return transferId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	channels_.GetStatus <- taskId
	select {
	case status = <-channels_.Status:
	case err = <-channels_.Error:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	var err error
	channels_.Cancel <- taskId
	select { // default block provides non-blocking error check
	case err = <-channels_.Error:
	default:
	}
	return err
}

//-----------
// Internals
//-----------

// global variables for managing tasks
var running_ bool              // true if pipelines are running, false if not
var channels_ DispatchChannels // channels used for dispatching requests to pipelines

// this goroutine dispatches client requests to pipelines
func listen() error {

	// determine the name of the pipeline save file
	var saveFile string
	if config.Service.Name != "" {
		saveFile = filepath.Join(config.Service.DataDirectory,
			fmt.Sprintf("dts-%s.gob", config.Service.Name))
	} else {
		saveFile = filepath.Join(config.Service.DataDirectory, "dts.gob")
	}

	// create pipeline goroutines
	availablePipelines := []func(chan<- uuid.UUID, chan<- TransferStatusUpdate) (*Pipeline, error){
		JdpToKBase,
	}
	pipelines := make([]*Pipeline, 0)
	for _, createPipeline := range availablePipelines {
		pipeline, err := createPipeline(channels_.NewTransfers, channels_.StatusUpdates)
		if err != nil {
			return err
		} else {
			pipelines = append(pipelines, pipeline)
		}
	}

	// load any pre-existing pipeline states
	err := loadPipelines(saveFile, pipelines)
	if err != nil {
		return err
	}

	// map transfer sources and destinations to pipelines
	pipelineBySourceAndDestination := make(map[string]map[string]*Pipeline)
	for _, pipeline := range pipelines {
		pipelineByDestination, found := pipelineBySourceAndDestination[pipeline.source]
		if !found {
			pipelineByDestination = make(map[string]*Pipeline)
			pipelineBySourceAndDestination[pipeline.source] = pipelineByDestination
		}
		pipelineByDestination[pipeline.destination] = pipeline
	}

	// create some transfer records (indexed by UUID)
	transfers := make(map[uuid.UUID]TransferRecord)

	// parse the task channels into directional types as needed
	var createChan <-chan Specification = channels_.Create
	var cancelChan <-chan uuid.UUID = channels_.Cancel
	var errorChan chan<- error = channels_.Error
	var getStatusChan <-chan uuid.UUID = channels_.GetStatus
	var statusChan chan<- TransferStatus = channels_.Status
	var haltChan <-chan struct{} = channels_.Halt
	var newTransferChan chan<- uuid.UUID = channels_.NewTransfers
	var statusUpdateChan <-chan TransferStatusUpdate = channels_.StatusUpdates

	// the task deletion period is specified in seconds
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// start handling requests
	for {
		select {
		case spec := <-createChan: // Create() called
			if destinations, found := pipelineBySourceAndDestination[spec.Source]; found {
				if pipeline, found := destinations[spec.Destination]; found {
					transferId, err := pipeline.Create(spec)
					if err != nil {
						errorChan <- err
					} else {
						transfers[transferId] = TransferRecord{
							Id:            transferId,
							Specification: spec,
							Status: TransferStatus{
								NumFiles: len(spec.FileIds),
							},
						}
						slog.Info(fmt.Sprintf("Created new transfer %s (%d files requested)",
							transferId.String(), len(spec.FileIds)))
						newTransferChan <- transferId
					}
				} else {
					errorChan <- InvalidDestinationError{Destination: spec.Destination}
				}
			} else {
				errorChan <- InvalidSourceError{Source: spec.Source}
			}
		case transferId := <-cancelChan: // Cancel() called
			if record, found := transfers[transferId]; found {
				slog.Info(fmt.Sprintf("Transfer %s: received cancellation request", transferId.String()))
				err := record.Pipeline.Cancel(transferId)
				if err != nil {
					slog.Error(fmt.Sprintf("Transfer %s: %s", transferId.String(), err.Error()))
				} else {
					record.Cancelled = true
					transfers[transferId] = record
				}
			} else {
				err := &NotFoundError{Id: transferId}
				errorChan <- err
			}
		case transferId := <-getStatusChan: // Status() called
			if record, found := transfers[transferId]; found {
				statusChan <- record.Status
			} else {
				err := &NotFoundError{Id: transferId}
				errorChan <- err
			}
		case statusUpdate := <-statusUpdateChan: // status updated by a pipeline
			if record, found := transfers[statusUpdate.Id]; found {
				if statusUpdate.Status.Code != record.Status.Code {
					// update the record as needed
					record.Status = statusUpdate.Status
					if record.Status.Code == TransferStatusSucceeded || record.Status.Code == TransferStatusFailed {
						record.CompletionTime = time.Now()
					}
					transfers[statusUpdate.Id] = record
				} else {
					// if the task completed long enough go, delete its entry
					age := time.Since(record.CompletionTime)
					if age > deleteAfter {
						slog.Debug(fmt.Sprintf("Transfer %s: purging record", statusUpdate.Id.String()))
						delete(transfers, statusUpdate.Id)
					}
				}
			} else {
				slog.Error(fmt.Sprintf("Status update received for invalid transfer ID %s", statusUpdate.Id.String()))
			}
		case <-haltChan: // Halt() called
			err := savePipelines(saveFile, pipelines)
			if err != nil {
				errorChan <- err
			}
			for _, pipeline := range pipelines {
				err := pipeline.Halt()
				if err != nil {
					errorChan <- err
				}
			}
			break
		}
	}
}

//------------------------------------
// Saving and Loading Pipeline States
//------------------------------------

// this struct defines a layout for saving a GOB-encoded state
type SavedState struct {
	Databases databases.DatabaseSaveStates
	Pipelines map[string][]byte
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func loadPipelines(saveFile string, pipelines []*Pipeline) error {
	_, err := os.Stat(saveFile)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			return nil // no data store, no big deal
		} else {
			return err
		}
	}

	// load stuff
	file, err := os.Open(saveFile)
	if err != nil {
		return err
	}
	slog.Debug(fmt.Sprintf("Found persistent pipelines in %s.", saveFile))
	defer file.Close()

	// read the saved state into memory
	var savedState SavedState
	enc := gob.NewDecoder(file)
	if err = enc.Decode(&savedState); err != nil {
		slog.Error(fmt.Sprintf("Reading save file %s: %s", saveFile, err.Error()))
		return err
	}

	// load available pipelines
	for _, pipeline := range pipelines {
		name := fmt.Sprintf("%s -> %s", pipeline.source, pipeline.destination)
		if err := pipeline.Load(savedState.Pipelines[name]); err != nil {
			slog.Error(fmt.Sprintf("Restoring state for pipeline %s: %s", name, err.Error()))
		}
	}

	// load databases (those that have state info, anyway)
	if err = databases.Load(savedState.Databases); err != nil {
		slog.Error(fmt.Sprintf("Restoring database states: %s", err.Error()))
	}
	slog.Debug(fmt.Sprintf("Restored %d pipelines from %s", len(pipelines), saveFile))
	return nil
}

// saves a map of task IDs to tasks to the given file
func savePipelines(saveFile string, pipelines []*Pipeline) error {
	if len(pipelines) > 0 {
		slog.Debug(fmt.Sprintf("Saving %d pipelines to %s", len(pipelines), saveFile))

		file, err := os.OpenFile(saveFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("Opening file %s: %s", saveFile, err.Error())
		}
		defer file.Close()

		enc := gob.NewEncoder(file)

		// save pipelines
		savedState := SavedState{
			Pipelines: make(map[string][]byte),
		}
		for _, pipeline := range pipelines {
			name := fmt.Sprintf("%s -> %s", pipeline.source, pipeline.destination)
			savedState.Pipelines[name], err = pipeline.Save()
			if err != nil {
				os.Remove(saveFile)
				return fmt.Errorf("Saving pipeline %s: %s", name, err.Error())
			}
		}
		if savedState.Databases, err = databases.Save(); err != nil {
			os.Remove(saveFile)
			return fmt.Errorf("Saving state for databases: %s", err.Error())
		}
		err = enc.Encode(savedState)
		if err != nil {
			os.Remove(saveFile)
			return fmt.Errorf("Saving pipeline states: %s", err.Error())
		}
		err = file.Close()
		if err != nil {
			os.Remove(saveFile)
			return fmt.Errorf("Writing save file %s: %s", saveFile, err.Error())
		}
		slog.Debug(fmt.Sprintf("Saved %d pipelines to %s", len(pipelines), saveFile))
	} else {
		_, err := os.Stat(saveFile)
		if !errors.Is(err, fs.ErrNotExist) { // file exists
			os.Remove(saveFile)
		}
	}
	return nil
}

// this function checks for the existence of the data directory and whether it
// is readable/writeable, returning a non-nil error if any of these conditions
// are not met
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
