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
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/deliveryhero/pipeline/v2"
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

//----------
// Transfer
//----------

// This type tracks the lifecycle of a file transfer: the copying of files from
// a source database to a destination database. A transfer comprises one or
// more Tasks, depending on how many transfer endpoints are involved.
type Transfer struct {
	DataDescriptors   []any          // in-line data descriptors
	DestinationFolder string         // folder path to which files are transferred
	Id                uuid.UUID      // task identifier
	Manifest          uuid.NullUUID  // manifest generation UUID (if any)
	ManifestFile      string         // name of locally-created manifest file
	Specification     Specification  // from which this Transfer is created
	Status            TransferStatus // status of file transfer operation
	Tasks             []Task         // list of constituent tasks
}

type TransferStatusCode int

const (
	TransferStatusUnknown TransferStatusCode = iota
	TransferStatusNew
	TransferStatusStaging
	TransferStatusTransferring
	TransferStatusProcessing
	TransferStatusFinalizing
	TransferStatusCanceled
	TransferStatusCompleted
	TransferStatusFailed
)

//------
// Task
//------

// A Task is an indivisible unit of work that is executed by stages in a pipeline.
type Task struct {
	TransferId          uuid.UUID               // ID of corresponding transfer
	Destination         string                  // name of destination database (in config)
	DestinationEndpoint string                  // name of destination database (in config)
	DestinationFolder   string                  // folder path to which files are transferred
	Descriptors         []any                   // Frictionless file descriptors
	Error               error                   // indicates an error occurred
	Source              string                  // name of source database (in config)
	SourceEndpoint      string                  // name of source endpoint (in config)
	Staging             uuid.NullUUID           // staging UUID (if any)
	StagingStatus       databases.StagingStatus // staging status
	Transfer            uuid.NullUUID           // file transfer UUID (if any)
	TransferStatus      TransferStatus          // status of file transfer operation
	User                auth.User               // info about user requesting transfer
}

type TaskStatusCode int

const (
	TaskStatusUnknown TaskStatusCode = iota
	TaskStatusNew
	TaskStatusStaging
	TaskStatusTransferring
	TaskStatusProcessing
	TaskStatusCanceled
	TaskStatusCompleted
	TaskStatusFailed
)

// basic status information regarding a transfer
type TransferRecord struct {
	Cancelled      bool
	CompletionTime time.Time
	Id             uuid.UUID
	PayloadSize    float64 // size of payload (gigabytes)
	Stage          int     // index of pipeline stage
	Specification  Specification
	Status         TransferStatus
}

// starts processing pipelines, returning an informative error if anything
// prevents it
func Start() error {
	if pipeline_ == nil {
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
		var err error
		pipeline_, err = CreatePipeline()
		if err != nil {
			return err
		}
	}
	return pipeline_.Start()
}

// Stops processing pipelines. Adding new pipelines and requesting statuses are
// disallowed in a stopped state.
func Stop() error {
	if pipeline_ != nil {
		return pipeline_.Stop()
	}
	return &NotRunningError{}
}

// Returns true if pipelines are currently being processed, false if not.
func Running() bool {
	if pipeline_ != nil {
		return pipeline_.Running()
	}
	return false
}

// Creates a new transfer associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	if pipeline_ != nil {
		return pipeline_.CreateTransfer(spec)
	}
	return uuid.UUID{}, &NotRunningError{}
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	if pipeline_ != nil {
		return pipeline_.Status(taskId)
	}
	return TransferStatus{}, &NotRunningError{}
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	if pipeline_ != nil {
		return pipeline_.Cancel(taskId)
	}
	return &NotRunningError{}
}

//-----------
// Internals
//-----------

// this type handles requests from the host and manages specific source->destination pipelines
type Pipeline struct {
	// context controlling pipeline(s)
	context context.Context
	// channels for handling requests from the host and internal data flow
	channels PipelineChannels
	// dispatch pipeline stages, into which provider stages are threaded
	stages DispatchStages
	// provider sequences (strings of stages) and associated their dispatch and return channels
	providerSequences map[string]ProviderSequence
	// true iff the pipelines are running
	running bool
}

// singleton pipeline instance
var pipeline_ *Pipeline

type PipelineChannels struct {
	// requests from host/client
	Create    chan Specification  // requests new transfers (host -> dispatch)
	Cancel    chan uuid.UUID      // requests transfer cancellations (host -> dispatch)
	GetStatus chan uuid.UUID      // requests transfer statuses (host -> dispatch)
	Status    chan TransferStatus // returns requested transfer statuses (host <- dispatch)
	Stop      chan struct{}       // stops pipeline and providers (host -> dispatch)

	// intra-pipeline communication
	NewTransfer     chan uuid.UUID            // sends new task UUID to initial pipeline stage
	TransferCreated chan uuid.UUID            // accepts newly created transfers (dispatch <- pipeline)
	TaskDispatch    chan Task                 // dispatches tasks to providers (dispatch -> pipeline)
	StatusUpdate    chan TransferStatusUpdate // accepts transfer status updates (dispatch <- pipeline)
	TaskComplete    chan Task                 // accepts completed tasks from providers

	// error reporting
	Error chan error
}

type DispatchStages struct {
	// initial stage: creates a transfer from a specification
	Initial pipeline.Processor[Specification, Transfer]
	// final stage: generates manifest and marks transfer as completed
	Final pipeline.Processor[Transfer, Transfer]
}

type ProviderSequence struct {
	DispatchChan, ReturnChan chan Task
	Sequence                 pipeline.Processor[Task, Task]
}

// information about a specific transfer status update (dispatch <- pipeline)
type TransferStatusUpdate struct {
	Id     uuid.UUID
	Status TransferStatus
}

func CreatePipeline() (*Pipeline, error) {
	statusUpdates := make(chan TransferStatusUpdate, 32) // channel for updating task statuses
	p := Pipeline{
		context: context.TODO(),
		channels: PipelineChannels{
			Create:          make(chan Specification, 32),
			Cancel:          make(chan uuid.UUID, 32),
			GetStatus:       make(chan uuid.UUID, 32),
			Status:          make(chan TransferStatus, 32),
			Stop:            make(chan struct{}),
			NewTransfer:     make(chan uuid.UUID, 32),
			TransferCreated: make(chan Transfer, 32),
			TaskDispatch:    make(chan Task, 32),
			StatusUpdate:    statusUpdates,
			TaskComplete:    make(chan Task, 32),
			Error:           make(chan error, 32),
		},
		stages: DispatchStages{
			Initial: InitialStage(),
			Final:   FinalStage(),
		},
		providerSequences: map[string]ProviderSequence{
			"jdp->kbase":  JdpToKBase(statusUpdates),
			"nmdc->kbase": NmdcToKBase(statusUpdates),
		},
	}

	return &p, nil
}

func (p *Pipeline) Start() error {
	if p.running {
		return &AlreadyRunningError{}
	}
	go p.listen()
	p.running = true
	return nil
}

func (p Pipeline) Running() bool {
	return p.running
}

func (p *Pipeline) CreateTransfer(spec Specification) (uuid.UUID, error) {
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
	p.channels.Create <- spec
	select {
	case transferId = <-p.channels.NewTransfers:
	case err = <-p.channels.Error:
	}
	return transferId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func (p *Pipeline) Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	p.channels.GetStatus <- taskId
	select {
	case status = <-p.channels.Status:
	case err = <-p.channels.Error:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func (p *Pipeline) Cancel(taskId uuid.UUID) error {
	var err error
	p.channels.Cancel <- taskId
	select { // default block provides non-blocking error check
	case err = <-p.channels.Error:
	default:
	}
	return err
}

func (p *Pipeline) Stop() error {
	if p.running {
		p.channels.Stop <- struct{}{}
		err := <-p.channels.Error
		if err != nil {
			return err
		}
		p.running = false
		return nil
	} else {
		return &NotRunningError{}
	}
}

// this goroutine dispatches client requests to pipelines
func (p *Pipeline) listen() error {

	// this function generates a provider key given a Specification
	providerKey := func(spec Specification) string {
		return fmt.Sprintf("%s->%s", spec.Source, spec.Destination)
	}

	// determine the name of the pipeline save file
	var saveFile string
	if config.Service.Name != "" {
		saveFile = filepath.Join(config.Service.DataDirectory,
			fmt.Sprintf("dts-%s.gob", config.Service.Name))
	} else {
		saveFile = filepath.Join(config.Service.DataDirectory, "dts.gob")
	}

	// load any transfers in progress and feed them to the pipelines
	transfers, err := loadTransfers(saveFile)
	if err != nil {
		return err
	}
	for _, transfer := range transfers {
		key := providerKey(transfer.Specification)
		if !transfer.Cancelled && transfer.Status.Code != TransferStatusFailed && transfer.Status.Code != TransferStatusSucceeded {
			_, found := p.providerSequences[key]
			if !found {
				slog.Error(fmt.Sprintf("Could not load transfer %s (invalid source '%s' or destination '%s')", transfer.Id.String(),
					transfer.Specification.Source, transfer.Specification.Destination))
			}
			// FIXME: restore each pipeline's state
		}
	}

	// cast channels to directional types as needed
	var createChan <-chan Specification = p.channels.Create
	var cancelChan <-chan uuid.UUID = p.channels.Cancel
	var errorChan chan<- error = p.channels.Error
	var getStatusChan <-chan uuid.UUID = p.channels.GetStatus
	var statusChan chan<- TransferStatus = p.channels.Status
	var stopChan <-chan struct{} = p.channels.Stop
	var newTransferChan chan<- uuid.UUID = p.channels.NewTransfer
	var taskDispatchChan chan<- Task = p.channels.TaskDispatch
	var statusUpdateChan <-chan TransferStatusUpdate = p.channels.StatusUpdate
	var taskCompleteChan <-chan Task = p.channels.TaskComplete

	// the task deletion period is specified in seconds
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// start handling requests
	for {
		select {
		case spec := <-createChan: // Create() called by client
			key := providerKey(spec)
			if _, found := p.providerSequences[key]; found {
				transferId := uuid.New()
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
			} else {
				errorChan <- InvalidDestinationError{Destination: spec.Destination} // FIXME: or source!
			}
		case transferId := <-cancelChan: // Cancel() called by client
			if record, found := transfers[transferId]; found {
				slog.Info(fmt.Sprintf("Transfer %s: received cancellation request", transferId.String()))
				key := providerKey(record.Specification)
				err := func(key string) error { return nil }(key) // FIXME: what goes here???
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
		case transferId := <-getStatusChan: // Status() called by client
			if record, found := transfers[transferId]; found {
				statusChan <- record.Status
			} else {
				err := &NotFoundError{Id: transferId}
				errorChan <- err
			}
		case statusUpdate := <-statusUpdateChan: // status updated by pipeline
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
		case <-stopChan: // Stop() called by client
			err := saveTransfers(transfers, saveFile)
			if err != nil {
				errorChan <- err
			}
			for _, pipeline := range pipelines {
				err := pipeline.Stop()
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
	Transfers map[uuid.UUID]TransferRecord
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func loadTransfers(saveFile string) (map[uuid.UUID]TransferRecord, error) {
	_, err := os.Stat(saveFile)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			return make(map[uuid.UUID]TransferRecord), nil // no data store, no big deal
		} else {
			return nil, err
		}
	}

	// load stuff
	file, err := os.Open(saveFile)
	if err != nil {
		return nil, err
	}
	slog.Debug(fmt.Sprintf("Found persistent transfer records in %s.", saveFile))
	defer file.Close()

	// read the saved state into memory
	var savedState SavedState
	enc := gob.NewDecoder(file)
	if err = enc.Decode(&savedState); err != nil {
		slog.Error(fmt.Sprintf("Reading save file %s: %s", saveFile, err.Error()))
		return nil, err
	}

	// load databases (those that have state info, anyway)
	if err = databases.Load(savedState.Databases); err != nil {
		slog.Error(fmt.Sprintf("Restoring database states: %s", err.Error()))
	}
	slog.Debug(fmt.Sprintf("Restored %d transfers from %s", len(savedState.Transfers), saveFile))
	return savedState.Transfers, nil
}

// saves a map of task IDs to tasks to the given file
func saveTransfers(transfers map[uuid.UUID]TransferRecord, saveFile string) error {
	slog.Debug(fmt.Sprintf("Saving transfers to %s", saveFile))

	file, err := os.OpenFile(saveFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("Opening file %s: %s", saveFile, err.Error())
	}
	defer file.Close()

	enc := gob.NewEncoder(file)

	// save transfer records
	savedState := SavedState{
		Transfers: transfers,
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
	slog.Debug(fmt.Sprintf("Saved transfers to %s", saveFile))
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
