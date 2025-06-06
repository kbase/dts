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

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
)

// this type handles requests from the host and manages specific source->destination pipelines
type Pipeline struct {
	// channels for handling requests from the host and internal data flow
	client   ClientChannels
	pipeline PipelineChannels
	// pipeline stages
	stages pipeline.Processor[IdAndSpecification, Transfer]
	// provider sequences (strings of stages) and associated their dispatch and return channels
	providers map[string]ProviderSequence
	// true iff the pipelines are running
	running bool
}

//----------
// Channels
//----------
// This looks like a lot, but it's really just an attempt to separate concerns

// channels for managing client requests
type ClientChannels struct {
	RequestsTransfer   chan Specification  // accepts new transfers requests (client -> dispatch)
	ReceivesTransferId chan uuid.UUID      // returns ID of new transfer to client (client <- dispatch)
	CancelsTransfer    chan uuid.UUID      // accepts transfer cancellations (client -> dispatch)
	RequestsStatus     chan uuid.UUID      // accepts transfer statuses (client -> dispatch)
	ReceivesStatus     chan TransferStatus // returns requested transfer statuses (client <- dispatch)
	RequestsStop       chan struct{}       // accepts requests to stop pipeline and providers (client -> dispatch)
	ReceivesError      chan error          // reports errors to clients
}

// communication between dispatch process and pipeline
type PipelineChannels struct {
	ReceivesTransferRequest chan IdAndSpecification // sends new task UUID to initial pipeline stage
	CreatesTransfer         <-chan Transfer         // accepts newly created transfers (dispatch <- pipeline)
	ReceivesCancellation    chan uuid.UUID
	ReceivesDispatch        chan Task                 // dispatches tasks to providers (dispatch -> pipeline)
	UpdatesStatus           chan TransferStatusUpdate // accepts transfer status updates (dispatch <- pipeline)
	ReportsError            chan error                // reports pipeline errors (dispatch <- pipeline)
}

// information about a specific transfer status update (dispatch <- pipeline)
type TransferStatusUpdate struct {
	Id     uuid.UUID
	Status TransferStatus
}

func CreatePipeline() (*Pipeline, error) {
	return CreatePipelineWithProviders(map[string]func(channels StageChannels) ProviderSequence{
		"jdp->kbase":  JdpToKBase,
		"nmdc->kbase": NmdcToKBase,
	})
}

func CreatePipelineWithProviders(providerFuncs map[string]func(channels StageChannels) ProviderSequence) (*Pipeline, error) {
	cancel := make(chan uuid.UUID)
	errorReporting := make(chan error, 32)
	taskComplete := make(chan Task, 32)
	statusUpdate := make(chan TransferStatusUpdate, 32)
	taskStatusUpdate := make(chan TaskStatusUpdate, 32)
	stageChannels := StageChannels{
		Cancel:     cancel,
		Complete:   taskComplete,
		Update:     statusUpdate,
		TaskUpdate: taskStatusUpdate,
		Error:      errorReporting,
	}
	providers := make(map[string]ProviderSequence)
	for name, f := range providerFuncs {
		providers[name] = f(stageChannels)
	}
	p := Pipeline{
		client: ClientChannels{
			RequestsTransfer:   make(chan Specification, 32),
			ReceivesTransferId: make(chan uuid.UUID, 32),
			CancelsTransfer:    make(chan uuid.UUID, 32),
			RequestsStatus:     make(chan uuid.UUID, 32),
			ReceivesStatus:     make(chan TransferStatus, 32),
			RequestsStop:       make(chan struct{}),
			ReceivesError:      make(chan error, 32),
		},
		pipeline: PipelineChannels{
			ReceivesTransferRequest: make(chan IdAndSpecification, 32),
			ReceivesCancellation:    cancel,
			UpdatesStatus:           statusUpdate,
			ReportsError:            errorReporting,
		},
		providers: providers,
	}

	p.assembleStages(stageChannels)

	return &p, nil
}

func (p *Pipeline) Start() error {
	if p.running {
		return &AlreadyRunningError{}
	}
	go p.listenToClients()
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
	p.client.RequestsTransfer <- spec
	select {
	case transferId = <-p.client.ReceivesTransferId:
	case err = <-p.client.ReceivesError:
	}
	return transferId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func (p *Pipeline) Status(transferId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	p.client.RequestsStatus <- transferId
	select {
	case status = <-p.client.ReceivesStatus:
	case err = <-p.client.ReceivesError:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func (p *Pipeline) Cancel(transferId uuid.UUID) error {
	var err error
	p.client.CancelsTransfer <- transferId
	select { // default block provides non-blocking error check
	case err = <-p.client.ReceivesError:
	default:
	}
	return err
}

func (p *Pipeline) Stop() error {
	if p.running {
		p.client.RequestsStop <- struct{}{}
		err := <-p.client.ReceivesError
		if err != nil {
			return err
		}
		p.running = false
		return nil
	} else {
		return &NotRunningError{}
	}
}

func (p *Pipeline) assembleStages(stageChannels StageChannels) {
	// wire together the pipeline's stages
	stages := pipeline.Join(CreateNewTransfer(stageChannels), DispatchToProvider(p.providers, stageChannels))
	p.stages = pipeline.Join(stages, GenerateManifest(stageChannels))
}

// this goroutine accepts client requests and dispatches them to pipelines
func (p *Pipeline) listenToClients() {

	// determine the name of the pipeline save file
	var saveFile string
	if config.Service.Name != "" {
		saveFile = filepath.Join(config.Service.DataDirectory,
			fmt.Sprintf("dts-%s.gob", config.Service.Name))
	} else {
		saveFile = filepath.Join(config.Service.DataDirectory, "dts.gob")
	}

	// restore our registry of transfers
	transfers, err := loadTransfers(saveFile)
	if err != nil {
		slog.Error("Couldn't load existing transfers!")
	}
	// feed any transfers still in progress back to their provider
	/* FIXME:
	for _, transfer := range transfers {
		key := providerKey(transfer.Specification)
		if !transfer.Canceled && transfer.Status.Code != TransferStatusFailed && transfer.Status.Code != TransferStatusSucceeded {
			_, found := p.providers[key]
			if !found {
				slog.Error(fmt.Sprintf("Could not load transfer %s (invalid source '%s' or destination '%s')", transfer.Id.String(),
					transfer.Specification.Source, transfer.Specification.Destination))
			}
			// FIXME: restore each pipeline's state
		}
	}
	*/

	// channels for communicating with the client
	var clientRequestsTransfer <-chan Specification = p.client.RequestsTransfer
	var clientReceivesTransferId chan<- uuid.UUID = p.client.ReceivesTransferId
	var clientCancelsTransfer <-chan uuid.UUID = p.client.CancelsTransfer
	var clientRequestsStatus <-chan uuid.UUID = p.client.RequestsStatus
	var clientReceivesStatus chan<- TransferStatus = p.client.ReceivesStatus
	var clientRequestsStop <-chan struct{} = p.client.RequestsStop
	var clientReceivesError chan<- error = p.client.ReceivesError

	// channels for communicating with the pipeline
	var pipelineReceivesTransferRequest chan<- IdAndSpecification = p.pipeline.ReceivesTransferRequest
	var pipelineCreatesTransfer <-chan Transfer = p.pipeline.CreatesTransfer
	var pipelineReceivesCancellation chan<- uuid.UUID = p.pipeline.ReceivesCancellation
	var pipelineReceivesDispatch chan<- Task = p.pipeline.ReceivesDispatch
	var pipelineUpdatesStatus <-chan TransferStatusUpdate = p.pipeline.UpdatesStatus
	var pipelineReportsError <-chan error = p.pipeline.ReportsError

	// channel for deleting old transfer records
	recordPurgeRequested := make(chan struct{})

	// period after which to purge records (in seconds)
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// heartbeat goroutine for purging old transfer records
	go func() {
		for {
			time.Sleep(time.Minute)
			recordPurgeRequested <- struct{}{}
		}
	}()

	// start the pipeline, sending status updates, error, and results to appropriate channels
	context, stop := context.WithCancel(context.Background())
	pipelineCompletesTransfer := pipeline.ProcessConcurrently(context, 1, p.stages, p.pipeline.ReceivesTransferRequest)

	// start handling client requests and coordinating with the pipeline
	for {
		select {
		case spec := <-clientRequestsTransfer:
			// FIXME: validate specification?
			// generate a new ID and return it immediately
			transferId := uuid.New()
			clientReceivesTransferId <- transferId

			slog.Info(fmt.Sprintf("Created new transfer %s (%d files requested)",
				transferId.String(), len(spec.FileIds)))
			pipelineReceivesTransferRequest <- IdAndSpecification{
				Id:            transferId,
				Specification: spec,
			}

		case transferId := <-clientCancelsTransfer:
			if _, found := transfers[transferId]; found {
				slog.Info(fmt.Sprintf("Transfer %s: received cancellation request", transferId.String()))
				pipelineReceivesCancellation <- transferId
			} else {
				err := &NotFoundError{Id: transferId}
				clientReceivesError <- err
			}

		case transferId := <-clientRequestsStatus:
			if record, found := transfers[transferId]; found {
				clientReceivesStatus <- record.Status
			} else {
				err := &NotFoundError{Id: transferId}
				clientReceivesError <- err
			}

		case <-clientRequestsStop:
			err := saveTransfers(transfers, saveFile)
			if err != nil {
				slog.Error("Couldn't save transfer records!")
			}
			stop()

		case newTransfer := <-pipelineCreatesTransfer:
			// the dispatch maintains transfer status information
			transfers[newTransfer.Id] = newTransfer
			// dispatch transfer tasks to the pipeline
			for _, task := range newTransfer.Tasks {
				pipelineReceivesDispatch <- task
			}

		case statusUpdate := <-pipelineUpdatesStatus:
			if record, found := transfers[statusUpdate.Id]; found {
				if statusUpdate.Status.Code != record.Status.Code {
					// update the record as needed
					record.Status = statusUpdate.Status
					if record.Status.Code == TransferStatusSucceeded || record.Status.Code == TransferStatusFailed {
						record.CompletionTime = time.Now()
					}
					transfers[statusUpdate.Id] = record
				}
			} else {
				slog.Error(fmt.Sprintf("Status update received for invalid transfer ID %s", statusUpdate.Id.String()))
			}

		case completedTransfer := <-pipelineCompletesTransfer:
			transfers[completedTransfer.Id] = completedTransfer
			slog.Info(fmt.Sprintf("Completed transfer %s", completedTransfer.Id.String()))
		case pipelineError := <-pipelineReportsError:
			// for now, we log pipeline errors when received
			slog.Error(fmt.Sprintf("Pipeline error: %s", pipelineError.Error()))

		case <-recordPurgeRequested:
			// comb over transfer records, purging those that have aged out
			for _, record := range transfers {
				age := time.Since(record.CompletionTime)
				if age > deleteAfter {
					slog.Debug(fmt.Sprintf("Transfer %s: purging record", record.Id.String()))
					delete(transfers, record.Id)
				}
			}
		}
	}
}

//------------------------------------
// Saving and Loading Pipeline States
//------------------------------------

// this struct defines a layout for saving a GOB-encoded state
type SavedState struct {
	Databases databases.DatabaseSaveStates
	Transfers map[uuid.UUID]Transfer
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func loadTransfers(saveFile string) (map[uuid.UUID]Transfer, error) {
	_, err := os.Stat(saveFile)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			return make(map[uuid.UUID]Transfer), nil // no data store, no big deal
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
func saveTransfers(transfers map[uuid.UUID]Transfer, saveFile string) error {
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
