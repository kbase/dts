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
	// context controlling pipeline(s)
	context context.Context
	// channels for handling requests from the host and internal data flow
	client   ClientChannels
	pipeline PipelineChannels
	// provider sequences (strings of stages) and associated their dispatch and return channels
	providers map[string]ProviderSequence
	// final stage for every transfer: generates manifest and marks transfer as completed
	generateManifest pipeline.Processor[Transfer, Transfer]
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
	CreatesTransfer         chan Transfer           // accepts newly created transfers (dispatch <- pipeline)
	ReceivesCancellation    chan uuid.UUID
	ReceivesDispatch        chan Task                 // dispatches tasks to providers (dispatch -> pipeline)
	UpdatesStatus           chan TransferStatusUpdate // accepts transfer status updates (dispatch <- pipeline)
	ReceivesCompletedTask   chan Task                 // (pipeline <- provider)
	ReceivesStop            chan struct{}             // dispatches tasks to providers (dispatch -> pipeline)
	ReportsError            chan error                // reports pipeline errors (dispatch <- pipeline)
}

// sequence of provider-specific stages
type ProviderSequence struct {
	Channels ProviderSequenceChannels
	Sequence pipeline.Processor[Task, Task]
}

// channels used to dispatch tasks to provider sequences and return them completed
type ProviderSequenceChannels struct {
	Dispatch chan Task
	Complete chan<- Task
}

// information about a specific transfer status update (dispatch <- pipeline)
type TransferStatusUpdate struct {
	Id     uuid.UUID
	Status TransferStatus
}

func CreatePipeline() (*Pipeline, error) {
	taskComplete := make(chan Task, 32)
	var taskCompleteIn chan<- Task = taskComplete        // for providers
	statusUpdates := make(chan TransferStatusUpdate, 32) // channel for updating task statuses
	p := Pipeline{
		context: context.TODO(),
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
			CreatesTransfer:         make(chan Transfer, 32),
			ReceivesCancellation:    make(chan uuid.UUID, 32),
			ReceivesDispatch:        make(chan Task, 32),
			UpdatesStatus:           make(chan TransferStatusUpdate, 32),
			ReceivesCompletedTask:   taskComplete,
			ReceivesStop:            make(chan struct{}),
			ReportsError:            make(chan error, 32),
		},
		providers: map[string]ProviderSequence{
			"jdp->kbase":  JdpToKBase(statusUpdates, taskCompleteIn),
			"nmdc->kbase": NmdcToKBase(statusUpdates, taskCompleteIn),
		},
		generateManifest: GenerateManifest(),
	}

	return &p, nil
}

func (p *Pipeline) Start() error {
	if p.running {
		return &AlreadyRunningError{}
	}
	go p.listenToClients()
	go p.runPipelines()
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
	var pipelineReceivesStop chan<- struct{} = p.pipeline.ReceivesStop
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
			pipelineReceivesStop <- struct{}{}

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

		case pipelineError := <-pipelineReportsError:
			// for now, we log pipeline errors when received
			slog.Error("Pipeline error: %s", pipelineError.Error())

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

// this goroutine handles all pipeline activity
func (p *Pipeline) runPipelines() {
	// this function generates a provider dispatch key given a Specification
	providerKey := func(source, destination string) string {
		return fmt.Sprintf("%s->%s", source, destination)
	}

	// channels for communicating with dispatch
	var pipelineReceivesTransferRequest <-chan IdAndSpecification = p.pipeline.ReceivesTransferRequest
	var pipelineCreatesTransfer chan<- Transfer = p.pipeline.CreatesTransfer
	var pipelineReceivesDispatch <-chan Task = p.pipeline.ReceivesDispatch
	var pipelineReceivesCancellation <-chan uuid.UUID = p.pipeline.ReceivesCancellation
	var pipelineUpdatesStatus chan<- TransferStatusUpdate = p.pipeline.UpdatesStatus
	var pipelineReceivesCompletedTask <-chan Task = p.pipeline.ReceivesCompletedTask
	var pipelineReceivesStop <-chan struct{} = p.pipeline.ReceivesStop
	var pipelineReportsError chan<- error = p.pipeline.ReportsError

	// handle messages from dispatch
	for {
		select {
		case idAndSpec := <-pipelineReceivesTransferRequest:
			newTransfer, err := createNewTransfer(idAndSpec.Id, idAndSpec.Specification)
			if err != nil {
				pipelineReportsError <- err
			} else {
				pipelineCreatesTransfer <- newTransfer
			}

		case task := <-pipelineReceivesDispatch:
			key := providerKey(task.Source, task.Destination)
			if provider, found := p.providers[key]; found {
				provider.Channels.Dispatch <- task
			} else {
				pipelineReportsError <- &InvalidSourceError{} // FIXME: or destination!
			}

		case task := <-pipelineReceivesCompletedTask:
			// FIXME: generate manifest
			pipelineUpdatesStatus <- TransferStatusUpdate{
				Id: task.TransferId,
				// FIXME: status!
			}

		case transferId := <-pipelineReceivesCancellation:
			// FIXME:
			pipelineUpdatesStatus <- TransferStatusUpdate{
				Id: transferId,
				// FIXME: status!
			}

		case <-pipelineReceivesStop:
			// FIXME:

		}
	}
}

// creates a new Transfer with the given ID from the given Specification
func createNewTransfer(id uuid.UUID, spec Specification) (Transfer, error) {
	// access the source database, resolving resource descriptors
	source, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return Transfer{}, err
	}

	transfer := Transfer{
		Id:            uuid.New(),
		Specification: spec,
		Status: TransferStatus{
			Code:     TransferStatusNew,
			NumFiles: len(spec.FileIds),
		},
		Tasks: make([]Task, 0),
	}

	transfer.DataDescriptors = make([]any, 0)
	{
		descriptors, err := source.Descriptors(transfer.Specification.User.Orcid, transfer.Specification.FileIds)
		if err != nil {
			return Transfer{}, err
		}

		// sift through the descriptors and separate files from in-line data
		for _, descriptor := range descriptors {
			if _, found := descriptor["path"]; found { // file to be transferred
				transfer.DataDescriptors = append(transfer.DataDescriptors, descriptor)
			} else if _, found := descriptor["data"]; found { // inline data
				transfer.DataDescriptors = append(transfer.DataDescriptors, descriptor)
			} else { // neither!
				err = fmt.Errorf("Descriptor '%s' (ID: %s) has no 'path' or 'data' field!",
					descriptor["name"], descriptor["id"])
				break
			}
		}
		if err != nil {
			return Transfer{}, err
		}
	}

	// if the database stores its files in more than one location, check that each
	// resource is associated with a valid endpoint
	if len(config.Databases[transfer.Specification.Source].Endpoints) > 1 {
		for _, d := range transfer.DataDescriptors {
			descriptor := d.(map[string]any)
			id := descriptor["id"].(string)
			endpoint := descriptor["endpoint"].(string)
			if endpoint == "" {
				return Transfer{}, &databases.ResourceEndpointNotFoundError{
					Database:   transfer.Specification.Source,
					ResourceId: id,
				}
			}
			if _, found := config.Endpoints[endpoint]; !found {
				return Transfer{}, &databases.InvalidResourceEndpointError{
					Database:   transfer.Specification.Source,
					ResourceId: id,
					Endpoint:   endpoint,
				}
			}
		}
	} else { // otherwise, just assign the database's endpoint to the resources
		for _, d := range transfer.DataDescriptors {
			descriptor := d.(map[string]any)
			descriptor["endpoint"] = config.Databases[transfer.Specification.Source].Endpoint
		}
	}

	// make sure the size of the payload doesn't exceed our specified limit
	transfer.PayloadSize = payloadSize(transfer.DataDescriptors) // (in GB)
	if transfer.PayloadSize > config.Service.MaxPayloadSize {
		return Transfer{}, &PayloadTooLargeError{Size: transfer.PayloadSize}
	}

	// determine the destination endpoint
	// FIXME: this conflicts with our redesign!!
	destinationEndpoint := config.Databases[transfer.Specification.Destination].Endpoint

	// construct a destination folder name
	destination, err := databases.NewDatabase(transfer.Specification.Destination)
	if err != nil {
		return Transfer{}, err
	}

	username, err := destination.LocalUser(transfer.Specification.User.Orcid)
	if err != nil {
		return Transfer{}, err
	}
	transfer.DestinationFolder = filepath.Join(username, "dts-"+transfer.Id.String())

	// assemble distinct endpoints and create a task for each
	distinctEndpoints := make(map[string]any)
	for _, d := range transfer.DataDescriptors {
		descriptor := d.(map[string]any)
		endpoint := descriptor["endpoint"].(string)
		if _, found := distinctEndpoints[endpoint]; !found {
			distinctEndpoints[endpoint] = struct{}{}
		}
	}
	for sourceEndpoint := range distinctEndpoints {
		// pick out the files corresponding to the source endpoint
		// NOTE: this is slow, but preserves file ID ordering
		descriptorsForEndpoint := make([]any, 0)
		for _, d := range transfer.DataDescriptors {
			descriptor := d.(map[string]any)
			endpoint := descriptor["endpoint"].(string)
			if endpoint == sourceEndpoint {
				descriptorsForEndpoint = append(descriptorsForEndpoint, descriptor)
			}
		}

		// set up a task for the endpoint
		transfer.Tasks = append(transfer.Tasks, Task{
			TransferId:          transfer.Id,
			Destination:         transfer.Specification.Destination,
			DestinationEndpoint: destinationEndpoint,
			DestinationFolder:   transfer.DestinationFolder,
			Descriptors:         descriptorsForEndpoint,
			Source:              transfer.Specification.Source,
			SourceEndpoint:      sourceEndpoint,
			Status: TaskStatus{
				Code: TaskStatusNew,
			},
			User: transfer.Specification.User,
		})
	}

	return transfer, nil
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

// computes the size of a payload for a transfer (in Gigabytes)
func payloadSize(descriptors []any) float64 {
	var size uint64
	for _, d := range descriptors {
		descriptor := d.(map[string]any)
		size += uint64(descriptor["bytes"].(int))
	}
	return float64(size) / float64(1024*1024*1024)
}
