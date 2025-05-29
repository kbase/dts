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
	Canceled          bool
	CompletionTime    time.Time
	DataDescriptors   []any          // in-line data descriptors
	DestinationFolder string         // folder path to which files are transferred
	Id                uuid.UUID      // task identifier
	ManifestFile      string         // name of locally-created manifest file
	PayloadSize       float64        // size of payload (gigabytes)
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

type TransferStatus struct {
	Code       TransferStatusCode
	ManifestId uuid.NullUUID // manifest generation UUID (if any)
	NumFiles   int
}

//------
// Task
//------

// A Task is an indivisible unit of work that is executed by stages in a pipeline.
type Task struct {
	TransferId          uuid.UUID  // ID of corresponding transfer
	Destination         string     // name of destination database (in config)
	DestinationEndpoint string     // name of destination database (in config)
	DestinationFolder   string     // folder path to which files are transferred
	Descriptors         []any      // Frictionless file descriptors
	Error               error      // indicates any error that has occurred
	Source              string     // name of source database (in config)
	SourceEndpoint      string     // name of source endpoint (in config)
	Status              TaskStatus // status of task
	User                auth.User  // info about user requesting transfer
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

type TaskStatus struct {
	Code           TaskStatusCode
	StagingId      uuid.NullUUID            // staging UUID (if any)
	StagingStatus  databases.StagingStatus  // staging status
	TransferId     uuid.NullUUID            // file transfer UUID (if any)
	TransferStatus endpoints.TransferStatus // status of file transfer operation
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
	client   ClientChannels
	pipeline PipelineChannels
	// dispatch pipeline stages, into which provider stages are threaded
	stages DispatchStages
	// provider sequences (strings of stages) and associated their dispatch and return channels
	providerSequences map[string]ProviderSequence
	// final stage for every transfer: generates manifest and marks transfer as completed
	generateManifest pipeline.Processor[Transfer, Transfer]
	// true iff the pipelines are running
	running bool
}

// singleton pipeline instance
var pipeline_ *Pipeline

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
	ReceivesStop            chan struct{}             // dispatches tasks to providers (dispatch -> pipeline)
	ReportsError            chan error                // reports pipeline errors (dispatch <- pipeline)
}

// stages common to all transfers
type DispatchStages struct {
	// initial stage: creates a transfer from a specification
	CreateTransfer pipeline.Processor[Specification, Transfer]
}

// sequence of provider-specific stages
type ProviderSequence struct {
	Channels ProviderSequenceChannels
	Sequence pipeline.Processor[Task, Task]
}

// channels used to dispatch tasks to provider sequences and return them completed
type ProviderSequenceChannels struct {
	Dispatch <-chan Task
	Complete chan<- Task
}

// information about a specific transfer status update (dispatch <- pipeline)
type TransferStatusUpdate struct {
	Id     uuid.UUID
	Status TransferStatus
}

func CreatePipeline() (*Pipeline, error) {
	taskDispatch := make(chan Task, 32)
	taskComplete := make(chan Task, 32)
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
			ReceivesStop:            make(chan struct{}),
			ReportsError:            make(chan error, 32),
		},
		stages: DispatchStages{
			CreateTransfer:   CreateTransfer(),
			GenerateManifest: GenerateManifest(),
		},
		providerSequences: map[string]ProviderSequence{
			"jdp->kbase":  JdpToKBase(taskDispatch, statusUpdates, taskComplete),
			"nmdc->kbase": NmdcToKBase(taskDispatch, statusUpdates, taskComplete),
		},
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
		return err
	}
	// feed any transfers still in progress back to their provider
	/* FIXME:
	for _, transfer := range transfers {
		key := providerKey(transfer.Specification)
		if !transfer.Canceled && transfer.Status.Code != TransferStatusFailed && transfer.Status.Code != TransferStatusSucceeded {
			_, found := p.providerSequences[key]
			if !found {
				slog.Error(fmt.Sprintf("Could not load transfer %s (invalid source '%s' or destination '%s')", transfer.Id.String(),
					transfer.Specification.Source, transfer.Specification.Destination))
			}
			// FIXME: restore each pipeline's state
		}
	}
	*/

	ctx := context.TODO()

	// channels for communicating with the client
	var clientRequestsTransfer <-chan Specification = p.Client.RequestsTransfer
	var clientReceivesTransferId chan<- uuid.UUID = p.Client.ReceivesTransferId
	var clientCancelsTransfer <-chan uuid.UUID = p.Client.CancelsTransfer
	var clientRequestsStatus <-chan uuid.UUID = p.Client.RequestsStatus
	var clientReceivesStatus chan<- TransferStatus = p.Client.ReceivesStatus
	var clientRequestsStop <-chan struct{} = p.Client.RequestsStop
	var clientReceivesError chan<- error = p.Client.ReceivesError

	// channels for communicating with the pipeline
	var pipelineReceivesTransferRequest chan<- IdAndSpecification = p.Pipeline.ReceivesTransferRequest
	var pipelineCreatesTransfer <-chan Transfer = p.Pipeline.CreatesTransfer
	var pipelineReceivesCancellation chan<- uuid.UUID = p.Pipeline.ReceivesCancellation
	var pipelineReceivesDispatch chan<- Task = p.Pipeline.ReceivesDispatch
	var pipelineUpdatesStatus <-chan TransferStatusUpdate = p.Pipeline.UpdatesStatus
	var pipelineReceivesStop chan<- struct{} = p.Pipeline.ReceivesStop
	var pipelineReportsError <-chan error = p.Pipeline.ReportsError

	// the task deletion period is specified in seconds
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// start handling requests
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
				clientReceivesError <- err
			}
			pipelineReceivesStop <- struct{}{}
		case statusUpdate := <-pipelineUpdatesStatus:
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
		}
	}
}

// this goroutine handles all pipeline activity
func (p *Pipeline) runPipelines() {
	// this function generates a provider dispatch key given a Specification
	providerKey := func(spec Specification) string {
		return fmt.Sprintf("%s->%s", spec.Source, spec.Destination)
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
