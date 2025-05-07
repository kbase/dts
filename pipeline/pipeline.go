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

package pipeline

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

// This type tracks the lifecycle of a file transfer: the copying of files from
// a source database to a destination database. A transfer comprises one or
// more Tasks, depending on how many transfer endpoints are involved.
type Transfer struct {
	Canceled          bool              // set if a cancellation request has been made
	CompletionTime    time.Time         // time at which the transfer completed
	DataDescriptors   []any             // in-line data descriptors
	Description       string            // Markdown description of the task
	Destination       string            // name of destination database (in config)
	DestinationFolder string            // folder path to which files are transferred
	FileIds           []string          // IDs of all files being transferred
	Id                uuid.UUID         // task identifier
	Instructions      map[string]any    // machine-readable task processing instructions
	Manifest          uuid.NullUUID     // manifest generation UUID (if any)
	ManifestFile      string            // name of locally-created manifest file
	PayloadSize       float64           // Size of payload (gigabytes)
	Source            string            // name of source database (in config)
	Status            TransferStatus    // status of file transfer operation
	Tasks             []Task 						// list of constituent tasks
	User              auth.User         // info about user requesting transfer
}

// A Task is an indivisible unit of work that is executed by stages in a
// pipeline.
type Task struct {
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

// A Stage is an indivisible portion of a pipeline. In a Stage, a Task is taken
// as input and returned as output. The Task itself contains all relevent
// information including error conditions.
type Stage interface {
	Execute(task Task) Task
}

// starts processing pipelines, returning an informative error if anything
// prevents it
func Start() error {
	if running {
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

	if err := createPipeline(); err != nil {
		return err
	}

	// allocate channels
	channels_ = channelsType{
		CreateTask:       make(chan Transfer, 32),
		CancelTask:       make(chan uuid.UUID, 32),
		GetTaskStatus:    make(chan uuid.UUID, 32),
		ReturnTaskId:     make(chan uuid.UUID, 32),
		ReturnTaskStatus: make(chan TransferStatus, 32),
		Error:            make(chan error, 32),
		Poll:             make(chan struct{}),
		Stop:             make(chan struct{}),
	}

	go listen()

	// okay, we're running now
	running_ = true

	return nil
}

// Stops processing pipelines. Adding new pipelines and requesting statuses are
// disallowed in a stopped state.
func Stop() error {
	var err error
	if running {
		channels_.Stop <- struct{}{}
		err = <-channels_.Error
		running = false
	} else {
		err = &NotRunningError{}
	}
	return err
}

// Returns true if pipelines are currently being processed, false if not.
func Running() bool {
	return running
}

// Creates a new transfer associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	var taskId uuid.UUID

	// have we requested files to be transferred?
	if len(spec.FileIds) == 0 {
		return taskId, &NoFilesRequestedError{}
	}

	// verify that we can fetch the task's source and destination databases
	// without incident
	_, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return taskId, err
	}
	_, err = databases.NewDatabase(spec.Destination)
	if err != nil {
		return taskId, err
	}

	// create a new task and send it along for processing
	channels_.CreateTask <- transferTask{
		User:         spec.User,
		Source:       spec.Source,
		Destination:  spec.Destination,
		FileIds:      spec.FileIds,
		Description:  spec.Description,
		Instructions: spec.Instructions,
	}
	select {
	case taskId = <-channels_.ReturnTaskId:
	case err = <-channels_.Error:
	}
	return taskId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	channels_.GetTaskStatus <- taskId
	select {
	case status = <-channels_.ReturnTaskStatus:
	case err = <-channels_.Error:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	var err error
	channels_.CancelTask <- taskId
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
var running_ bool            // true if tasks are processing, false if not
var channels_ channelsType   // channels used for processing tasks
var pipeline_ pipeline       // persistent DTS pipeline

// this type holds various channels used by the task manager to communicate
// with its worker goroutine
type channelsType struct {
	Create   chan Specification  // used by client to create a new transfer
	Cancel   chan uuid.UUID      // used by client to cancel a transfer
	GetStatus    chan uuid.UUID      // used by client to request transfer status
	ReturnId     chan uuid.UUID      // returns transfer ID to client
	ReturnStatus chan TransferStatus // returns transfer status to client
	Error            chan error          // returns error to client
	Stop             chan struct{}       // used by client to stop task management
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func createOrLoadTransfers(dataFile string) map[uuid.UUID]Transfer {
	file, err := os.Open(dataFile)
	if err != nil {
		return make(map[uuid.UUID]Transfer)
	}
	slog.Debug(fmt.Sprintf("Found previous tasks in %s.", dataFile))
	defer file.Close()
	enc := gob.NewDecoder(file)
	var tasks map[uuid.UUID]Transfer
	var databaseStates databases.DatabaseSaveStates
	if err = enc.Decode(&tasks); err == nil {
		err = enc.Decode(&databaseStates)
	}
	if err != nil { // file not readable
		slog.Error(fmt.Sprintf("Reading task file %s: %s", dataFile, err.Error()))
		return make(map[uuid.UUID]Transfer)
	}
	if err = databases.Load(databaseStates); err != nil {
		slog.Error(fmt.Sprintf("Restoring database states: %s", err.Error()))
	}
	slog.Debug(fmt.Sprintf("Restored %d tasks from %s", len(tasks), dataFile))
	return tasks
}

// saves a map of task IDs to tasks to the given file
func saveTransfers(tasks map[uuid.UUID]Transfer, dataFile string) error {
	if len(tasks) > 0 {
		slog.Debug(fmt.Sprintf("Saving %d tasks to %s", len(tasks), dataFile))
		file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("Opening task file %s: %s", dataFile, err.Error())
		}
		enc := gob.NewEncoder(file)
		if err = enc.Encode(tasks); err == nil {
			var databaseStates databases.DatabaseSaveStates
			if databaseStates, err = databases.Save(); err == nil {
				err = enc.Encode(databaseStates)
			}
		}
		if err != nil {
			file.Close()
			os.Remove(dataFile)
			return fmt.Errorf("Saving tasks: %s", err.Error())
		}
		err = file.Close()
		if err != nil {
			os.Remove(dataFile)
			return fmt.Errorf("Writing task file %s: %s", dataFile, err.Error())
		}
		slog.Debug(fmt.Sprintf("Saved %d tasks to %s", len(tasks), dataFile))
	} else {
		_, err := os.Stat(dataFile)
		if !errors.Is(err, fs.ErrNotExist) { // file exists
			os.Remove(dataFile)
		}
	}
	return nil
}

// this function coordinates workflows with the DTS pipeline
func listen() {
	// create or recreate a persistent table of transfer-related tasks
	var dataStore string
	if config.Service.Name != "" {
		dataStore = filepath.Join(config.Service.DataDirectory,
			fmt.Sprintf("dts-%s.gob", config.Service.Name))
	} else {
		dataStore = filepath.Join(config.Service.DataDirectory, "dts.gob")
	}
	transfers := createOrLoadTransfers(dataStore)

	// parse the task channels into directional types as needed
	var createChan <-chan Transfer = channels_.Create
	var cancelChan <-chan uuid.UUID = channels_.Cancel
	var getStatusChan <-chan uuid.UUID = channels_.GetStatus
	var returnIdChan chan<- uuid.UUID = channels_.ReturnId
	var returnStatusChan chan<- TransferStatus = channels_.ReturnStatus
	var errorChan chan<- error = channels_.Error
	var pollChan <-chan struct{} = channels_.Poll
	var stopChan <-chan struct{} = channels_.Stop

	// the task deletion period is specified in seconds
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// start scurrying around
	for {
		select {
		case transferSpec := <-createTransferChan: // Create() called
			newTransfer.Id = uuid.New()
			transfers[newTracreateOrRestorePipeline(newTransfer)
			transfers[newTransfer.Id] = newTransfer
			returnTaskIdChan <- newTransfer.Id
			slog.Info(fmt.Sprintf("Created new transfer %s (%d file(s) requested)",
				newTransfer.Id.String(), len(newTransfer.FileIds)))
		case transferId := <-cancelTaskChan: // Cancel() called
			if transfer, found := transfers[transferId]; found {
				slog.Info(fmt.Sprintf("Transfer %s: received cancellation request", transferId.String()))
				err := task.Cancel()
				if err != nil {
					task.Status.Code = TransferStatusUnknown
					task.Status.Message = fmt.Sprintf("error in cancellation: %s", err.Error())
					task.CompletionTime = time.Now()
					slog.Error(fmt.Sprintf("Task %s: %s", task.Id.String(), task.Status.Message))
					tasks[task.Id] = task
				}
			} else {
				err := &NotFoundError{Id: taskId}
				errorChan <- err
			}
		case taskId := <-getTaskStatusChan: // Status() called
			if task, found := tasks[taskId]; found {
				returnTaskStatusChan <- task.Status
			} else {
				err := &NotFoundError{Id: taskId}
				errorChan <- err
			}
		case <-pollChan: // time to move things along
			for taskId, task := range tasks {
				if !task.Completed() {
					oldStatus := task.Status
					err := task.Update()
					if err != nil {
						// We log task update errors but do not propagate them. All
						// task errors result in a failed status.
						task.Status.Code = TransferStatusFailed
						task.Status.Message = err.Error()
						task.CompletionTime = time.Now()
						slog.Error(fmt.Sprintf("Task %s: %s", task.Id.String(), err.Error()))
					}
					if task.Status.Code != oldStatus.Code {
						switch task.Status.Code {
						case TransferStatusStaging:
							slog.Info(fmt.Sprintf("Task %s: staging %d file(s) (%g GB)",
								task.Id.String(), len(task.FileIds), task.PayloadSize))
						case TransferStatusActive:
							slog.Info(fmt.Sprintf("Task %s: beginning transfer (%d file(s), %g GB)",
								task.Id.String(), len(task.FileIds), task.PayloadSize))
						case TransferStatusInactive:
							slog.Info(fmt.Sprintf("Task %s: suspended transfer", task.Id.String()))
						case TransferStatusFinalizing:
							slog.Info(fmt.Sprintf("Task %s: finalizing transfer", task.Id.String()))
						case TransferStatusSucceeded:
							slog.Info(fmt.Sprintf("Task %s: completed successfully", task.Id.String()))
						case TransferStatusFailed:
							slog.Info(fmt.Sprintf("Task %s: failed", task.Id.String()))
						}
					}
				}

				// if the task completed a long enough time go, delete its entry
				if task.Age() > deleteAfter {
					slog.Debug(fmt.Sprintf("Task %s: purging transfer record", task.Id.String()))
					delete(tasks, taskId)
				} else { // update its entry
					tasks[taskId] = task
				}
			}
		case <-stopChan: // Stop() called
			err := saveTransfers(tasks, dataStore) // don't forget to save our state!
			errorChan <- err
			break
		}
	}
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

//----------------------
// Pipelines and stages
//----------------------

func pipeline(in <-chan Transfer) <- chan Transfer {

	// build pipeline stages
	scatterChan := make(chan Transfer)
	scatteredChan := scatter(scatterChan)

	prepareChan := make(chan Task)
	preparedChan := prepare(prepareChan)

	transferChan := make(chan Task)
	transferedChan := transfer(transferChan)

	extractChan := make(chan Task)
	extractedChan := extract(extractChan)

	gatherChan := make(chan Task)
	gatheredChan := gather(gatherChan)

	manifestChan := make(chan Transfer)
	manifestedChan := generateManifest(manifestChan)

	// fire it up!
	out := make(chan Transfer)
	go func() {
		// guide the transfer through the stages
		// FIXME: 
	}

	return out
}

// scatter: divides a transfer up into a set of tasks
func scatter(in <-chan Transfer) <-chan Task {
	out := make(chan Task)
	go func() {
	}
	return out
}

// prepare: moves files into a location from which they can be transferred
func prepare(in <-chan Task) <-chan Task {
	out := make(chan Task)
	go func() {
		task := <-in
		// check whether files are in place
		out <- task
	}
	return out
}

// transfer: moves files from one place to another
func transfer(in <-chan Task) <-chan Task {
	out := make(chan Task)

	go func() {
		task := <-in
		// determine whether this is a Globus or http transfer
		// if Globus, start the transfer going there
		// else if http, dispatch to http transfer pool
		out <- task
	}
	return out
}

// extract: extracts selected files from archives
func extract(in <-chan Task) <-chan Task {
	out := make(chan Task)

	go func() {
		task := <-in
		// FIXME: how do we determine which files to extract??
		out <- task
	}
	return out
}

// gather: collects tasks into their constituent transfers
func scatter(in <-chan Task) <-chan Transfer {
	out := make(chan Transfer)
	go func() {
		// FIXME: do we need some sort of ref to the original transfer?
	}
	return out
}

// manifest generation stage (final)
func generateManifest(in <-chan Transfer) <-chan Transfer {
	out := make(chan Transfer)
	go func() {
		transfer := <-in
		out <- transfer
	}
	return out
}
