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

package tasks

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

// starts processing tasks according to the given configuration, returning an
// informative error if anything prevents this
func Start() error {
	if running {
		return &AlreadyRunningError{}
	}

	// if this is the first call to Start(), register our built-in endpoint
	// and database providers
	if firstCall {
		// start a transfer log/journal
		handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
		transferJournal = slog.New(handler)

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
		if _, found := config.Databases["jdp"]; found {
			err = databases.RegisterDatabase("jdp", jdp.NewDatabase)
			if err != nil {
				return err
			}
		}
		if _, found := config.Databases["kbase"]; found {
			err = databases.RegisterDatabase("kbase", kbase.NewDatabase)
			if err != nil {
				return err
			}
		}
		if _, found := config.Databases["nmdc"]; found {
			err = databases.RegisterDatabase("nmdc", nmdc.NewDatabase)
			if err != nil {
				return err
			}
		}
		firstCall = false
	}

	// do the necessary directories exist, and are they writable/readable?
	err := validateDirectory("data", config.Service.DataDirectory)
	if err != nil {
		return err
	}
	err = validateDirectory("manifest", config.Service.ManifestDirectory)
	if err != nil {
		return err
	}

	// can we access the local endpoint?
	_, err = endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return err
	}

	// allocate channels
	taskChannels = channelsType{
		CreateTask:       make(chan transferTask, 32),
		CancelTask:       make(chan uuid.UUID, 32),
		GetTaskStatus:    make(chan uuid.UUID, 32),
		ReturnTaskId:     make(chan uuid.UUID, 32),
		ReturnTaskStatus: make(chan TransferStatus, 32),
		Error:            make(chan error, 32),
		Poll:             make(chan struct{}),
		Stop:             make(chan struct{}),
	}

	// start processing tasks
	go processTasks()

	// start the polling heartbeat
	slog.Info(fmt.Sprintf("Task statuses are updated every %d ms",
		config.Service.PollInterval))
	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	go heartbeat(pollInterval, taskChannels.Poll)

	// okay, we're running now
	running = true

	return nil
}

// Stops processing tasks. Adding new tasks and requesting task statuses are
// disallowed in a stopped state.
func Stop() error {
	var err error
	if running {
		taskChannels.Stop <- struct{}{}
		err = <-taskChannels.Error
		running = false
	} else {
		err = &NotRunningError{}
	}
	return err
}

// Returns true if tasks are currently being processed, false if not.
func Running() bool {
	return running
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
	taskChannels.CreateTask <- transferTask{
		User:         spec.User,
		Source:       spec.Source,
		Destination:  spec.Destination,
		FileIds:      spec.FileIds,
		Description:  spec.Description,
		Instructions: spec.Instructions,
	}
	select {
	case taskId = <-taskChannels.ReturnTaskId:
	case err = <-taskChannels.Error:
	}
	return taskId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	taskChannels.GetTaskStatus <- taskId
	select {
	case status = <-taskChannels.ReturnTaskStatus:
	case err = <-taskChannels.Error:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	var err error
	taskChannels.CancelTask <- taskId
	select { // default block provides non-blocking error check
	case err = <-taskChannels.Error:
	default:
	}
	return err
}

//-----------
// Internals
//-----------

// global variables for managing tasks
var firstCall = true            // indicates first call to Start()
var running bool                // true if tasks are processing, false if not
var taskChannels channelsType   // channels used for processing tasks
var stopHeartbeat chan struct{} // send a pulse to this channel to halt polling

var transferJournal *slog.Logger

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func createOrLoadTasks(dataFile string) map[uuid.UUID]transferTask {
	file, err := os.Open(dataFile)
	if err != nil {
		return make(map[uuid.UUID]transferTask)
	}
	slog.Debug(fmt.Sprintf("Found previous tasks in %s.", dataFile))
	defer file.Close()
	enc := gob.NewDecoder(file)
	var tasks map[uuid.UUID]transferTask
	var databaseStates databases.DatabaseSaveStates
	if err = enc.Decode(&tasks); err == nil {
		err = enc.Decode(&databaseStates)
	}
	if err != nil { // file not readable
		slog.Error(fmt.Sprintf("Reading task file %s: %s", dataFile, err.Error()))
		return make(map[uuid.UUID]transferTask)
	}
	if err = databases.Load(databaseStates); err != nil {
		slog.Error(fmt.Sprintf("Restoring database states: %s", err.Error()))
	}
	slog.Debug(fmt.Sprintf("Restored %d tasks from %s", len(tasks), dataFile))
	return tasks
}

// saves a map of task IDs to tasks to the given file
func saveTasks(tasks map[uuid.UUID]transferTask, dataFile string) error {
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

// this type holds various channels used by the task manager to communicate
// with its worker goroutine
type channelsType struct {
	CreateTask       chan transferTask   // used by client to request task creation
	CancelTask       chan uuid.UUID      // used by client to request task cancellation
	GetTaskStatus    chan uuid.UUID      // used by client to request task status
	ReturnTaskId     chan uuid.UUID      // returns task ID to client
	ReturnTaskStatus chan TransferStatus // returns task status to client
	Error            chan error          // returns error to client
	Poll             chan struct{}       // carries heartbeat signal for task updates
	Stop             chan struct{}       // used by client to stop task management
}

// this function runs in its own goroutine, using the given local endpoint
// for local file transfers, and the given channels to communicate with
// the main thread
func processTasks() {
	// create or recreate a persistent table of transfer-related tasks
	var dataStore string
	if config.Service.Name != "" {
		dataStore = filepath.Join(config.Service.DataDirectory,
			fmt.Sprintf("dts-%s.gob", config.Service.Name))
	} else {
		dataStore = filepath.Join(config.Service.DataDirectory, "dts.gob")
	}
	tasks := createOrLoadTasks(dataStore)

	// parse the task channels into directional types as needed
	var createTaskChan <-chan transferTask = taskChannels.CreateTask
	var cancelTaskChan <-chan uuid.UUID = taskChannels.CancelTask
	var getTaskStatusChan <-chan uuid.UUID = taskChannels.GetTaskStatus
	var returnTaskIdChan chan<- uuid.UUID = taskChannels.ReturnTaskId
	var returnTaskStatusChan chan<- TransferStatus = taskChannels.ReturnTaskStatus
	var errorChan chan<- error = taskChannels.Error
	var pollChan <-chan struct{} = taskChannels.Poll
	var stopChan <-chan struct{} = taskChannels.Stop

	// the task deletion period is specified in seconds
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// start scurrying around
	running := true
	for running {
		select {
		case newTask := <-createTaskChan: // Create() called
			newTask.Id = uuid.New()
			tasks[newTask.Id] = newTask
			returnTaskIdChan <- newTask.Id
			slog.Info(fmt.Sprintf("Created new transfer task %s (%d file(s) requested)",
				newTask.Id.String(), len(newTask.FileIds)))
		case taskId := <-cancelTaskChan: // Cancel() called
			if task, found := tasks[taskId]; found {
				slog.Info(fmt.Sprintf("Task %s: received cancellation request", taskId.String()))
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
			err := saveTasks(tasks, dataStore) // don't forget to save our state!
			errorChan <- err
			running = false
		}
	}
}

// this function sends a regular pulse on its poll channel until the global
// variable running is found to be false
func heartbeat(pollInterval time.Duration, pollChan chan<- struct{}) {
	for {
		time.Sleep(pollInterval)
		pollChan <- struct{}{}
		if !running {
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
