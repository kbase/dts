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
	if orchestrator.Running {
		return &AlreadyRunningError{}
	}

	// if this is the first call to Start(), register our built-in endpoint and database providers
	if !orchestrator.Started {
		if err := registerEndpointProviders(); err != nil {
			return err
		}
		if err := registerDatabases(); err != nil {
			return err
		}
		orchestrator.Started = true
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

	startOrchestration()

	// okay, we're running now
	orchestrator.Running = true

	return nil
}

// Stops processing tasks. Adding new tasks and requesting task statuses are
// disallowed in a stopped state.
func Stop() error {
	var err error
	if global.Running {
		global.Channels.Stop <- struct{}{}
		err = <-global.Channels.Error
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
	global.Channels.Client.RequestTransfer <- spec
	select {
	case taskId = <-global.Channels.Client.FetchTransferId:
	case err = <-global.Channels.Client.Error:
	}

	return taskId, err
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	var status TransferStatus
	var err error
	global.Channels.Client.RequestStatus <- taskId
	select {
	case status = <-global.Channels.Client.FetchStatus:
	case err = <-global.Channels.Client.Error:
	}
	return status, err
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	var err error
	global.Channels.Client.CancelTransfer <- taskId
	select { // default block provides non-blocking error check
	case err = <-global.Channels.Client.Error:
	default:
	}
	return err
}

//===========
// Internals
//===========

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
// * dispatch: handles all client requests, communicates with other goroutines as needed
// * stageFiles: handles file staging by communicating with provider databases and endpoints
// * transferFiles: handles file transfers by communicatig with provider databases and endpoints
// * sendManifests: generates a transfer manifest after each transfer has completed and sends it to
//                  the correct destination
//
// Transfer Datastore
//
// Transfer information is stored in a centralized data store maintained by a goroutine
// named transferStore, which offers a simple API to create, update, and fetch information
// (see data_store.go):
//
// * createNewTransfer() -> (transferId, numFiles, error)
// * cancelTransfer() -> error
// * getTransferStatus(transferId) -> (TransferStatus, error)
// * stopTransfers() -> error

type orchestratorState struct {
	Channels orchestratorChannels
	Running, Started bool
}

type orchestratorChannels struct {
	Dispatcher dispatcherChannels
	Stager stagerChannels
	Mover moverChannels
	Manifestor manifestorChannels
	Store storeChannels
}

var orchestrator orchestratorState

// channels used internally by non-main goroutines
type internalChannels struct {
	StageFiles       chan []any         // sends file descriptors to stageFiles
	TransferFiles    chan []any         // sends file descriptors to transferFiles
	GenerateManifest chan []any         // sends file descriptors IDs to sendManifests
	UpdateStatus     chan transferInfo  // sends status updates to monitorTransfers
}

func startOrchestration() {
	orchestrator.Channels.Dispatcher = startDispatcher()
	orchestrator.Channels.Stager = startStager()
	orchestrator.Channels.Mover = startMover()
	orchestrator.Channels.Manifestor = startManifestor()
	orchestrator.Channels.Store = startStore()
}

func stopOrchestration() error {
	if err := stopDispatcher(); err != nil {
		return err
	}
	if err := stopStager(); err != nil {
		return err
	}
	if err := startMover(); err != nil {
		return err
	}
	if err := startManifestor(); err != nil {
		return err
	}
	return startStore()
}

func stageFiles() {
	for global.Running {
	}
}

// this goroutine handles the transfer of file payloads
func transferFiles() {
	for global.Running {
	}
}

// this goroutine accepts a set of file descriptors from the transferFiles goroutine, generating a
// manifest from it and sending it along to its destination and updating the transfer status
func sendManifests() {
	for global.Running {
	}
}

// This goroutine maintains transfer statuses, accepting updates from other goroutines and managing
// state transitions.
func updateStatuses() {
	// create or recreate a persistent table of transfer-related tasks
	dataStore := filepath.Join(config.Service.DataDirectory, "dts.gob")
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
			newTask.StartTime = time.Now()
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
							err := journal.RecordTransfer(journal.Record{
								Id:          task.Id,
								Source:      task.Source,
								Destination: task.Destination,
								Orcid:       task.User.Orcid,
								StartTime:   task.StartTime,
								StopTime:    time.Now(),
								Status:      "failed",
								PayloadSize: int64(1024 * 1024 * 1024 * task.PayloadSize), // GB -> B
								NumFiles:    len(task.FileIds),
							})
							if err != nil {
								slog.Error(err.Error())
							}
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

// this function runs in its own goroutine, using the given local endpoint
// for local file transfers, and the given channels to communicate with
// the main thread
func processTasks() {
	// create or recreate a persistent table of transfer-related tasks
	dataStore := filepath.Join(config.Service.DataDirectory, "dts.gob")
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
			newTask.StartTime = time.Now()
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
							err := journal.RecordTransfer(journal.Record{
								Id:          task.Id,
								Source:      task.Source,
								Destination: task.Destination,
								Orcid:       task.User.Orcid,
								StartTime:   task.StartTime,
								StopTime:    time.Now(),
								Status:      "failed",
								PayloadSize: int64(1024 * 1024 * 1024 * task.PayloadSize), // GB -> B
								NumFiles:    len(task.FileIds),
							})
							if err != nil {
								slog.Error(err.Error())
							}
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
