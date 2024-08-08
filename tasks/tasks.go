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
	"encoding/json"
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
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
	"github.com/kbase/dts/endpoints/local"
	"github.com/kbase/dts/frictionless"
)

// useful type aliases
type Database = databases.Database
type DataPackage = frictionless.DataPackage
type DataResource = frictionless.DataResource
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

// this type holds a specification used to create a valid transfer task
type Specification struct {
	// information about the user requesting the task
	UserInfo auth.UserInfo
	// the name of source database from which files are transferred (as specified
	// in the DTS config file)
	Source string
	// the name of destination database from which files are transferred (as
	// specified in the DTS config file)
	Destination string
	// an array of identifiers for files to be transferred from Source to
	// Destination
	FileIds []string
	// a Markdown description of the transfer task (can contain machine-readable
	// instructions for processing the payload at its destination)
	Description string
}

// this type holds multiple (possibly null) UUIDs corresponding to different
// states in the file transfer lifecycle
type taskType struct {
	Id                  uuid.UUID      // task identifier
	DestinationFolder   string         // folder path to which files are transferred
	UserInfo            auth.UserInfo  // info about user requesting transfer
	Source, Destination string         // names of source and destination databases
	FileIds             []string       // IDs of files within Source
	Description         string         // Markdown description of the task
	Resources           []DataResource // Frictionless DataResources for files
	PayloadSize         float64        // Size of payload (gigabytes)
	Canceled            bool           // set if a cancellation request has been made
	Staging, Transfer   uuid.NullUUID  // staging and file transfer UUIDs (if any)
	Manifest            uuid.NullUUID  // manifest generation UUID (if any)
	ManifestFile        string         // name of locally-created manifest file
	Status              TransferStatus // status of file transfer operation
	CompletionTime      time.Time      // time at which the transfer completed
}

// This error type is returned when a payload is requested that is too large.
type PayloadTooLargeError struct {
	size float64 // size of the requested payload in gigabytes
}

func (e PayloadTooLargeError) Error() string {
	return fmt.Sprintf("Requested payload is too large: %g GB (limit is %g GB).",
		e.size, config.Service.MaxPayloadSize)
}

// computes the size of a payload for a transfer task (in gigabytes)
func payloadSize(resources []DataResource) float64 {
	var size uint64
	for _, resource := range resources {
		size += uint64(resource.Bytes)
	}
	return float64(size) / float64(1024*1024*1024)
}

// starts a task going, initiating staging if needed
func (task *taskType) start() error {
	source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
	if err != nil {
		return err
	}

	// resolve file paths using file IDs
	task.Resources, err = source.Resources(task.FileIds)
	if err != nil {
		return err
	}

	// make sure the size of the payload doesn't exceed our specified limit
	task.PayloadSize = payloadSize(task.Resources) // (in GB)
	if task.PayloadSize > config.Service.MaxPayloadSize {
		return &PayloadTooLargeError{size: task.PayloadSize}
	}

	// are the files already staged? (only works for public data)
	sourceEndpoint, err := source.Endpoint()
	if err != nil {
		return err
	}
	staged, err := sourceEndpoint.FilesStaged(task.Resources)
	if err != nil {
		return err
	}

	if staged {
		err = task.beginTransfer()
	} else {
		// tell the source DB to stage the files, stash the task, and return
		// its new ID
		task.Staging.UUID, err = source.StageFiles(task.FileIds)
		task.Staging.Valid = true
		if err != nil {
			return err
		}
		task.Status = TransferStatus{
			Code:     TransferStatusStaging,
			NumFiles: len(task.FileIds),
		}
	}
	return err
}

// updates the status of a canceled task depending on where it is in its
// lifecycle
func (task *taskType) checkCancellation() error {
	if task.Transfer.Valid {
		// the task's status is the same as its transfer status
		source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
		if err != nil {
			return err
		}
		endpoint, err := source.Endpoint()
		if err != nil {
			return err
		}
		task.Status, err = endpoint.Status(task.Id)
		return err
	} else {
		// at any other point in the lifecycle, terminate the task
		task.Status.Code = TransferStatusFailed
		task.Status.Message = "Task canceled at user request"
	}
	if task.Completed() {
		task.CompletionTime = time.Now()
	}
	return nil
}

// initiates a file transfer on a set of staged files
func (task *taskType) beginTransfer() error {
	source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
	if err != nil {
		return err
	}
	destination, err := databases.NewDatabase(task.UserInfo.Orcid, task.Destination)
	if err != nil {
		return err
	}

	// construct the source/destination file paths
	username, err := destination.LocalUser(task.UserInfo.Orcid)
	if err != nil {
		return err
	}
	task.DestinationFolder = filepath.Join(username, "dts-"+task.Id.String())
	fileXfers := make([]FileTransfer, len(task.Resources))
	for i, resource := range task.Resources {
		destinationPath := filepath.Join(task.DestinationFolder, resource.Path)
		fileXfers[i] = FileTransfer{
			SourcePath:      resource.Path,
			DestinationPath: destinationPath,
			Hash:            resource.Hash,
		}
	}

	// initiate the transfer
	sourceEndpoint, err := source.Endpoint()
	if err != nil {
		return err
	}
	destinationEndpoint, err := destination.Endpoint()
	if err != nil {
		return err
	}
	task.Transfer.UUID, err = sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
	if err != nil {
		return err
	}

	task.Status = TransferStatus{
		Code:     TransferStatusActive,
		NumFiles: len(task.FileIds),
	}
	task.Staging = uuid.NullUUID{}
	task.Transfer.Valid = true
	return nil
}

// checks whether files for a task are finished staging and, if so,
// initiates the transfer process
func (task *taskType) checkStaging() error {
	source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
	if err != nil {
		return err
	}
	// check with the database first to see whether the files are staged
	stagingStatus, err := source.StagingStatus(task.Staging.UUID)
	if err != nil {
		return err
	}

	if stagingStatus == databases.StagingStatusSucceeded { // staged!
		return task.beginTransfer() // move along
	} else if stagingStatus == databases.StagingStatusFailed {
		// staging failed, so cancel the task
		task.Cancel()
		task.Status.Code = TransferStatusUnknown
		task.Status.Message = "task cancelled because of staging failure"
	}
	return nil
}

// creates a DataPackage that serves as the transfer manifest
func (task *taskType) createManifest() frictionless.DataPackage {
	manifest := DataPackage{
		Name:      "manifest",
		Resources: make([]DataResource, len(task.Resources)),
		Created:   time.Now().Format(time.RFC3339),
		Profile:   "data-package",
		Keywords:  []string{"dts", "manifest"},
		Contributors: []frictionless.Contributor{
			{
				Title:        task.UserInfo.Name,
				Email:        task.UserInfo.Email,
				Role:         "author",
				Organization: task.UserInfo.Organization,
			},
		},
	}
	copy(manifest.Resources, task.Resources)
	return manifest
}

// checks whether files for a task are finished transferring and, if so,
// initiates the generation of the file manifest
func (task *taskType) checkTransfer() error {
	// has the data transfer completed?
	source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
	if err != nil {
		return err
	}
	sourceEndpoint, err := source.Endpoint()
	if err != nil {
		return err
	}
	xferStatus, err := sourceEndpoint.Status(task.Transfer.UUID)
	if err != nil {
		return err
	}
	if xferStatus.Code == TransferStatusSucceeded ||
		xferStatus.Code == TransferStatusFailed { // transfer finished
		task.Transfer = uuid.NullUUID{}
		if xferStatus.Code == TransferStatusSucceeded {
			localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
			if err != nil {
				return err
			}
			// generate a manifest for the transfer
			manifest := task.createManifest()

			// write the manifest to disk and begin transferring it to the
			// destination endpoint
			var manifestBytes []byte
			manifestBytes, err = json.Marshal(manifest)
			if err != nil {
				return fmt.Errorf("marshalling manifest content: %s", err.Error())
			}
			var manifestFile *os.File
			manifestFile, err = os.CreateTemp(config.Service.ManifestDirectory,
				"manifest.json")
			if err != nil {
				return fmt.Errorf("creating manifest file: %s", err.Error())
			}
			_, err = manifestFile.Write(manifestBytes)
			if err != nil {
				return fmt.Errorf("writing manifest file content: %s", err.Error())
			}
			task.ManifestFile = manifestFile.Name()
			err = manifestFile.Close()
			if err != nil {
				return fmt.Errorf("closing manifest file: %s", err.Error())
			}

			// construct the source/destination file manifest paths
			destination, err := databases.NewDatabase(task.UserInfo.Orcid, task.Destination)
			if err != nil {
				return err
			}
			fileXfers := []FileTransfer{
				{
					SourcePath:      task.ManifestFile,
					DestinationPath: filepath.Join(task.DestinationFolder, "manifest.json"),
				},
			}

			// begin transferring the manifest
			destinationEndpoint, err := destination.Endpoint()
			if err != nil {
				return err
			}
			task.Manifest.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				return fmt.Errorf("transferring manifest file: %s", err.Error())
			}

			task.Status = TransferStatus{
				Code: TransferStatusFinalizing,
			}
			task.Transfer.Valid = false
			task.Manifest.Valid = true
		} else {
			// the transfer failed, so make sure we cancel it in case it's still
			// trying (because e.g. Globus continues trying transfers for ~3 days!!)
			task.Cancel()
		}
	}
	return nil
}

// checks whether the file manifest for a task has been generated and, if so,
// marks the task as completed
func (task *taskType) checkManifest() error {
	// has the manifest transfer completed?
	localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return err
	}
	xferStatus, err := localEndpoint.Status(task.Manifest.UUID)
	if err != nil {
		return err
	}
	if xferStatus.Code == TransferStatusSucceeded ||
		xferStatus.Code == TransferStatusFailed { // manifest transferred
		task.Manifest = uuid.NullUUID{}
		os.Remove(task.ManifestFile)
		task.ManifestFile = ""
		task.Status.Code = xferStatus.Code
		task.Status.Message = ""
		task.CompletionTime = time.Now()
	}
	return nil
}

// returns the duration since the task completed (successfully or otherwise),
// or 0 if the task has not completed
func (task taskType) Age() time.Duration {
	if task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed {
		return time.Since(task.CompletionTime)
	} else {
		return time.Duration(0)
	}
}

// returns true if the task has completed (successfully or not), false otherwise
func (task taskType) Completed() bool {
	return task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed
}

// requests that the task be canceled
func (task *taskType) Cancel() error {
	task.Canceled = true // mark as canceled

	if task.Transfer.Valid { // we're transferring
		// fetch the source endpoint
		var endpoint endpoints.Endpoint
		source, err := databases.NewDatabase(task.UserInfo.Orcid, task.Source)
		if err != nil {
			return err
		}
		endpoint, err = source.Endpoint()
		if err != nil {
			return err
		}
		// request that the task be canceled using its UUID
		return endpoint.Cancel(task.Transfer.UUID)
	}
	return nil
}

// updates the state of a task, setting its status as necessary
func (task *taskType) Update() error {
	var err error
	if task.Resources == nil { // new task!
		err = task.start()
	} else if task.Canceled { // cancellation requested
		err = task.checkCancellation()
	} else if task.Staging.Valid { // we're staging
		err = task.checkStaging()
	} else if task.Transfer.Valid { // we're transferring
		err = task.checkTransfer()
	} else if task.Manifest.Valid { // we're generating/sending a manifest
		err = task.checkManifest()
	}
	return err
}

// loads a map of task IDs to tasks from a previously saved file if available,
// or creates an empty map if no such file is available or valid
func createOrLoadTasks(dataFile string) map[uuid.UUID]taskType {
	file, err := os.Open(dataFile)
	if err != nil {
		return make(map[uuid.UUID]taskType)
	}
	slog.Debug(fmt.Sprintf("Found previous tasks in %s.", dataFile))
	defer file.Close()
	enc := gob.NewDecoder(file)
	var tasks map[uuid.UUID]taskType
	err = enc.Decode(&tasks)
	if err != nil { // file not readable
		slog.Error(fmt.Sprintf("Reading task file %s: %s", dataFile, err.Error()))
		return make(map[uuid.UUID]taskType)
	}
	slog.Debug(fmt.Sprintf("Restored %d tasks from %s", len(tasks), dataFile))
	return tasks
}

// saves a map of task IDs to tasks to the given file
func saveTasks(tasks map[uuid.UUID]taskType, dataFile string) error {
	if len(tasks) > 0 {
		slog.Debug(fmt.Sprintf("Saving %d tasks to %s", len(tasks), dataFile))
		file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("Opening task file %s: %s", dataFile, err.Error())
		}
		enc := gob.NewEncoder(file)
		err = enc.Encode(tasks)
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
	CreateTask       chan taskType       // used by client to request task creation
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
// the TaskManager
func processTasks() {
	// create or recreate a persistent table of transfer-related tasks
	dataStore := filepath.Join(config.Service.DataDirectory, "dts.gob")
	tasks := createOrLoadTasks(dataStore)

	// parse the task channels into directional types as needed
	var createTaskChan <-chan taskType = taskChannels.CreateTask
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
	for {
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
				err := NotFoundError{Id: taskId}
				errorChan <- err
			}
		case taskId := <-getTaskStatusChan: // Status() called
			if task, found := tasks[taskId]; found {
				returnTaskStatusChan <- task.Status
			} else {
				err := NotFoundError{Id: taskId}
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
								task.Id.String(), len(task.Resources), task.PayloadSize))
						case TransferStatusActive:
							slog.Info(fmt.Sprintf("Task %s: beginning transfer (%d file(s), %g GB)",
								task.Id.String(), len(task.Resources), task.PayloadSize))
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
			break
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

// global variables for managing tasks
var firstCall = true            // indicates first call to Start()
var running bool                // true if tasks are processing, false if not
var taskChannels channelsType   // channels used for processing tasks
var stopHeartbeat chan struct{} // send a pulse to this channel to halt polling

// This error type is returned when a task is sought but not found.
type NotFoundError struct {
	Id uuid.UUID
}

func (t NotFoundError) Error() string {
	return fmt.Sprintf("The task %s was not found.", t.Id.String())
}

// This error type is returned if Start() is called when tasks are being
// processed
type AlreadyRunningError struct{}

func (t AlreadyRunningError) Error() string {
	return fmt.Sprintf("Tasks are already running and cannot be started again.")
}

// This error type is returned if Stop() is called when tasks are not being
// processed
type NotRunningError struct{}

func (t NotRunningError) Error() string {
	return fmt.Sprintf("Tasks are not currently being processed.")
}

// Starts processing tasks according to the given configuration, returning an
// informative error if anything prevents this.
func Start() error {
	if running {
		return AlreadyRunningError{}
	}

	// if this is the first call to Start(), register our built-in endpoint
	// and database providers
	if firstCall {
		endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint)
		endpoints.RegisterEndpointProvider("local", local.NewEndpoint)
		databases.RegisterDatabase("jdp", jdp.NewDatabase)
		databases.RegisterDatabase("kbase", kbase.NewDatabase)
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
		CreateTask:       make(chan taskType, 32),
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
		err = NotRunningError{}
	}
	return err
}

// Returns true if tasks are currently being processed, false if not.
func Running() bool {
	return running
}

// Creates a new transfer task associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	var taskId uuid.UUID

	// verify that we can fetch the task's source and destination databases
	// without incident
	_, err := databases.NewDatabase(spec.UserInfo.Orcid, spec.Source)
	if err != nil {
		return taskId, err
	}
	_, err = databases.NewDatabase(spec.UserInfo.Orcid, spec.Destination)
	if err != nil {
		return taskId, err
	}

	// create a new task and send it along for processing
	taskChannels.CreateTask <- taskType{
		UserInfo:    spec.UserInfo,
		Source:      spec.Source,
		Destination: spec.Destination,
		FileIds:     spec.FileIds,
		Description: spec.Description,
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
