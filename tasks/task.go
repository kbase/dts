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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// This type tracks the lifecycle of a file transfer task that copies files from
// a source database to a destination database. A transferTask can have one or
// more subtasks, depending on how many transfer endpoints are involved.
type transferTask struct {
	Canceled          bool              // set if a cancellation request has been made
	CompletionTime    time.Time         // time at which the transfer completed
	DataDescriptors   []interface{}     // in-line data descriptors
	Description       string            // Markdown description of the task
	Destination       string            // name of destination database (in config)
	DestinationFolder string            // folder path to which files are transferred
	FileIds           []string          // IDs of all files being transferred
	Id                uuid.UUID         // task identifier
	Instructions      json.RawMessage   // machine-readable task processing instructions
	Manifest          uuid.NullUUID     // manifest generation UUID (if any)
	ManifestFile      string            // name of locally-created manifest file
	PayloadSize       float64           // Size of payload (gigabytes)
	Source            string            // name of source database (in config)
	Status            TransferStatus    // status of file transfer operation
	Subtasks          []transferSubtask // list of constituent file transfer subtasks
	User              auth.User         // info about user requesting transfer
}

// computes the size of a payload for a transfer task (in Gigabytes)
func payloadSize(descriptors []map[string]interface{}) float64 {
	var size uint64
	for _, descriptor := range descriptors {
		size += uint64(descriptor["bytes"].(int))
	}
	return float64(size) / float64(1024*1024*1024)
}

// starts a task going, initiating staging if needed
func (task *transferTask) start() error {
	source, err := databases.NewDatabase(task.Source)
	if err != nil {
		return err
	}

	// resolve resource data using file IDs
	fileDescriptors := make([]map[string]interface{}, 0)
	{
		descriptors, err := source.Descriptors(task.User.Orcid, task.FileIds)
		if err != nil {
			return err
		}

		// sift through the descriptors and separate files from in-line data
		for _, descriptor := range descriptors {
			if _, found := descriptor["path"]; found { // file to be transferred
				fileDescriptors = append(fileDescriptors, descriptor)
			} else if _, found := descriptor["data"]; found { // inline data
				task.DataDescriptors = append(task.DataDescriptors, descriptor)
			} else { // neither!
				return fmt.Errorf("Descriptor '%s' (ID: %s) has no 'path' or 'data' field!",
					descriptor["name"], descriptor["id"])
			}
		}
	}

	// if the database stores its files in more than one location, check that each
	// resource is associated with a valid endpoint
	if len(config.Databases[task.Source].Endpoints) > 1 {
		for _, descriptor := range fileDescriptors {
			id := descriptor["id"].(string)
			endpoint := descriptor["endpoint"].(string)
			if endpoint == "" {
				return databases.ResourceEndpointNotFoundError{
					Database:   task.Source,
					ResourceId: id,
				}
			}
			if _, found := config.Endpoints[endpoint]; !found {
				return databases.InvalidResourceEndpointError{
					Database:   task.Source,
					ResourceId: id,
					Endpoint:   endpoint,
				}
			}
		}
	} else { // otherwise, just assign the database's endpoint to the resources
		for _, descriptor := range fileDescriptors {
			descriptor["endpoint"] = config.Databases[task.Source].Endpoint
		}
	}

	// make sure the size of the payload doesn't exceed our specified limit
	task.PayloadSize = payloadSize(fileDescriptors) // (in GB)
	if task.PayloadSize > config.Service.MaxPayloadSize {
		return &PayloadTooLargeError{Size: task.PayloadSize}
	}

	// determine the destination endpoint
	// FIXME: this conflicts with our redesign!!
	destinationEndpoint := config.Databases[task.Destination].Endpoint

	// construct a destination folder name
	destination, err := databases.NewDatabase(task.Destination)
	if err != nil {
		return err
	}
	username, err := destination.LocalUser(task.User.Orcid)
	if err != nil {
		return err
	}
	task.DestinationFolder = filepath.Join(username, "dts-"+task.Id.String())

	// assemble distinct endpoints and create a subtask for each
	distinctEndpoints := make(map[string]interface{})
	for _, descriptor := range fileDescriptors {
		endpoint := descriptor["endpoint"].(string)
		if _, found := distinctEndpoints[endpoint]; !found {
			distinctEndpoints[endpoint] = struct{}{}
		}
	}
	task.Subtasks = make([]transferSubtask, 0)
	for sourceEndpoint := range distinctEndpoints {
		// pick out the files corresponding to the source endpoint
		// NOTE: this is slow, but preserves file ID ordering
		descriptorsForEndpoint := make([]interface{}, 0)
		for _, descriptor := range fileDescriptors {
			endpoint := descriptor["endpoint"].(string)
			if endpoint == sourceEndpoint {
				descriptorsForEndpoint = append(descriptorsForEndpoint, descriptor)
			}
		}

		// set up a subtask for the endpoint
		task.Subtasks = append(task.Subtasks, transferSubtask{
			Destination:         task.Destination,
			DestinationEndpoint: destinationEndpoint,
			DestinationFolder:   task.DestinationFolder,
			Descriptors:         descriptorsForEndpoint,
			Source:              task.Source,
			SourceEndpoint:      sourceEndpoint,
			User:                task.User,
		})
	}

	// start the subtasks
	for i := range task.Subtasks {
		subErr := task.Subtasks[i].start()
		if subErr != nil {
			err = subErr
		}
	}

	// provisionally, we set the tasks's status to "staging"
	task.Status.Code = TransferStatusStaging
	return err
}

// updates the state of a task, setting its status as necessary
func (task *transferTask) Update() error {
	var err error
	if len(task.Subtasks) == 0 { // new task!
		err = task.start()
	} else if task.Canceled { // cancellation requested
		for i := range task.Subtasks {
			err = task.Subtasks[i].checkCancellation()
		}
		if task.Completed() {
			task.CompletionTime = time.Now()
		}
	} else if task.Manifest.Valid { // we're generating/sending a manifest
		err = task.checkManifest()
	} else { // update subtasks
		// track subtask failures
		var subtaskFailed bool
		var failedSubtaskStatus TransferStatus

		// update each subtask and check for failures
		subtaskStaging := false
		allTransfersSucceeded := true
		for i := range task.Subtasks {
			err := task.Subtasks[i].update()
			if err != nil {
				return err
			}

			if task.Subtasks[i].StagingStatus == databases.StagingStatusFailed {
				subtaskFailed = true
				failedSubtaskStatus.Code = TransferStatusUnknown
				failedSubtaskStatus.Message = "task canceled because of staging failure"
			} else if task.Subtasks[i].TransferStatus.Code == TransferStatusFailed {
				subtaskFailed = true
				failedSubtaskStatus.Code = TransferStatusFailed
				failedSubtaskStatus.Message = "task canceled because of transfer failure"
			}
			if task.Subtasks[i].TransferStatus.Code != TransferStatusSucceeded {
				allTransfersSucceeded = false
			}
		}

		// if a subtask failed, cancel the task -- otherwise, update the task's
		// status based on those of its subtasks
		if subtaskFailed {
			// overwrite only the error code and message fields
			task.Status.Code = failedSubtaskStatus.Code
			task.Status.Message = failedSubtaskStatus.Message
			task.Cancel()
		} else {
			// accumulate statistics
			task.Status.Code = TransferStatusActive
			task.Status.NumFiles = 0
			task.Status.NumFilesTransferred = 0
			task.Status.NumFilesSkipped = 0
			for _, subtask := range task.Subtasks {
				task.Status.NumFiles += subtask.TransferStatus.NumFiles
				if subtask.Staging.Valid {
					subtaskStaging = true
				} else if subtask.Transfer.Valid {
					task.Status.NumFilesTransferred += subtask.TransferStatus.NumFilesTransferred
					task.Status.NumFilesSkipped += subtask.TransferStatus.NumFilesSkipped
				}
			}
		}

		if subtaskStaging && task.Status.NumFilesTransferred == 0 {
			task.Status.Code = TransferStatusStaging
		} else if allTransfersSucceeded { // write a manifest
			localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
			if err != nil {
				return err
			}

			// generate a manifest for the transfer
			manifest := task.createManifest()

			// write the manifest to disk and begin transferring it to the
			// destination endpoint
			task.ManifestFile = filepath.Join(config.Service.ManifestDirectory, fmt.Sprintf("manifest-%s.json", task.Id.String()))
			err = manifest.SaveDescriptor(task.ManifestFile)
			if err != nil {
				return fmt.Errorf("creating manifest file: %s", err.Error())
			}

			// construct the source/destination file manifest paths
			fileXfers := []FileTransfer{
				{
					SourcePath:      task.ManifestFile,
					DestinationPath: filepath.Join(task.DestinationFolder, "manifest.json"),
				},
			}

			// begin transferring the manifest
			// FIXME: how do we determine the database's destination endpoint?
			destinationEndpointName := config.Databases[task.Destination].Endpoint
			destinationEndpoint, err := endpoints.NewEndpoint(destinationEndpointName)
			if err != nil {
				return err
			}
			task.Manifest.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				return fmt.Errorf("transferring manifest file: %s", err.Error())
			}

			task.Status.Code = TransferStatusFinalizing
			task.Manifest.Valid = true
		}
	}
	return err
}

// requests that the task be canceled
func (task *transferTask) Cancel() error {
	task.Canceled = true           // mark as canceled
	for i := range task.Subtasks { // cancel subtasks
		task.Subtasks[i].cancel()
	}
	return nil
}

// returns the duration since the task completed (successfully or otherwise),
// or 0 if the task has not completed
func (task transferTask) Age() time.Duration {
	if task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed {
		return time.Since(task.CompletionTime)
	} else {
		return time.Duration(0)
	}
}

// returns true if the task has completed (successfully or not), false otherwise
func (task transferTask) Completed() bool {
	if task.Status.Code == TransferStatusSucceeded ||
		task.Status.Code == TransferStatusFailed {
		return true
	} else {
		return false
	}
}

// creates a DataPackage that serves as the transfer manifest
func (task *transferTask) createManifest() *datapackage.Package {
	// gather all file and data descriptors
	descriptors := make([]interface{}, 0)
	for _, subtask := range task.Subtasks {
		descriptors = append(descriptors, subtask.Descriptors...)
	}
	for _, dataDescriptor := range task.DataDescriptors {
		descriptors = append(descriptors, dataDescriptor)
	}

	taskUser := map[string]interface{}{
		"title": task.User.Name,
		"role":  "author",
	}
	if task.User.Organization != "" {
		taskUser["organization"] = task.User.Organization
	}
	if task.User.Email != "" {
		taskUser["email"] = task.User.Email
	}

	descriptor := map[string]interface{}{
		"name":      "manifest",
		"resources": descriptors,
		"created":   time.Now().Format(time.RFC3339),
		"profile":   "data-package",
		"keywords":  []interface{}{"dts", "manifest"},
		"contributors": []interface{}{
			taskUser,
		},
		"description":  task.Description,
		"instructions": task.Instructions,
	}

	manifest, err := datapackage.New(descriptor, ".")
	if err != nil {
		slog.Error(err.Error())
	}

	return manifest
}

// checks whether the file manifest for a task has been generated and, if so,
// finalizes the transfer and marks the task as completed
func (task *transferTask) checkManifest() error {
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

		destination, err := databases.NewDatabase(task.Destination)
		if err != nil {
			return err
		}
		err = destination.Finalize(task.User.Orcid, task.Id)
		if err != nil {
			return err
		}

		task.ManifestFile = ""
		task.Status.Code = xferStatus.Code
		task.Status.Message = ""
		task.CompletionTime = time.Now()
	}
	return nil
}
