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
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// This type tracks subtasks within a transfer (e.g. files transferred from
// multiple endpoints attached to a single source/destination database pair).
// It holds multiple (possibly null) UUIDs corresponding to different
// states in the file transfer lifecycle
type transferSubtask struct {
	Destination         string                  // name of destination database (in config)
	DestinationEndpoint string                  // name of destination database (in config)
	DestinationFolder   string                  // folder path to which files are transferred
	Resources           []DataResource          // Frictionless DataResources for files
	Source              string                  // name of source database (in config)
	SourceEndpoint      string                  // name of source endpoint (in config)
	Staging             uuid.NullUUID           // staging UUID (if any)
	StagingStatus       databases.StagingStatus // staging status
	Transfer            uuid.NullUUID           // file transfer UUID (if any)
	TransferStatus      TransferStatus          // status of file transfer operation
	UserInfo            auth.UserInfo           // info about user requesting transfer
}

func (subtask *transferSubtask) start() error {
	// are the files already staged? (only works for public data)
	sourceEndpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
	if err != nil {
		return err
	}
	staged, err := sourceEndpoint.FilesStaged(subtask.Resources)
	if err != nil {
		return err
	}

	if staged {
		err = subtask.beginTransfer()
	} else {
		// tell the source DB to stage the files, stash the task, and return
		// its new ID
		source, err := databases.NewDatabase(subtask.UserInfo.Orcid, subtask.Source)
		if err != nil {
			return err
		}
		fileIds := make([]string, len(subtask.Resources))
		for i, resource := range subtask.Resources {
			fileIds[i] = resource.Id
		}
		taskId, err := source.StageFiles(fileIds)
		if err != nil {
			return err
		}
		subtask.Staging = uuid.NullUUID{
			UUID:  taskId,
			Valid: true,
		}
		subtask.TransferStatus = TransferStatus{
			Code:     TransferStatusStaging,
			NumFiles: len(subtask.Resources),
		}
	}
	return err
}

// updates the state of a subtask, setting its status as necessary
func (subtask *transferSubtask) update() error {
	var err error
	if subtask.Staging.Valid { // we're staging
		err = subtask.checkStaging()
	} else if subtask.Transfer.Valid { // we're transferring
		err = subtask.checkTransfer()
	}
	return err
}

// checks whether files for a subtask are finished staging and, if so,
// initiates the transfer process
func (subtask *transferSubtask) checkStaging() error {
	source, err := databases.NewDatabase(subtask.UserInfo.Orcid, subtask.Source)
	if err != nil {
		return err
	}
	// check with the database first to see whether the files are staged
	subtask.StagingStatus, err = source.StagingStatus(subtask.Staging.UUID)
	if err != nil {
		return err
	}

	if subtask.StagingStatus == databases.StagingStatusSucceeded { // staged!
		if config.Service.DoubleCheckStaging {
			// the database thinks the files are staged. Does its endpoint agree?
			endpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
			if err != nil {
				return err
			}
			staged, err := endpoint.FilesStaged(subtask.Resources)
			if err != nil {
				return err
			}
			if !staged {
				return fmt.Errorf("Database %s reports staged files, but endpoint %s cannot see them. Is the endpoint's root set properly?",
					subtask.Source, subtask.SourceEndpoint)
			}
		}
		return subtask.beginTransfer() // move along
	}
	return nil
}

// checks whether files for a task are finished transferring and, if so,
// initiates the generation of the file manifest
func (subtask *transferSubtask) checkTransfer() error {
	// has the data transfer completed?
	sourceEndpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
	if err != nil {
		return err
	}
	subtask.TransferStatus, err = sourceEndpoint.Status(subtask.Transfer.UUID)
	if err != nil {
		return err
	}
	if subtask.TransferStatus.Code == TransferStatusSucceeded ||
		subtask.TransferStatus.Code == TransferStatusFailed { // transfer finished
		subtask.Transfer = uuid.NullUUID{}
	}
	return nil
}

// issues a cancellation request to the endpoint associated with the subtask
func (subtask *transferSubtask) cancel() error {
	if subtask.Transfer.Valid { // we're transferring
		// fetch the source endpoint
		endpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
		if err != nil {
			return err
		}

		// request that the task be canceled using its UUID
		return endpoint.Cancel(subtask.Transfer.UUID)
	}
	return nil
}

// updates the status of a canceled subtask depending on where it is in its
// lifecycle
func (subtask *transferSubtask) checkCancellation() error {
	if subtask.Transfer.Valid {
		endpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
		if err != nil {
			return err
		}
		subtask.TransferStatus, err = endpoint.Status(subtask.Transfer.UUID)
		return err
	}

	// at any other point in the lifecycle, terminate the task
	subtask.TransferStatus.Code = TransferStatusFailed
	subtask.TransferStatus.Message = "Task canceled at user request"
	return nil
}

// initiates a file transfer on a set of staged files
func (subtask *transferSubtask) beginTransfer() error {
	slog.Debug(fmt.Sprintf("Transferring %d file(s) from %s to %s",
		len(subtask.Resources), subtask.SourceEndpoint, subtask.DestinationEndpoint))
	// assemble a list of file transfers
	fileXfers := make([]FileTransfer, len(subtask.Resources))
	for i, resource := range subtask.Resources {
		destinationPath := filepath.Join(subtask.DestinationFolder, resource.Path)
		fileXfers[i] = FileTransfer{
			SourcePath:      resource.Path,
			DestinationPath: destinationPath,
			Hash:            resource.Hash,
		}
	}

	// initiate the transfer
	sourceEndpoint, err := endpoints.NewEndpoint(subtask.SourceEndpoint)
	if err != nil {
		return err
	}
	destinationEndpoint, err := endpoints.NewEndpoint(subtask.DestinationEndpoint)
	if err != nil {
		return err
	}
	transferId, err := sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
	if err != nil {
		return err
	}
	subtask.Transfer = uuid.NullUUID{
		UUID:  transferId,
		Valid: true,
	}
	subtask.TransferStatus = TransferStatus{
		Code:     TransferStatusActive,
		NumFiles: len(subtask.Resources),
	}
	subtask.Staging = uuid.NullUUID{}
	return nil
}
