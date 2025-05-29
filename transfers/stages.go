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
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/deliveryhero/pipeline/v2"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//-----------------
// Pipeline Stages
//-----------------

// Call these functions within your pipeline's goroutine to establish stages. Stages are simply
// goroutines that asynchronously process work, chained together in sequence by their input/output
// channels.

type IdAndSpecification struct {
	Id            uuid.UUID
	Specification Specification
}

// this stage creates a Transfer from an ID and Specification
func CreateTransfer() pipeline.Processor[IdAndSpecification, Transfer] {
	process := func(ctx context.Context, IdAndSpec IdAndSpecification) (Transfer, error) {

		// access the source database, resolving resource descriptors
		source, err := databases.NewDatabase(IdAndSpec.Specification.Source)
		if err != nil {
			return Transfer{}, err
		}

		transfer := Transfer{
			Id:            IdAndSpec.Id,
			Specification: IdAndSpec.Specification,
			Status: TransferStatus{
				Code:     TransferStatusNew,
				NumFiles: len(IdAndSpec.Specification.FileIds),
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
	cancel := func(spec Specification, err error) {
	}
	return pipeline.NewProcessor(process, cancel)
}

// moves files into place at the source, for transfer elsewhere
func StageFilesAtSource(statusUpdate chan<- TransferStatusUpdate) pipeline.Processor[Task, Task] {
	process := func(ctx context.Context, task Task) (Task, error) {
		// check whether files are in place
		sourceEndpoint, err := endpoints.NewEndpoint(task.SourceEndpoint)
		if err != nil {
			return Task{}, err
		}
		staged, err := sourceEndpoint.FilesStaged(task.Descriptors)
		if err != nil {
			return Task{}, err
		}

		if !staged {
			// tell the source DB to stage the files, stash the task, and return
			// its new ID
			source, err := databases.NewDatabase(task.Source)
			if err != nil {
				return Task{}, err
			}
			fileIds := make([]string, len(task.Descriptors))
			for i, d := range task.Descriptors {
				descriptor := d.(map[string]any)
				fileIds[i] = descriptor["id"].(string)
			}
			taskId, err := source.StageFiles(task.User.Orcid, fileIds)
			if err != nil {
				return Task{}, err
			}
			task.Status.StagingId = uuid.NullUUID{
				UUID:  taskId,
				Valid: true,
			}
			task.Status.TransferStatus = endpoints.TransferStatus{
				Code:     endpoints.TransferStatusStaging,
				NumFiles: len(task.Descriptors),
			}
		}
		return task, nil
	}
	cancel := func(task Task, err error) {
	}
	return pipeline.NewProcessor(process, cancel)
}

func TransferToDestination(statusUpdate chan<- TransferStatusUpdate) pipeline.Processor[Task, Task] {
	process := func(ctx context.Context, task Task) (Task, error) {
		slog.Debug(fmt.Sprintf("Transferring %d file(s) from %s to %s",
			len(task.Descriptors), task.SourceEndpoint, task.DestinationEndpoint))

		// assemble a list of file transfers
		fileXfers := make([]FileTransfer, len(task.Descriptors))
		for i, d := range task.Descriptors {
			descriptor := d.(map[string]any)
			path := descriptor["path"].(string)
			destinationPath := filepath.Join(task.DestinationFolder, path)
			fileXfers[i] = FileTransfer{
				SourcePath:      path,
				DestinationPath: destinationPath,
				Hash:            descriptor["hash"].(string),
			}
		}

		// initiate the transfer
		sourceEndpoint, err := endpoints.NewEndpoint(task.SourceEndpoint)
		if err != nil {
			return Task{}, err
		}
		destinationEndpoint, err := endpoints.NewEndpoint(task.DestinationEndpoint)
		if err != nil {
			return Task{}, err
		}
		transferId, err := sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
		if err != nil {
			return Task{}, err
		}
		task.Status.TransferId = uuid.NullUUID{
			UUID:  transferId,
			Valid: true,
		}
		task.Status.TransferStatus = endpoints.TransferStatus{
			Code:     endpoints.TransferStatusActive,
			NumFiles: len(task.Descriptors),
		}
		task.Status.StagingId = uuid.NullUUID{}
		return task, nil
	}
	cancel := func(task Task, err error) {
	}
	return pipeline.NewProcessor(process, cancel)
}

// manifest generation stage
func GenerateManifest() pipeline.Processor[Transfer, Transfer] {
	process := func(ctx context.Context, transfer Transfer) (Transfer, error) {
		localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
		if err != nil {
			return Transfer{}, nil
		}

		// generate a manifest for the transfer
		descriptors := make([]any, 0)
		for _, task := range transfer.Tasks {
			descriptors = append(descriptors, task.Descriptors...)
		}
		for _, dataDescriptor := range transfer.DataDescriptors {
			descriptors = append(descriptors, dataDescriptor)
		}

		transferUser := map[string]any{
			"title": transfer.Specification.User.Name,
			"role":  "author",
		}
		if transfer.Specification.User.Organization != "" {
			transferUser["organization"] = transfer.Specification.User.Organization
		}
		if transfer.Specification.User.Email != "" {
			transferUser["email"] = transfer.Specification.User.Email
		}

		descriptor := map[string]any{
			"name":      "manifest",
			"resources": descriptors,
			"created":   time.Now().Format(time.RFC3339),
			"profile":   "data-package",
			"keywords":  []any{"dts", "manifest"},
			"contributors": []any{
				transferUser,
			},
			"description":  transfer.Specification.Description,
			"instructions": transfer.Specification.Instructions,
		}

		manifest, err := datapackage.New(descriptor, ".")
		if err != nil {
			slog.Error(err.Error())
		}

		// write the manifest to disk and begin transferring it to the
		// destination endpoint
		transfer.ManifestFile = filepath.Join(config.Service.ManifestDirectory, fmt.Sprintf("manifest-%s.json", transfer.Id.String()))
		err = manifest.SaveDescriptor(transfer.ManifestFile)
		if err != nil {
			return Transfer{}, fmt.Errorf("creating manifest file: %s", err.Error())
		}

		// construct the source/destination file manifest paths
		fileXfers := []FileTransfer{
			{
				SourcePath:      transfer.ManifestFile,
				DestinationPath: filepath.Join(transfer.DestinationFolder, "manifest.json"),
			},
		}

		// begin transferring the manifest
		// FIXME: how do we determine the database's destination endpoint?
		destinationEndpointName := config.Databases[transfer.Specification.Destination].Endpoint
		destinationEndpoint, err := endpoints.NewEndpoint(destinationEndpointName)
		if err != nil {
			return Transfer{}, err
		}
		transfer.Status.ManifestId.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
		if err != nil {
			return Transfer{}, fmt.Errorf("transferring manifest file: %s", err.Error())
		}

		transfer.Status.Code = TransferStatusFinalizing
		transfer.Status.ManifestId.Valid = true
		return transfer, nil
	}
	cancel := func(transfer Transfer, err error) {
	}
	return pipeline.NewProcessor(process, cancel)
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
