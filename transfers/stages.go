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
