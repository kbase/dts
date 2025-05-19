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

package pipelines

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//----------
// Pipeline
//----------

// A pipeline consists of a set of goroutines that handle transfers from a specific source to a
// specific destination, on the assumption that this (source, destination) pair uniquely and
// unambiguously determines the set of operations required. Pipelines communicate with the
// dispatch goroutine and the host application via a set of channels.
type Pipeline struct {
	source, destination string

	// channels used for dispatch <-> pipeline communication
	create        chan Specification
	newTransfer   chan uuid.UUID
	cancel        chan uuid.UUID
	halt          chan struct{}
	sink          <-chan Transfer
	error         chan error
	statusUpdates chan<- TransferStatusUpdate // conveys transfer status updates to a client

	// goroutine syncronization
	waitGroup sync.WaitGroup
	running   bool
}

// creates a new pipeline with the given parameters
func NewPipeline(source, destination string, statusUpdateChan chan<- TransferStatusUpdate) *Pipeline {
	chanBufferSize := 32
	return &Pipeline{
		source:      source,
		destination: destination,
		create:      make(chan Specification, chanBufferSize),
		cancel:      make(chan uuid.UUID, chanBufferSize),
		error:       make(chan error, chanBufferSize),
		halt:        make(chan struct{}),
		newTransfer: make(chan uuid.UUID, chanBufferSize),
		// NOTE: sink channel not set--must be set using p.AddFinalStage()
		statusUpdates: statusUpdateChan,
		running:       true,
	}
}

//--------------
// Pipeline API
//--------------

// creates a new Transfer for the pipeline from the given Specification, returning its UUID or a
// non-nil error
func (p *Pipeline) Create(spec Specification) (uuid.UUID, error) {
	p.create <- spec
	select {
	case transferId := <-p.newTransfer:
		return transferId, nil
	case err := <-p.error:
		return uuid.UUID{}, err
	}
}

// cancels the Transfer with the given UUID, returning any error encountered
func (p *Pipeline) Cancel(transferId uuid.UUID) error {
	p.cancel <- transferId
	return <-p.error
}

// halts the Pipeline synchronously, returning any error encountered
func (p *Pipeline) Halt() error {
	p.halt <- struct{}{}
	return <-p.error
}

// call this to update the status of a transfer
func (p *Pipeline) UpdateStatus(id uuid.UUID, status TransferStatus) {
	p.statusUpdates <- TransferStatusUpdate{
		Id:     id,
		Status: status,
	}
}

//----------
// Transfer
//----------

// This type tracks the lifecycle of a file transfer: the copying of files from
// a source database to a destination database. A transfer comprises one or
// more Tasks, depending on how many transfer endpoints are involved.
type Transfer struct {
	DataDescriptors   []any          // in-line data descriptors
	Description       string         // Markdown description of the task
	Destination       string         // name of destination database (in config)
	DestinationFolder string         // folder path to which files are transferred
	FileIds           []string       // IDs of all files being transferred
	Id                uuid.UUID      // task identifier
	Instructions      map[string]any // machine-readable task processing instructions
	Manifest          uuid.NullUUID  // manifest generation UUID (if any)
	ManifestFile      string         // name of locally-created manifest file
	PayloadSize       float64        // Size of payload (gigabytes)
	Source            string         // name of source database (in config)
	Status            TransferStatus // status of file transfer operation
	Tasks             []Task         // list of constituent tasks
	User              auth.User      // info about user requesting transfer
}

//------
// Task
//------

// A Task is an indivisible unit of work that is executed by stages in a pipeline.
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

//-----------------
// Pipeline Stages
//-----------------

// Call these functions within your pipeline's goroutine to establish stages. Stages are simply
// goroutines that asynchronously process work, chained together in sequence by their input/output
// channels.

// this should always be the first stage in your pipeline
func (p *Pipeline) AddCreateStage() <-chan Transfer {
	out := make(chan Transfer)
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		var in <-chan Specification = p.create
		for p.running {
			spec := <-in

			transfer := Transfer{
				Id:           uuid.New(),
				User:         spec.User,
				Source:       spec.Source,
				Destination:  spec.Destination,
				FileIds:      spec.FileIds,
				Description:  spec.Description,
				Instructions: spec.Instructions,
			}

			// access the source database, resolving resource descriptors
			source, err := databases.NewDatabase(spec.Source)
			if err != nil {
				p.error <- err
				continue
			}
			transfer.DataDescriptors = make([]any, 0)
			{
				descriptors, err := source.Descriptors(transfer.User.Orcid, transfer.FileIds)
				if err != nil {
					p.error <- err
					continue
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
					p.error <- err
					continue
				}
			}

			// if the database stores its files in more than one location, check that each
			// resource is associated with a valid endpoint
			if len(config.Databases[transfer.Source].Endpoints) > 1 {
				for _, d := range transfer.DataDescriptors {
					descriptor := d.(map[string]any)
					id := descriptor["id"].(string)
					endpoint := descriptor["endpoint"].(string)
					if endpoint == "" {
						err = &databases.ResourceEndpointNotFoundError{
							Database:   transfer.Source,
							ResourceId: id,
						}
						break
					}
					if _, found := config.Endpoints[endpoint]; !found {
						err = &databases.InvalidResourceEndpointError{
							Database:   transfer.Source,
							ResourceId: id,
							Endpoint:   endpoint,
						}
						break
					}
				}
				if err != nil {
					p.error <- err
					continue
				}
			} else { // otherwise, just assign the database's endpoint to the resources
				for _, d := range transfer.DataDescriptors {
					descriptor := d.(map[string]any)
					descriptor["endpoint"] = config.Databases[transfer.Source].Endpoint
				}
			}

			// make sure the size of the payload doesn't exceed our specified limit
			transfer.PayloadSize = payloadSize(transfer.DataDescriptors) // (in GB)
			if transfer.PayloadSize > config.Service.MaxPayloadSize {
				p.error <- &PayloadTooLargeError{Size: transfer.PayloadSize}
				continue
			}

			// determine the destination endpoint
			// FIXME: this conflicts with our redesign!!
			destinationEndpoint := config.Databases[transfer.Destination].Endpoint

			// construct a destination folder name
			destination, err := databases.NewDatabase(transfer.Destination)
			if err != nil {
				p.error <- err
				continue
			}

			username, err := destination.LocalUser(transfer.User.Orcid)
			if err != nil {
				p.error <- err
				continue
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
			transfer.Tasks = make([]Task, 0)
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
					Destination:         transfer.Destination,
					DestinationEndpoint: destinationEndpoint,
					DestinationFolder:   transfer.DestinationFolder,
					Descriptors:         descriptorsForEndpoint,
					Source:              transfer.Source,
					SourceEndpoint:      sourceEndpoint,
					User:                transfer.User,
				})
			}

			// set a provisional status
			transfer.Status.Code = TransferStatusStaging
			p.UpdateStatus(transfer.Id, transfer.Status)

			out <- transfer
		}
	}()
	return out
}

// the Scatter stage divides a transfer up into a set of tasks
func (p *Pipeline) AddScatterStage(in <-chan Transfer) <-chan Task {
	out := make(chan Task)
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		for p.running {
			transfer := <-in
			for _, task := range transfer.Tasks {
				out <- task
			}
		}
	}()
	return out
}

// the Prepare stage moves files in a task to a location from which they can be transferred
func (p *Pipeline) AddPrepareStage(in <-chan Task) <-chan Task {
	out := make(chan Task)
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		for p.running {
			task := <-in
			// check whether files are in place
			sourceEndpoint, err := endpoints.NewEndpoint(task.SourceEndpoint)
			if err != nil {
				p.error <- err
				continue
			}
			staged, err := sourceEndpoint.FilesStaged(task.Descriptors)
			if err != nil {
				p.error <- err
				continue
			}

			if !staged {
				// tell the source DB to stage the files, stash the task, and return
				// its new ID
				source, err := databases.NewDatabase(task.Source)
				if err != nil {
					p.error <- err
					continue
				}
				fileIds := make([]string, len(task.Descriptors))
				for i, d := range task.Descriptors {
					descriptor := d.(map[string]any)
					fileIds[i] = descriptor["id"].(string)
				}
				taskId, err := source.StageFiles(task.User.Orcid, fileIds)
				if err != nil {
					p.error <- err
					continue
				}
				task.Staging = uuid.NullUUID{
					UUID:  taskId,
					Valid: true,
				}
				task.TransferStatus = TransferStatus{
					Code:     TransferStatusStaging,
					NumFiles: len(task.Descriptors),
				}
			}
			out <- task
		}
	}()
	return out
}

func (p *Pipeline) AddGlobusTransferStage(in <-chan Task) <-chan Task {
	out := make(chan Task)

	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		for p.running {
			task := <-in
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
				p.error <- err
				continue
			}
			destinationEndpoint, err := endpoints.NewEndpoint(task.DestinationEndpoint)
			if err != nil {
				p.error <- err
				continue
			}
			transferId, err := sourceEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				p.error <- err
				continue
			}
			task.Transfer = uuid.NullUUID{
				UUID:  transferId,
				Valid: true,
			}
			task.TransferStatus = TransferStatus{
				Code:     TransferStatusActive,
				NumFiles: len(task.Descriptors),
			}
			task.Staging = uuid.NullUUID{}
			out <- task
		}
	}()
	return out
}

// gather: collects tasks into their constituent transfers
func (p *Pipeline) AddGatherStage(in <-chan Task) <-chan Transfer {
	out := make(chan Transfer)
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		for p.running {
		}
	}()
	return out
}

// manifest generation stage
func (p *Pipeline) AddManifestStage(in <-chan Transfer) <-chan Transfer {
	out := make(chan Transfer)
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		for p.running {
			transfer := <-in

			localEndpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
			if err != nil {
				p.error <- err
				continue
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
				"title": transfer.User.Name,
				"role":  "author",
			}
			if transfer.User.Organization != "" {
				transferUser["organization"] = transfer.User.Organization
			}
			if transfer.User.Email != "" {
				transferUser["email"] = transfer.User.Email
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
				"description":  transfer.Description,
				"instructions": transfer.Instructions,
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
				p.error <- fmt.Errorf("creating manifest file: %s", err.Error())
				continue
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
			destinationEndpointName := config.Databases[transfer.Destination].Endpoint
			destinationEndpoint, err := endpoints.NewEndpoint(destinationEndpointName)
			if err != nil {
				p.error <- err
				continue
			}
			transfer.Manifest.UUID, err = localEndpoint.Transfer(destinationEndpoint, fileXfers)
			if err != nil {
				p.error <- fmt.Errorf("transferring manifest file: %s", err.Error())
				continue
			}

			transfer.Status.Code = TransferStatusFinalizing
			transfer.Manifest.Valid = true
			p.UpdateStatus(transfer.Id, transfer.Status)
			out <- transfer
		}
	}()
	return out
}

// call this to add the final stage (sink) to a pipeline
func (p *Pipeline) AddFinalStage(in <-chan Transfer) {
	p.sink = in
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
