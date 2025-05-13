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
	"path/filepath"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
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
	load          chan []byte
	save          chan struct{}
	savedState    chan []byte
	error         chan error
	statusUpdates chan<- TransferStatusUpdate // conveys transfer status updates to a client
}

func CreatePipeline(source, destination string, statusUpdateChan chan<- TransferStatusUpdate) *Pipeline {
	chanBufferSize := 32
	return &Pipeline{
		source:        source,
		destination:   destination,
		create:        make(chan Specification, chanBufferSize),
		cancel:        make(chan uuid.UUID, chanBufferSize),
		error:         make(chan error, chanBufferSize),
		halt:          make(chan struct{}),
		load:          make(chan []byte),
		newTransfer:   make(chan uuid.UUID, chanBufferSize),
		save:          make(chan struct{}),
		savedState:    make(chan []byte),
		statusUpdates: statusUpdateChan,
	}
}

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

// loads a pipeline from the given buffer synchronously, returning any error that occurs
func (p *Pipeline) Load(data []byte) error {
	p.load <- data
	return <-p.error
}

// saves the pipeline synchronously, returning a buffer and any error that occurs
func (p *Pipeline) Save() ([]byte, error) {
	p.save <- struct{}{}
	select {
	case data := <-p.savedState:
		return data, nil
	case err := <-p.error:
		return nil, err
	}
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

// use this in your pipeline's main goroutine to create a new Transfer from a specification
func CreateTransfer(spec Specification) Transfer {
	return Transfer{
		Id:           uuid.New(),
		User:         spec.User,
		Source:       spec.Source,
		Destination:  spec.Destination,
		FileIds:      spec.FileIds,
		Description:  spec.Description,
		Instructions: spec.Instructions,
	}
}

//------
// Task
//------

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

//-----------------
// Pipeline Stages
//-----------------

// Call these functions within your pipeline's goroutine to establish stages. Stages are simply
// goroutines that asynchronously process work, chained together in sequence by their input/output
// channels.

func (p *Pipeline) addCreateStage(in <-chan Specification) <-chan Transfer {
	out := make(chan Transfer)
	go func() {
		for {
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

			// assemble distinct endpoints and create a subtask for each
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
func (p *Pipeline) addScatterStage(in <-chan Transfer) <-chan Task {
	out := make(chan Task)
	go func() {
		for {
			transfer := <-in
			for _, task := range transfer.Tasks {
				out <- task
			}
		}
	}()
	return out
}

// the Prepare stage moves files in a task to a location from which they can be transferred
func (p *Pipeline) addPrepareStage(in <-chan Task) <-chan Task {
	out := make(chan Task)
	go func() {
		for {
			task := <-in
			// check whether files are in place
			out <- task
		}
	}()
	return out
}

// the Transfer stage moves files from one place to another
func (p *Pipeline) addTransferStage(in <-chan Task) <-chan Task {
	out := make(chan Task)

	go func() {
		for {
			task := <-in
			// determine whether this is a Globus or http transfer
			// if Globus, start the transfer going there
			// else if http, dispatch to http transfer pool
			out <- task
		}
	}()
	return out
}

// gather: collects tasks into their constituent transfers
func (p *Pipeline) gather(in <-chan Task) <-chan Transfer {
	out := make(chan Transfer)
	go func() {
		for {
		}
	}()
	return out
}

// manifest generation stage (final)
func generateManifest(in <-chan Transfer) <-chan Transfer {
	out := make(chan Transfer)
	go func() {
		for {
			transfer := <-in
			out <- transfer
		}
	}()
	return out
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
