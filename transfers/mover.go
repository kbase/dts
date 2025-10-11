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
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/endpoints"
)

//-------
// Mover
//-------

// The mover manages actual file transfer operations and cancellations.

// mover global state
var mover moverState

type moverState struct {
	Channels  moverChannels
	Endpoints map[string]endpoints.Endpoint
}

type moverChannels struct {
	RequestMove  chan moveRequest
	ReturnMoveId chan uuid.UUID

	RequestStatus chan uuid.UUID
	ReturnStatus  chan TransferStatus

	Error chan error
	Stop  chan struct{}
}

type moveRequest struct {
	Descriptors         []map[string]any
	Source, Destination string
}

// starts the mover
func (m *moverState) Start() error {
	m.Channels = moverChannels{
		RequestMove:   make(chan moveRequest, 32),
		ReturnMoveId:  make(chan uuid.UUID, 32),
		RequestStatus: make(chan uuid.UUID, 32),
		ReturnStatus:  make(chan TransferStatus, 32),
		Error:         make(chan error, 32),
		Stop:          make(chan struct{}),
	}
	m.Endpoints = make(map[string]endpoints.Endpoint)
	go m.process()
	return nil
}

// stops the store goroutine
func (m *moverState) Stop() error {
	m.Channels.Stop <- struct{}{}
	return <-m.Channels.Error
}

func (m *moverState) Move(descriptors []map[string]any, source, destination string) (uuid.UUID, error) {
	m.Channels.RequestMove <- moveRequest{
		Descriptors: descriptors,
		Source:      source,
		Destination: destination,
	}
	select {
	case id := <-mover.Channels.ReturnMoveId:
		return id, nil
	case err := <-mover.Channels.Error:
		return uuid.UUID{}, err
	}
}

func (m *moverState) GetStatus(transferId uuid.UUID) (TransferStatus, error) {
	m.Channels.RequestStatus <- transferId
	select {
	case status := <-mover.Channels.ReturnStatus:
		return status, nil
	case err := <-mover.Channels.Error:
		return TransferStatus{}, err
	}
}

//----------------------------------------------------
// everything past here runs in the mover's goroutine
//----------------------------------------------------

// the goroutine itself
func (m *moverState) process() {
	running := true
	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	moves := make(map[uuid.UUID]moveRequest)
	for running {
		select {
		case move := <-mover.Channels.RequestMove:
			id, err := m.start(move)
			if err != nil {
				mover.Channels.Error <- err
			}
			moves[id] = move
			mover.Channels.ReturnMoveId <- id
		case id := <-mover.Channels.RequestStatus:
			move, found := moves[id]
			if !found {
				mover.Channels.Error <- errors.New(fmt.Sprintf("Invalid move ID: %s", id.String()))
				break
			}
			source := mover.Endpoints[move.Source]
			status, err := source.Status(id)
			if err != nil {
				mover.Channels.Error <- err
				break
			}
			mover.Channels.ReturnStatus <- status
		case <-mover.Channels.Stop:
			running = false
		}

		time.Sleep(pollInterval)
	}
}

func (m *moverState) start(move moveRequest) (uuid.UUID, error) {
	// obtain (and/or record) the properly-resolved source and destination endpoints
	if _, found := mover.Endpoints[move.Source]; !found {
		source, err := endpoints.NewEndpoint(move.Source)
		if err != nil {
			return uuid.UUID{}, err
		}
		mover.Endpoints[move.Source] = source
	}
	if _, found := mover.Endpoints[move.Destination]; !found {
		destination, err := destinationEndpoint(move.Destination)
		if err != nil {
			return uuid.UUID{}, err
		}
		mover.Endpoints[move.Destination] = destination
	}

	source := mover.Endpoints[move.Source]
	destination := mover.Endpoints[move.Destination]

	// assemble file transfers from the descriptors
	files := make([]endpoints.FileTransfer, len(move.Descriptors))
	for i, d := range move.Descriptors {
		path := d["path"].(string)
		destinationPath := destinationFolder(move.Destination, path)
		files[i] = FileTransfer{
			SourcePath:      path,
			DestinationPath: destinationPath,
			Hash:            d["hash"].(string),
		}
	}

	id, err := source.Transfer(destination, files)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}
