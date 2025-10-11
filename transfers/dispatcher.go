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
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

//------------
// Dispatcher
//------------

// dispatcher global state
var dispatcher dispatcherState

type dispatcherState struct {
	Channels dispatcherChannels
}

type dispatcherChannels struct {
	RequestTransfer  chan Specification // used by client to create a new transfer
	ReturnTransferId chan uuid.UUID     // returns task ID to client

	CancelTransfer chan uuid.UUID // used by client to cancel a transfer

	RequestStatus chan uuid.UUID      // used by client to request transfer status
	ReturnStatus  chan TransferStatus // returns task status to client

	Error chan error    // internal -> client error propagation
	Stop  chan struct{} // used by client to stop task management
}

func (d *dispatcherState) Start() error {
	d.Channels = dispatcherChannels{
		RequestTransfer:  make(chan Specification, 32),
		ReturnTransferId: make(chan uuid.UUID, 32),
		CancelTransfer:   make(chan uuid.UUID, 32),
		RequestStatus:    make(chan uuid.UUID, 32),
		ReturnStatus:     make(chan TransferStatus, 32),
		Error:            make(chan error, 32),
		Stop:             make(chan struct{}),
	}
	go d.process()

	return nil
}

func (d *dispatcherState) Stop() error {
	d.Channels.Stop <- struct{}{}
	return <-d.Channels.Error
}

func (d *dispatcherState) CreateTransfer(spec Specification) (uuid.UUID, error) {
	d.Channels.RequestTransfer <- spec
	select {
	case id := <-d.Channels.ReturnTransferId:
		return id, nil
	case err := <-d.Channels.Error:
		return uuid.UUID{}, err
	}
}

func (d *dispatcherState) GetTransferStatus(id uuid.UUID) (TransferStatus, error) {
	d.Channels.RequestStatus <- id
	select {
	case status := <-d.Channels.ReturnStatus:
		return status, nil
	case err := <-d.Channels.Error:
		return TransferStatus{}, err
	}
}

// This goroutine handles all client interactions, sending data along channels to internal
// goroutines as needed.
func (d *dispatcherState) process() {

	// client input channels
	var newTransferRequested <-chan Specification = dispatcher.Channels.RequestTransfer
	var cancellationRequested <-chan uuid.UUID = dispatcher.Channels.CancelTransfer
	var statusRequested <-chan uuid.UUID = dispatcher.Channels.RequestStatus
	var stopRequested <-chan struct{} = dispatcher.Channels.Stop

	// client output channels
	var returnTransferId chan<- uuid.UUID = dispatcher.Channels.ReturnTransferId
	var returnStatus chan<- TransferStatus = dispatcher.Channels.ReturnStatus
	var returnError chan<- error = dispatcher.Channels.Error

	// respond to client requests
	running := true
	for running {
		select {
		case spec := <-newTransferRequested:
			transferId, numFiles, err := store.NewTransfer(spec)
			if err != nil {
				returnError <- err
				break
			}
			returnTransferId <- transferId
			slog.Info(fmt.Sprintf("Created new transfer task %s (%d file(s) requested)",
				transferId.String(), numFiles))

		case transferId := <-cancellationRequested:
			if err := cancelTransfer(transferId); err != nil {
				slog.Error(fmt.Sprintf("Transfer %s: %s", transferId.String(), err.Error()))
				returnError <- err
			}
		case transferId := <-statusRequested:
			status, err := store.GetStatus(transferId)
			if err != nil {
				returnError <- err
				break
			}
			returnStatus <- status
		case <-stopRequested:
			running = false
		}
	}
}
