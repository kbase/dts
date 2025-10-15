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

	"github.com/kbase/dts/endpoints"
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

func (channels *dispatcherChannels) close() {
	close(channels.RequestTransfer)
	close(channels.ReturnTransferId)
	close(channels.CancelTransfer)
	close(channels.RequestStatus)
	close(channels.ReturnStatus)
	close(channels.Error)
	close(channels.Stop)
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
	err := <-d.Channels.Error
	d.Channels.close()
	return err
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

func (d *dispatcherState) CancelTransfer(id uuid.UUID) error {
	d.Channels.CancelTransfer <- id
	return <-d.Channels.Error
}

//---------------------------------------------------------
// everything past here runs in the dispatcher's goroutine
//---------------------------------------------------------

// the goroutine itself
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
			transferId, numFiles, err := d.create(spec)
			if err != nil {
				returnError <- err
				break
			}
			returnTransferId <- transferId
			slog.Info(fmt.Sprintf("Created new transfer %s (%d file(s) requested)", transferId.String(),
				numFiles))
		case transferId := <-cancellationRequested:
			slog.Info(fmt.Sprintf("Canceling transfer %s", transferId.String()))
			if err := d.cancel(transferId); err != nil {
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
			returnError <- nil
		}
	}
}

// creates a transfer from the given specification and starts things moving; returns a UUID for the
// transfer, the number of files in the payload, and/or an error
func (d *dispatcherState) create(spec Specification) (uuid.UUID, int, error) {
	transferId, numFiles, err := store.NewTransfer(spec)
	if err != nil {
		return uuid.UUID{}, 0, err
	}
	descriptors, err := store.GetDescriptors(transferId)
	if err != nil {
		return uuid.UUID{}, 0, err
	}

	// do we need to stage files for the source database?
	filesStaged := true
	descriptorsForEndpoint, err := descriptorsByEndpoint(spec, descriptors)
	for source, descriptorsForSource := range descriptorsForEndpoint {
		sourceEndpoint, err := endpoints.NewEndpoint(source)
		if err != nil {
			return uuid.UUID{}, 0, err
		}
		filesStaged, err = sourceEndpoint.FilesStaged(descriptorsForSource)
		if err != nil {
			return uuid.UUID{}, 0, err
		}
		if !filesStaged {
			break
		}
	}

	if !filesStaged {
		stager.StageFiles(transferId)
	} else {
		mover.MoveFiles(transferId)
	}

	return transferId, numFiles, err
}

func (d *dispatcherState) cancel(transferId uuid.UUID) error {
	status, err := store.GetStatus(transferId)
	if err != nil {
		return err
	}
	switch status.Code {
	case TransferStatusUnknown, TransferStatusSucceeded, TransferStatusFailed:
		return nil
	case TransferStatusStaging:
		return stager.Cancel(transferId)
	case TransferStatusActive, TransferStatusInactive:
		return mover.Cancel(transferId)
	case TransferStatusFinalizing:
		return manifestor.Cancel(transferId)
	}
	return nil
}
