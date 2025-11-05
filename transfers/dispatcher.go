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
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//------------
// Dispatcher
//------------

// The dispatcher handles transfer-related requests from clients. When the dispatcher is started,
// it starts the store, stager, mover, and manifestor, loading any previous transfers from disk.
//
// When a client requests that the dispatcher create a transfer, the dispatcher asks the store to
// create a new transfer record, and then dispatches a request to the stager or the mover, based on
// whether the files to be transferred require staging.
//
// A client can request the status of ongoing and completed transfers, which the dispatcher fetches
// from the store. A client can also request that an ongoing transfer be deleted, which the
// dispatcher propagates to the store, stager, mover, and manifestor.
//
// When the dispatcher is stopped, it stops the store, stager, mover, and manifestor.

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

func newDispatcherChannels(maxConnections int) dispatcherChannels {
	return dispatcherChannels{
		RequestTransfer:  make(chan Specification, maxConnections),
		ReturnTransferId: make(chan uuid.UUID),
		CancelTransfer:   make(chan uuid.UUID, maxConnections),
		RequestStatus:    make(chan uuid.UUID, maxConnections),
		ReturnStatus:     make(chan TransferStatus),
		Error:            make(chan error),
		Stop:             make(chan struct{}),
	}
}

func (channels *dispatcherChannels) Close() {
	close(channels.RequestTransfer)
	close(channels.ReturnTransferId)
	close(channels.CancelTransfer)
	close(channels.RequestStatus)
	close(channels.ReturnStatus)
	close(channels.Error)
	close(channels.Stop)
}

func (d *dispatcherState) Start() error {
	slog.Debug("dispatcher.Start()")
	d.Channels = newDispatcherChannels(config.Service.MaxConnections)
	go d.process()
	return <-d.Channels.Error
}

func (d *dispatcherState) Stop() error {
	slog.Debug("dispatcher.Stop")
	d.Channels.Stop <- struct{}{}
	err := <-d.Channels.Error
	d.Channels.Close()
	return err
}

func (d *dispatcherState) CreateTransfer(spec Specification) (uuid.UUID, error) {
	slog.Debug("dispatcher.CreateTransfer")
	d.Channels.RequestTransfer <- spec
	select {
	case id := <-d.Channels.ReturnTransferId:
		return id, nil
	case err := <-d.Channels.Error:
		return uuid.UUID{}, err
	}
}

func (d *dispatcherState) GetTransferStatus(transferId uuid.UUID) (TransferStatus, error) {
	slog.Debug("dispatcher.GetTransferStatus")
	d.Channels.RequestStatus <- transferId
	select {
	case status := <-d.Channels.ReturnStatus:
		return status, nil
	case err := <-d.Channels.Error:
		return TransferStatus{}, err
	}
}

func (d *dispatcherState) CancelTransfer(transferId uuid.UUID) error {
	slog.Debug("dispatcher.CancelTransfer")
	d.Channels.CancelTransfer <- transferId
	err := <-d.Channels.Error
	if err != nil {
		slog.Error(fmt.Sprintf("Transfer %s: %s", transferId.String(), err.Error()))
	}
	return err
}

//---------------------------------------------------------
// everything past here runs in the dispatcher's goroutine
//---------------------------------------------------------

// the goroutine itself
func (d *dispatcherState) process() {
	running := true
	d.Channels.Error <- d.start()

	for running {
		select {
		case spec := <-d.Channels.RequestTransfer:
			transferId, err := d.create(spec)
			if err != nil {
				d.Channels.Error <- err
			} else {
				d.Channels.ReturnTransferId <- transferId
			}
		case transferId := <-d.Channels.CancelTransfer:
			err := d.cancel(transferId)
			if err == nil {
				status, err := store.GetStatus(transferId)
				if err == nil {
					publish(Message{
						Description:    fmt.Sprintf("Canceling transfer %s", transferId),
						TransferId:     transferId,
						TransferStatus: status,
						Time:           time.Now(),
					})
				}
			}
			d.Channels.Error <- err
		case transferId := <-d.Channels.RequestStatus:
			status, err := store.GetStatus(transferId)
			if err != nil {
				d.Channels.Error <- err
			} else {
				d.Channels.ReturnStatus <- status
			}
		case <-d.Channels.Stop:
			err := d.stop()
			d.Channels.Error <- err
			running = false
		}
	}
}

func (d *dispatcherState) start() error {
	saveFilename := filepath.Join(config.Service.DataDirectory, "dts.gob")
	saveFile, err := os.Open(saveFilename)
	if err != nil { // no save file -- fresh start
		slog.Debug("no previous transfers found")
		if err := store.Start(); err != nil {
			return err
		}
		if err := stager.Start(); err != nil {
			return err
		}
		if err := mover.Start(); err != nil {
			return err
		}
		return manifestor.Start()
	}

	slog.Debug(fmt.Sprintf("found previous tasks in %s", saveFilename))
	defer saveFile.Close()
	decoder := gob.NewDecoder(saveFile)
	var databaseStates databases.DatabaseSaveStates
	if err := decoder.Decode(&databaseStates); err == nil {
		if err = databases.Load(databaseStates); err != nil {
			slog.Error(fmt.Sprintf("Restoring database states: %s", err.Error()))
		}
		if err := store.Load(decoder); err != nil {
			return err
		}
		if err := stager.Load(decoder); err != nil {
			return err
		}
		if err := mover.Load(decoder); err != nil {
			return err
		}
		if err := manifestor.Load(decoder); err != nil {
			return err
		}
	} else {
		return &SaveFileError{
			Filename: saveFilename,
			Message:  fmt.Sprintf("Reading save file: %s", err.Error()),
		}
	}
	slog.Debug(fmt.Sprintf("Restored transfers from %s", saveFilename))
	return nil
}

// creates a transfer from the given specification and starts things moving; returns a UUID for the
// transfer, the number of files in the payload, and/or an error
func (d *dispatcherState) create(spec Specification) (uuid.UUID, error) {
	transferId, err := store.NewTransfer(spec)
	if err != nil {
		return uuid.UUID{}, err
	}
	descriptors, err := store.GetDescriptors(transferId)
	if err != nil {
		return uuid.UUID{}, err
	}

	// do we need to stage files for the source database?
	filesStaged := true
	descriptorsForEndpoint, err := descriptorsByEndpoint(spec, descriptors)
	if err != nil {
		return uuid.UUID{}, err
	}
	for source, descriptorsForSource := range descriptorsForEndpoint {
		sourceEndpoint, err := endpoints.NewEndpoint(source)
		if err != nil {
			return uuid.UUID{}, err
		}
		filesStaged, err = sourceEndpoint.FilesStaged(descriptorsForSource)
		if err != nil {
			return uuid.UUID{}, err
		}
		if !filesStaged {
			break
		}
	}

	if !filesStaged {
		err = stager.StageFiles(transferId)
	} else {
		err = mover.MoveFiles(transferId)
	}

	return transferId, err
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

func (d *dispatcherState) stop() error {
	// save states into a file using a gob encoder
	saveFilename := filepath.Join(config.Service.DataDirectory, "dts.gob")
	saveFile, err := os.OpenFile(saveFilename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return &SaveFileError{
			Filename: saveFilename,
			Message:  fmt.Sprintf("Opening save file: %s", err.Error()),
		}
	}

	encoder := gob.NewEncoder(saveFile)
	if databaseStates, err := databases.Save(); err == nil {
		if err := encoder.Encode(databaseStates); err != nil {
			os.Remove(saveFilename)
			return err
		}
		if err := store.SaveAndStop(encoder); err != nil {
			os.Remove(saveFilename)
			return err
		}
		if err := stager.SaveAndStop(encoder); err != nil {
			os.Remove(saveFilename)
			return err
		}
		if err := mover.SaveAndStop(encoder); err != nil {
			os.Remove(saveFilename)
			return err
		}
		if err := manifestor.SaveAndStop(encoder); err != nil {
			os.Remove(saveFilename)
			return err
		}
		slog.Debug(fmt.Sprintf("saving transfer data to %s", saveFilename))
	} else {
		return &SaveFileError{
			Filename: saveFilename,
			Message:  fmt.Sprintf("Writing save file: %s", err.Error()),
		}
	}

	return err
}
