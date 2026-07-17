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

type transferCancellationRequest struct {
	Id    uuid.UUID
	Orcid string
}

type dispatcherChannels struct {
	RequestTransfer  chan Specification         // used by client to create a new transfer
	ReturnTransferId chan resultType[uuid.UUID] // returns transfer ID to client

	CancelTransfer chan transferCancellationRequest // used by client to cancel a transfer
	CancelResult   chan error

	RequestStatus chan uuid.UUID                  // used by client to request transfer status
	ReturnStatus  chan resultType[TransferStatus] // returns transfer status to client

	Error chan error    // internal -> client error propagation
	Stop  chan struct{} // used by client to stop transfer management
}

func newDispatcherChannels(maxConnections int) dispatcherChannels {
	return dispatcherChannels{
		RequestTransfer:  make(chan Specification, maxConnections),
		ReturnTransferId: make(chan resultType[uuid.UUID]),
		CancelTransfer:   make(chan transferCancellationRequest, maxConnections),
		CancelResult:     make(chan error),
		RequestStatus:    make(chan uuid.UUID, maxConnections),
		ReturnStatus:     make(chan resultType[TransferStatus]),
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
	d.Channels = newDispatcherChannels(config.Service.MaxConnections)
	go d.process()
	return <-d.Channels.Error
}

func (d *dispatcherState) Stop() error {
	d.Channels.Stop <- struct{}{}
	err := <-d.Channels.Error
	d.Channels.Close()
	return err
}

func (d *dispatcherState) CreateTransfer(spec Specification) (uuid.UUID, error) {
	d.Channels.RequestTransfer <- spec
	id := <-d.Channels.ReturnTransferId
	if id.Error != nil {
		return uuid.UUID{}, id.Error
	}
	return id.Value, nil
}

func (d *dispatcherState) GetTransferStatus(transferId uuid.UUID) (TransferStatus, error) {
	d.Channels.RequestStatus <- transferId
	status := <-d.Channels.ReturnStatus
	return status.Value, status.Error
}

func (d *dispatcherState) CancelTransfer(transferId uuid.UUID, orcid string) error {
	d.Channels.CancelTransfer <- transferCancellationRequest{Id: transferId, Orcid: orcid}
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
			var transferId resultType[uuid.UUID]
			transferId.Value, transferId.Error = d.create(spec)
			d.Channels.ReturnTransferId <- transferId

			err := d.initialize(transferId.Value)
			if err != nil {
				slog.Error(fmt.Sprintf("Transfer %s failed: %s", transferId.Value.String(), err.Error()))
				status := TransferStatus{
					Code:    TransferStatusFailed,
					Message: err.Error(),
				}
				store.SetStatus(transferId.Value, status)
				publish(Message{
					Description:    fmt.Sprintf("Transfer %s failed: %s", transferId.Value.String(), err.Error()),
					TransferId:     transferId.Value,
					TransferStatus: status,
					Time:           time.Now(),
				})
			}
		case request := <-d.Channels.CancelTransfer:
			err := d.cancel(request.Id, request.Orcid)
			if err == nil {
				status, err := store.GetStatus(request.Id)
				if err == nil {
					publish(Message{
						Description:    fmt.Sprintf("Canceling transfer %s", request.Id),
						TransferId:     request.Id,
						TransferStatus: status,
						Time:           time.Now(),
					})
				}
			}
			d.Channels.CancelResult <- err
		case transferId := <-d.Channels.RequestStatus:
			var status resultType[TransferStatus]
			status.Value, status.Error = store.GetStatus(transferId)
			d.Channels.ReturnStatus <- status
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

	slog.Debug(fmt.Sprintf("found previous transfers in %s", saveFilename))
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
	err := validateSpecification(spec)
	if err != nil {
		return uuid.UUID{}, err
	}

	return store.NewTransfer(spec), nil
}

func (d *dispatcherState) initialize(transferId uuid.UUID) error {
	descriptors, err := store.GetDescriptors(transferId)
	if err != nil {
		return err
	}
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return err
	}

	// do we need to stage files for the source database?
	filesStaged := true
	descriptorsForEndpoint, err := descriptorsByEndpoint(spec, descriptors)
	if err != nil {
		return err
	}
	for source, descriptorsForSource := range descriptorsForEndpoint {
		sourceEndpoint, err := endpoints.NewEndpoint(source)
		if err != nil {
			return err
		}
		filesStaged, err = sourceEndpoint.FilesStaged(descriptorsForSource)
		if err != nil {
			return err
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

	return err
}

func validateSpecification(spec Specification) error {
	destDb, err := databases.NewDatabase(spec.Destination)
	if err != nil {
		_, err2 := endpoints.ParseCustomSpec(spec.Destination)
		if err2 != nil {
			return fmt.Errorf("invalid destination: %s", spec.Destination)
		}
		return nil
	}

	_, err = destDb.LocalUser(spec.User.Orcid)
	if err != nil {
		return &InvalidOrcidError{
			Orcid:   spec.User.Orcid,
			Message: err.Error(),
		}
	}
	return nil
}

func (d *dispatcherState) cancel(transferId uuid.UUID, orcid string) error {
	status, err := store.GetStatus(transferId)
	if err != nil {
		return err
	}

	// check that the requestor initiated the transfer
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return err
	}
	if spec.User.Orcid != orcid {
		return fmt.Errorf("cannot cancel transfer %s: invalid Orcid", transferId.String())
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
