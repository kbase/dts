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
	"log/slog"
	"path/filepath"

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
	RequestMove         chan uuid.UUID
	RequestCancellation chan uuid.UUID
	Error               chan error
	Stop                chan struct{}
}

func (channels *moverChannels) close() {
	close(channels.RequestMove)
	close(channels.RequestCancellation)
	close(channels.Error)
	close(channels.Stop)
}

// starts the mover
func (m *moverState) Start() error {
	slog.Debug("mover.Start")
	m.Channels = moverChannels{
		RequestMove:         make(chan uuid.UUID, 32),
		RequestCancellation: make(chan uuid.UUID, 32),
		Error:               make(chan error, 32),
		Stop:                make(chan struct{}),
	}
	m.Endpoints = make(map[string]endpoints.Endpoint)
	go m.process()
	return <-m.Channels.Error
}

// stops the mover goroutine
func (m *moverState) Stop() error {
	slog.Debug("mover.Stop")
	m.Channels.Stop <- struct{}{}
	err := <-m.Channels.Error
	m.Channels.close()
	return err
}

// starts moving files associated with the given transfer ID
func (m *moverState) MoveFiles(transferId uuid.UUID) error {
	slog.Debug("mover.MoveFiles")
	m.Channels.RequestMove <- transferId
	return <-m.Channels.Error
}

// cancels a file move operation
func (m *moverState) Cancel(transferId uuid.UUID) error {
	slog.Debug("mover.Cancel")
	m.Channels.RequestCancellation <- transferId
	return <-m.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the mover's goroutine
//----------------------------------------------------

// the goroutine itself
func (m *moverState) process() {
	running := true
	moveOperations := make(map[uuid.UUID][]moveOperation) // a single transfer can be several move operations!
	pulse := clock.Subscribe()
	m.Channels.Error <- nil

	for running {
		select {
		case transferId := <-m.Channels.RequestMove:
			moves, err := m.moveFiles(transferId)
			if err == nil {
				moveOperations[transferId] = moves
			}
			m.Channels.Error <- err
		case transferId := <-m.Channels.RequestCancellation:
			if moves, found := moveOperations[transferId]; found {
				err := m.cancel(moves)
				if err == nil {
					delete(moveOperations, transferId)
				}
				m.Channels.Error <- err
			} else {
				m.Channels.Error <- TransferNotFoundError{Id: transferId}
			}
		case <-pulse:
			// check the move statuses and advance as needed
			for transferId, moves := range moveOperations {
				if status, err := m.updateStatus(transferId, moves); err != nil {
					slog.Error(err.Error())
				} else if status.Code >= TransferStatusFinalizing { // finalizing or failed
					if status.Code == TransferStatusFinalizing {
						err = manifestor.Generate(transferId)
						if err != nil {
							slog.Error(err.Error())
						}
					}
					delete(moveOperations, transferId)
				}
			}
		case <-m.Channels.Stop:
			running = false
			m.Channels.Error <- nil
		}
	}
	clock.Unsubscribe()
}

type moveOperation struct {
	Id                                  uuid.UUID // move ID (distinct from transfer ID)
	SourceEndpoint, DestinationEndpoint string
	Completed                           bool
}

// starts moving files for the transfer with the given ID, returning one or more move operations,
// depending on the number of relevant source endpoints
func (m *moverState) moveFiles(transferId uuid.UUID) ([]moveOperation, error) {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return nil, err
	}
	descriptors, err := store.GetDescriptors(transferId)
	if err != nil {
		return nil, err
	}

	// start transfers for each endpoint
	descriptorsForEndpoint, err := descriptorsByEndpoint(spec, descriptors)
	moves := make([]moveOperation, 0)
	for source, descriptorsForSource := range descriptorsForEndpoint {
		files := make([]endpoints.FileTransfer, len(descriptorsForSource))
		for i, descriptor := range descriptorsForSource {
			path := descriptor["path"].(string)
			destinationPath := filepath.Join(destinationFolder(spec.Destination), path)
			files[i] = endpoints.FileTransfer{
				SourcePath:      path,
				DestinationPath: destinationPath,
				Hash:            descriptor["hash"].(string),
			}
		}
		sourceEndpoint, err := endpoints.NewEndpoint(source)
		if err != nil {
			return nil, err
		}
		destination := config.Databases[spec.Destination].Endpoint
		destinationEndpoint, err := endpoints.NewEndpoint(destination)
		if err != nil {
			return nil, err
		}
		moveId, err := sourceEndpoint.Transfer(destinationEndpoint, files)
		if err != nil {
			return nil, err
		}

		// update the transfer status
		if status, err := store.GetStatus(transferId); err == nil {
			status.Code = TransferStatusActive
			if err := store.SetStatus(transferId, status); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}

		moves = append(moves, moveOperation{
			Id:                  moveId,
			SourceEndpoint:      source,
			DestinationEndpoint: spec.Destination,
		})
	}
	return moves, nil
}

// update the status of the transfer with the given ID given its distinct file move operations,
// returning true if the transfer has completed (successfully or unsuccessfully), false otherwise
func (m *moverState) updateStatus(transferId uuid.UUID, moves []moveOperation) (TransferStatus, error) {
	oldStatus, err := store.GetStatus(transferId)
	if err != nil {
		return oldStatus, err
	}
	newStatus := oldStatus

	atLeastOneMoveFailed := false
	movesAllSucceeded := true
	for i, move := range moves {
		source, err := endpoints.NewEndpoint(move.SourceEndpoint)
		if err != nil {
			return oldStatus, err
		}
		moveStatus, err := source.Status(move.Id)
		if err != nil {
			return oldStatus, err
		}
		newStatus.NumFiles += moveStatus.NumFiles
		newStatus.NumFilesTransferred += moveStatus.NumFilesTransferred
		newStatus.NumFilesSkipped += moveStatus.NumFilesSkipped

		if moveStatus.Code == TransferStatusSucceeded {
			moves[i].Completed = true
		} else {
			movesAllSucceeded = false
			if moveStatus.Code == TransferStatusFailed {
				newStatus.Message = moveStatus.Message
				atLeastOneMoveFailed = true
				moves[i].Completed = true
			}
		}
	}

	// take stock and update status as needed
	if movesAllSucceeded {
		newStatus.Code = TransferStatusFinalizing
	} else if atLeastOneMoveFailed {
		newStatus.Code = TransferStatusFailed
	}
	if newStatus != oldStatus {
		err = store.SetStatus(transferId, newStatus)
	}

	return newStatus, err
}

func (m *moverState) cancel(moves []moveOperation) error {
	var e error
	for _, move := range moves {
		endpoint, err := endpoints.NewEndpoint(move.SourceEndpoint)
		if err != nil {
			return err
		}
		err = endpoint.Cancel(move.Id)
		if err != nil && e == nil {
			e = err
		}
	}
	return e
}
