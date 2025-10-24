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
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/databases"
)

//--------
// Stager
//--------

// The stager coordinates the staging of files at a source database in preparation for transfer.

// stager global state
var stager stagerState

type stagerState struct {
	Channels  stagerChannels
	Databases map[string]databases.Database
}

type stagerChannels struct {
	RequestStaging      chan uuid.UUID
	RequestCancellation chan uuid.UUID
	Error               chan error
	Stop                chan struct{}
}

func (channels *stagerChannels) close() {
	close(channels.RequestStaging)
	close(channels.RequestCancellation)
	close(channels.Error)
	close(channels.Stop)
}

// starts the stager
func (s *stagerState) Start() error {
	slog.Debug("stager.Start")
	s.Channels = stagerChannels{
		RequestStaging:      make(chan uuid.UUID, 31),
		RequestCancellation: make(chan uuid.UUID, 31),
		Error:               make(chan error, 31),
		Stop:                make(chan struct{}),
	}
	go s.process()
	return <-s.Channels.Error
}

// stops the stager goroutine
func (s *stagerState) Stop() error {
	slog.Debug("stager.Stop")
	s.Channels.Stop <- struct{}{}
	err := <-s.Channels.Error
	s.Channels.close()
	return err
}

// requests that files be staged for the transfer with the given ID
func (s *stagerState) StageFiles(id uuid.UUID) error {
	slog.Debug("stager.StageFiles")
	s.Channels.RequestStaging <- id
	return <-s.Channels.Error
}

// cancels a file staging operation
func (s *stagerState) Cancel(transferId uuid.UUID) error {
	slog.Debug("stager.Cancel")
	s.Channels.RequestCancellation <- transferId
	return <-s.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the stager's goroutine
//----------------------------------------------------

// the goroutine itself
func (s *stagerState) process() {
	running := true
	stagings := make(map[uuid.UUID]stagingEntry)
	pulse := clock.Subscribe()
	s.Channels.Error <- nil

	for running {
		select {
		case transferId := <-s.Channels.RequestStaging:
			entry, err := s.stageFiles(transferId)
			if err == nil {
				stagings[transferId] = entry
			}
			s.Channels.Error <- nil
		case transferId := <-s.Channels.RequestCancellation:
			if _, found := stagings[transferId]; found {
				delete(stagings, transferId) // simply remove the entry and stop tracking file staging
				s.Channels.Error <- nil
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: transferId}
			}
		case <-pulse:
			// check the staging status and advance to a transfer if it's finished
			for transferId, staging := range stagings {
				if err := s.updateStatus(transferId, staging); err != nil {
					slog.Error(err.Error())
				}
			}
		case <-s.Channels.Stop:
			running = false
			s.Channels.Error <- nil
		}
	}
	clock.Unsubscribe()
}

type stagingEntry struct {
	Id uuid.UUID // staging ID (distinct from transfer ID)
}

func (s *stagerState) stageFiles(transferId uuid.UUID) (stagingEntry, error) {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return stagingEntry{}, err
	}
	db, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return stagingEntry{}, err
	}
	id, err := db.StageFiles(spec.User.Orcid, spec.FileIds)
	if err != nil {
		return stagingEntry{}, err
	}
	return stagingEntry{Id: id}, nil
}

func (s *stagerState) updateStatus(transferId uuid.UUID, staging stagingEntry) error {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return err
	}
	source, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return err
	}

	stagingStatus, err := source.StagingStatus(staging.Id)
	if err != nil {
		return err
	}

	oldStatus, err := store.GetStatus(transferId)
	if err == nil {
		return err
	}
	newStatus := oldStatus
	switch stagingStatus {
	case databases.StagingStatusSucceeded:
		newStatus.Message = fmt.Sprintf("file staging succeeded for transfer %s", transferId.String())
		newStatus.Code = TransferStatusActive
	case databases.StagingStatusFailed:
		newStatus.Code = TransferStatusFailed
		newStatus.Message = fmt.Sprintf("file staging failed for transfer %s", transferId.String())
	default: // still staging
		newStatus.Code = TransferStatusStaging
	}

	if newStatus.Code != oldStatus.Code {
		if err := store.SetStatus(transferId, newStatus); err != nil {
			return err
		}
		publish(Message{
			Description:    newStatus.Message,
			TransferId:     transferId,
			TransferStatus: newStatus,
			Time:           time.Now(),
		})
	}

	if newStatus.Code == TransferStatusActive {
		if err := mover.MoveFiles(transferId); err != nil {
			return err
		}
	}
	return nil
}
