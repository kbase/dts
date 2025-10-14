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
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
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

// starts the stager
func (s *stagerState) Start() error {
	s.Channels = stagerChannels{
		RequestStaging: make(chan uuid.UUID, 32),
		Error:          make(chan error, 32),
		Stop:           make(chan struct{}),
	}
	go s.process()
	return nil
}

// stops the stager goroutine
func (s *stagerState) Stop() error {
	s.Channels.Stop <- struct{}{}
	return <-s.Channels.Error
}

// requests that files be staged for the transfer with the given ID
func (s *stagerState) StageFiles(id uuid.UUID) error {
	s.Channels.RequestStaging <- id
	return <-stager.Channels.Error
}

// cancels a file staging operation
func (s *stagerState) Cancel(transferId uuid.UUID) error {
	s.Channels.RequestCancellation <- transferId
	return <-stager.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the stager's goroutine
//----------------------------------------------------

// the goroutine itself
func (s *stagerState) process() {
	running := true
	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	stagings := make(map[uuid.UUID]stagingEntry)
	for running {
		select {
		case transferId := <-stager.Channels.RequestStaging:
			entry, err := s.start(transferId)
			if err != nil {
				stager.Channels.Error <- err
			}
			stagings[transferId] = entry
		case transferId := <-mover.Channels.RequestCancellation:
			if _, found := stagings[transferId]; found {
				delete(stagings, transferId) // simply remove the entry and stop tracking file staging
				stager.Channels.Error <- nil
			} else {
				stager.Channels.Error <- NotFoundError{Id: transferId}
			}
		case <-stager.Channels.Stop:
			running = false
		}

		time.Sleep(pollInterval)

		// check the staging status and advance to a transfer if it's finished
		for transferId, staging := range stagings {
			if err := s.updateStatus(transferId, staging); err != nil {
				stager.Channels.Error <- err
			}
		}
	}
}

type stagingEntry struct {
	Id uuid.UUID // staging ID (distinct from transfer ID)
}

func (s *stagerState) start(transferId uuid.UUID) (stagingEntry, error) {
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

	status, err := source.StagingStatus(staging.Id)
	if err != nil {
		return err
	}

	if status == databases.StagingStatusSucceeded {
		err := mover.MoveFiles(transferId)
		if err != nil {
			return err
		}
	} else if status == databases.StagingStatusFailed {
		// FIXME: handle staging failures here!
	} else { // still staging
		xferStatus, err := store.GetStatus(transferId)
		if err != nil {
			return err
		}
		if xferStatus.Code != TransferStatusStaging {
			xferStatus.Code = TransferStatusStaging
			store.SetStatus(transferId, xferStatus)
		}
	}
	return nil
}
