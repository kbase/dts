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
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//--------
// Stager
//--------

// The stager coordinates the staging of files at a source endpoint in preparation for transfer.

// stager global state
var stager stagerState

type stagerState struct {
	Channels  stagerChannels
	Endpoints map[string]endpoints.Endpoint
}

type stagerChannels struct {
	RequestStaging  chan stagingRequest
	ReturnStagingId chan uuid.UUID

	RequestStatus chan uuid.UUID
	ReturnStatus  chan TransferStatus

	Error chan error
	Stop  chan struct{}
}

type stagingRequest struct {
	Descriptors []map[string]any
	Endpoint    string
}

// starts the stager
func (s *stagerState) Start() error {
	s.Channels = stagerChannels{
		RequestStaging:  make(chan stagingRequest, 32),
		ReturnStagingId: make(chan uuid.UUID, 32),
		RequestStatus:   make(chan uuid.UUID, 32),
		ReturnStatus:    make(chan TransferStatus, 32),
		Error:           make(chan error, 32),
		Stop:            make(chan struct{}),
	}
	s.Endpoints = make(map[string]endpoints.Endpoint)
	go s.process()
	return nil
}

// stops the store goroutine
func (s *stagerState) Stop() error {
	s.Channels.Stop <- struct{}{}
	return <-s.Channels.Error
}

func (s *stagerState) StageFiles(descriptors []map[string]any, endpoint string) (uuid.UUID, error) {
	s.Channels.RequestStaging <- stagingRequest{
		Descriptors: descriptors,
		Endpoint:    endpoint,
	}
	select {
	case id := <-stager.Channels.ReturnStagingId:
		return id, nil
	case err := <-stager.Channels.Error:
		return uuid.UUID{}, err
	}
}

func (s *stagerState) GetStatus(transferId uuid.UUID) (TransferStatus, error) {
	s.Channels.RequestStatus <- transferId
	select {
	case status := <-stager.Channels.ReturnStatus:
		return status, nil
	case err := <-stager.Channels.Error:
		return TransferStatus{}, err
	}
}

//----------------------------------------------------
// everything past here runs in the stager's goroutine
//----------------------------------------------------

// the goroutine itself
func (s *stagerState) process() {
	running := true
	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	stagings := make(map[uuid.UUID]stagingRequest)
	for running {
		select {
		case staging := <-stager.Channels.RequestStaging:
			id, err := s.start(staging)
			if err != nil {
				stager.Channels.Error <- err
			}
			stagings[id] = staging
			stager.Channels.ReturnStagingId <- id
		case id := <-stager.Channels.RequestStatus:
			staging, found := stagings[id]
			if !found {
				stager.Channels.Error <- errors.New(fmt.Sprintf("Invalid staging ID: %s", id.String()))
				break
			}
			source := stager.Endpoints[staging.Endpoint]
			status, err := source.Status(id)
			if err != nil {
				stager.Channels.Error <- err
				break
			}
			stager.Channels.ReturnStatus <- status
		case <-stager.Channels.Stop:
			running = false
		}

		time.Sleep(pollInterval)
	}
}

func (s *stagerState) start(staging stagingRequest) (uuid.UUID, error) {
	// assemble file IDs from the descriptors
	fileIds := make([]string, len(staging.Descriptors))
	for i, d := range staging.Descriptors {
		fileIds[i] = d["id"].(string)
	}

	db, err := databases.NewDatabase(staging.Endpoint)
	id, err := db.StageFiles(staging.Endpoint, fileIds)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}
