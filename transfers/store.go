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
	"github.com/google/uuid"
)

//-------
// Store
//-------

// The transfer metadata store maintains a table of active and completed transfers and all related
// metadata. The store only tracks the state of the transfers--it doesn't initiate any activity.

// store global state
var store storeState

type storeState struct {
	Channels storeChannels
}

type storeChannels struct {
	RequestNewTransfer chan Specification
	ReturnNewTransfer  chan transferIdAndNumFiles

	SetStatus     chan transferIdAndStatus
	RequestStatus chan uuid.UUID
	ReturnStatus  chan TransferStatus

	RequestDescriptors chan uuid.UUID
	ReturnDescriptors  chan []map[string]any

	Error chan error
	Stop  chan struct{}
}

type transferIdAndNumFiles struct {
	Id       uuid.UUID
	NumFiles int
}

type transferIdAndStatus struct {
	Id     uuid.UUID
	Status TransferStatus
}

// starts the store goroutine
func (s *storeState) Start() error {
	s.Channels = storeChannels{
		RequestNewTransfer: make(chan Specification, 32),
		ReturnNewTransfer:  make(chan transferIdAndNumFiles, 32),
		SetStatus:          make(chan transferIdAndStatus, 32),
		RequestStatus:      make(chan uuid.UUID, 32),
		ReturnStatus:       make(chan TransferStatus, 32),
		Error:              make(chan error, 32),
		Stop:               make(chan struct{}),
	}
	go s.process()
	return nil
}

// stops the store goroutine
func (s *storeState) Stop() error {
	return nil
}

// creates a new entry for a transfer within the store, populating it with relevant metadata
func (s *storeState) NewTransfer(spec Specification) (uuid.UUID, int, error) {
	s.Channels.RequestNewTransfer <- spec
	select {
	case idAndNumFiles := <-store.Channels.ReturnNewTransfer:
		return idAndNumFiles.Id, idAndNumFiles.NumFiles, nil
	case err := <-store.Channels.Error:
		return uuid.UUID{}, 0, err
	}
}

func (s *storeState) SetStatus(transferId uuid.UUID, status TransferStatus) error {
	s.Channels.SetStatus <- transferIdAndStatus{
		Id:     transferId,
		Status: status,
	}
	return <-store.Channels.Error
}

func (s *storeState) GetStatus(transferId uuid.UUID) (TransferStatus, error) {
	s.Channels.RequestStatus <- transferId
	select {
	case status := <-store.Channels.ReturnStatus:
		return status, nil
	case err := <-store.Channels.Error:
		return TransferStatus{}, err
	}
}

func (s *storeState) GetDescriptors(transferId uuid.UUID) ([]map[string]any, error) {
	store.Channels.RequestDescriptors <- transferId
	select {
	case descriptors := <-store.Channels.ReturnDescriptors:
		return descriptors, nil
	case err := <-store.Channels.Error:
		return nil, err
	}
}

// goroutine for transfer data store
func (s *storeState) process() {
	running := true
	transfers := make(map[uuid.UUID]transferStoreEntry)
	for running {
		select {
		case spec := <-store.Channels.RequestNewTransfer:

		case idAndStatus := <-store.Channels.SetStatus:
			if transfer, found := transfers[idAndStatus.Id]; found {
				transfer.Status = idAndStatus.Status
				transfers[idAndStatus.Id] = transfer
			}
		case id := <-store.Channels.RequestStatus:
		case <-store.Channels.Stop:
			running = false
		}
	}
}

// an entry in the transfer metadata store
type transferStoreEntry struct {
	Destination string // name of destination database (in config) OR custom spec
	Source      string // name of source database (in config)
	Status      TransferStatus
	Tasks       []transferTaskEntry // single source -> destination transfer tasks
}

// Transfers consist of one or more "tasks", each of which transfers files from a single source to a
// single destination portion of a transfer
type transferTaskEntry struct {
	Source      string // name of source endpoint (in config)
	Destination string // name of destination endpoint (in config) OR custom spec
}
