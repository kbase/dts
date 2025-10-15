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

	"github.com/kbase/dts/databases"
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
	ReturnNewTransfer  chan uuid.UUID

	RequestSpec chan uuid.UUID
	ReturnSpec  chan Specification

	RequestDescriptors chan uuid.UUID
	ReturnDescriptors  chan []map[string]any

	SetStatus     chan transferIdAndStatus
	RequestStatus chan uuid.UUID
	ReturnStatus  chan TransferStatus

	RequestRemoval chan uuid.UUID

	Error chan error
	Stop  chan struct{}
}

func (channels *storeChannels) close() {
	close(channels.RequestNewTransfer)
	close(channels.ReturnNewTransfer)
	close(channels.RequestSpec)
	close(channels.ReturnSpec)
	close(channels.RequestDescriptors)
	close(channels.ReturnDescriptors)
	close(channels.SetStatus)
	close(channels.RequestStatus)
	close(channels.ReturnStatus)
	close(channels.RequestRemoval)
	close(channels.Error)
	close(channels.Stop)
}

type transferIdAndStatus struct {
	Id     uuid.UUID
	Status TransferStatus
}

// starts the store goroutine
func (s *storeState) Start() error {
	s.Channels = storeChannels{
		RequestNewTransfer: make(chan Specification, 32),
		ReturnNewTransfer:  make(chan uuid.UUID, 32),
		RequestSpec:        make(chan uuid.UUID, 32),
		ReturnSpec:         make(chan Specification, 32),
		RequestDescriptors: make(chan uuid.UUID, 32),
		ReturnDescriptors:  make(chan []map[string]any, 32),
		SetStatus:          make(chan transferIdAndStatus, 32),
		RequestStatus:      make(chan uuid.UUID, 32),
		ReturnStatus:       make(chan TransferStatus, 32),
		RequestRemoval:     make(chan uuid.UUID, 32),
		Error:              make(chan error, 32),
		Stop:               make(chan struct{}),
	}
	go s.process()
	return nil
}

// stops the store goroutine
func (s *storeState) Stop() error {
	s.Channels.Stop <- struct{}{}
	err := <-s.Channels.Error
	s.Channels.close()
	return err
}

// creates a new entry for a transfer within the store, populating it with relevant metadata and
// returning a UUID, number of files, and/or error condition for the request
func (s *storeState) NewTransfer(spec Specification) (uuid.UUID, int, error) {
	s.Channels.RequestNewTransfer <- spec
	select {
	case id := <-store.Channels.ReturnNewTransfer:
		return id, len(spec.FileIds), nil
	case err := <-store.Channels.Error:
		return uuid.UUID{}, 0, err
	}
}

func (s *storeState) GetSpecification(transferId uuid.UUID) (Specification, error) {
	store.Channels.RequestSpec <- transferId
	select {
	case spec := <-store.Channels.ReturnSpec:
		return spec, nil
	case err := <-store.Channels.Error:
		return Specification{}, err
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

func (s *storeState) Remove(transferId uuid.UUID) error {
	s.Channels.RequestRemoval <- transferId
	return <-store.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the store's goroutine
//----------------------------------------------------

// the goroutine itself
func (s *storeState) process() {
	running := true
	transfers := make(map[uuid.UUID]transferStoreEntry)
	for running {
		select {
		case spec := <-store.Channels.RequestNewTransfer:
			id := uuid.New()
			source, err := databases.NewDatabase(spec.Source)
			if err != nil {
				store.Channels.Error <- err
				break
			}
			descriptors, err := source.Descriptors(spec.User.Orcid, spec.FileIds)
			if err != nil {
				store.Channels.Error <- err
				break
			}
			transfers[id] = transferStoreEntry{
				Descriptors: descriptors,
				Spec:        spec,
			}
			store.Channels.ReturnNewTransfer <- id
		case id := <-store.Channels.RequestDescriptors:
			if transfer, found := transfers[id]; found {
				store.Channels.ReturnDescriptors <- transfer.Descriptors
			} else {
				store.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case id := <-store.Channels.RequestSpec:
			if transfer, found := transfers[id]; found {
				store.Channels.ReturnSpec <- transfer.Spec
			} else {
				store.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case idAndStatus := <-store.Channels.SetStatus:
			if transfer, found := transfers[idAndStatus.Id]; found {
				transfer.Status = idAndStatus.Status
				transfers[idAndStatus.Id] = transfer
			} else {
				store.Channels.Error <- TransferNotFoundError{Id: idAndStatus.Id}
			}
		case id := <-store.Channels.RequestStatus:
			if transfer, found := transfers[id]; found {
				store.Channels.ReturnStatus <- transfer.Status
			} else {
				store.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case id := <-store.Channels.RequestRemoval:
			if _, found := transfers[id]; found {
				delete(transfers, id)
			} else {
				store.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case <-store.Channels.Stop:
			store.Channels.Error <- nil
			running = false
		}
	}
}

// an entry in the transfer metadata store
type transferStoreEntry struct {
	Descriptors []map[string]any
	Spec        Specification
	Status      TransferStatus
}
