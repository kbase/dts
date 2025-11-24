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
	"cmp"
	"encoding/gob"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
)

//-------
// Store
//-------

// The transfer metadata store maintains a table of active and completed transfers and all related
// metadata. The store only tracks the state of the transfers--it doesn't initiate any activity.
//
// The store can create a new transfer record given a specification. This operation returns a UUID
// that can be used to fetch and manipulate the record in the following ways:
//
// * the specification for the transfer can be requested
// * an array of Frictionless DataResources ("descriptors") for the transfer can be requested
// * the status of a transfer can be requested or updated
// * a transfer record can be removed
//
// The store removes records that are older than the given maximum record "age". The age of a
// transfer record is the amount of time that has elapsed since the transfer completed (successfully
// or unsuccessfully).
//
// The store is started and stopped by the dispatcher.

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

	RequestPayloadSize chan uuid.UUID
	ReturnPayloadSize  chan uint64

	SetStatus     chan transferIdAndStatus
	RequestStatus chan uuid.UUID
	ReturnStatus  chan TransferStatus

	RequestRemoval chan uuid.UUID

	Error       chan error
	SaveAndStop chan *gob.Encoder
}

func newStoreChannels() storeChannels {
	numClients := 4 // dispatcher, stager, mover, manifestor
	return storeChannels{
		RequestNewTransfer: make(chan Specification),
		ReturnNewTransfer:  make(chan uuid.UUID),
		RequestSpec:        make(chan uuid.UUID, numClients),
		ReturnSpec:         make(chan Specification),
		RequestDescriptors: make(chan uuid.UUID, numClients),
		ReturnDescriptors:  make(chan []map[string]any),
		RequestPayloadSize: make(chan uuid.UUID, numClients),
		ReturnPayloadSize:  make(chan uint64),
		SetStatus:          make(chan transferIdAndStatus, numClients),
		RequestStatus:      make(chan uuid.UUID, numClients),
		ReturnStatus:       make(chan TransferStatus),
		RequestRemoval:     make(chan uuid.UUID, numClients),
		Error:              make(chan error),
		SaveAndStop:        make(chan *gob.Encoder),
	}
}

func (channels *storeChannels) Close() {
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
	close(channels.SaveAndStop)
}

type transferIdAndStatus struct {
	Id     uuid.UUID
	Status TransferStatus
}

func (s *storeState) Start() error {
	s.Channels = newStoreChannels()
	go s.process(nil)
	return <-s.Channels.Error
}

func (s *storeState) Load(decoder *gob.Decoder) error {
	s.Channels = newStoreChannels()
	go s.process(decoder)
	return <-s.Channels.Error
}

func (s *storeState) SaveAndStop(encoder *gob.Encoder) error {
	s.Channels.SaveAndStop <- encoder
	err := <-s.Channels.Error
	s.Channels.Close()
	return err
}

// creates a new entry for a transfer within the store, populating it with relevant metadata and
// returning a UUID, number of files, and/or error condition for the request
func (s *storeState) NewTransfer(spec Specification) (uuid.UUID, error) {
	s.Channels.RequestNewTransfer <- spec
	select {
	case id := <-s.Channels.ReturnNewTransfer:
		return id, nil
	case err := <-s.Channels.Error:
		return uuid.UUID{}, err
	}
}

func (s *storeState) GetSpecification(transferId uuid.UUID) (Specification, error) {
	s.Channels.RequestSpec <- transferId
	select {
	case spec := <-s.Channels.ReturnSpec:
		return spec, nil
	case err := <-s.Channels.Error:
		return Specification{}, err
	}
}

func (s *storeState) GetDescriptors(transferId uuid.UUID) ([]map[string]any, error) {
	s.Channels.RequestDescriptors <- transferId
	select {
	case descriptors := <-s.Channels.ReturnDescriptors:
		return descriptors, nil
	case err := <-s.Channels.Error:
		return nil, err
	}
}

func (s *storeState) GetPayloadSize(transferId uuid.UUID) (uint64, error) {
	s.Channels.RequestPayloadSize <- transferId
	select {
	case size := <-s.Channels.ReturnPayloadSize:
		return size, nil
	case err := <-s.Channels.Error:
		return 0, err
	}
}

func (s *storeState) SetStatus(transferId uuid.UUID, status TransferStatus) error {
	s.Channels.SetStatus <- transferIdAndStatus{
		Id:     transferId,
		Status: status,
	}
	return <-s.Channels.Error
}

func (s *storeState) GetStatus(transferId uuid.UUID) (TransferStatus, error) {
	s.Channels.RequestStatus <- transferId
	select {
	case status := <-s.Channels.ReturnStatus:
		return status, nil
	case err := <-s.Channels.Error:
		return TransferStatus{}, err
	}
}

func (s *storeState) Remove(transferId uuid.UUID) error {
	s.Channels.RequestRemoval <- transferId
	return <-s.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the store's goroutine
//----------------------------------------------------

// the goroutine itself
func (s *storeState) process(decoder *gob.Decoder) {
	// load or create transfer records
	var transfers map[uuid.UUID]transferStoreEntry
	if decoder != nil {
		if err := decoder.Decode(&transfers); err != nil {
			s.Channels.Error <- err
			return
		}
	} else {
		transfers = make(map[uuid.UUID]transferStoreEntry)
	}

	running := true
	pulse := clock.Subscribe()
	s.Channels.Error <- nil

	// time period after which to delete completed transfers
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	for running {
		select {
		case spec := <-s.Channels.RequestNewTransfer:
			id, transfer, err := s.newTransfer(spec)
			if err != nil {
				s.Channels.Error <- err
			} else {
				transfers[id] = transfer
				s.Channels.ReturnNewTransfer <- id
			}
		case id := <-s.Channels.RequestDescriptors:
			if transfer, found := transfers[id]; found {
				s.Channels.ReturnDescriptors <- transfer.Descriptors
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case id := <-s.Channels.RequestPayloadSize:
			if transfer, found := transfers[id]; found {
				var size uint64
				for _, descriptor := range transfer.Descriptors {
					switch v := descriptor["bytes"].(type) {
					case int:
						size += uint64(v)
					case int64:
						size += uint64(v)
					default:
						s.Channels.Error <- fmt.Errorf("invalid 'bytes' field type in descriptor: %T", v)
					}
				}
				s.Channels.ReturnPayloadSize <- size
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case id := <-s.Channels.RequestSpec:
			if transfer, found := transfers[id]; found {
				s.Channels.ReturnSpec <- transfer.Spec
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case idAndStatus := <-s.Channels.SetStatus:
			if transfer, found := transfers[idAndStatus.Id]; found {
				transfer.Status = idAndStatus.Status
				if idAndStatus.Status.Code == TransferStatusFailed || idAndStatus.Status.Code == TransferStatusSucceeded {
					transfer.CompletionTime = time.Now()
				}
				transfers[idAndStatus.Id] = transfer
				s.Channels.Error <- nil
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: idAndStatus.Id}
			}
		case id := <-s.Channels.RequestStatus:
			if transfer, found := transfers[id]; found {
				s.Channels.ReturnStatus <- transfer.Status
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case id := <-s.Channels.RequestRemoval:
			if _, found := transfers[id]; found {
				delete(transfers, id)
				s.Channels.Error <- nil
			} else {
				s.Channels.Error <- TransferNotFoundError{Id: id}
			}
		case <-pulse: // prune old records
			for id, transfer := range transfers {
				if transfer.Status.Code == TransferStatusFailed || transfer.Status.Code == TransferStatusSucceeded {
					if time.Since(transfer.CompletionTime) > deleteAfter {
						delete(transfers, id)
					}
				}
			}
		case encoder := <-s.Channels.SaveAndStop:
			s.Channels.Error <- encoder.Encode(transfers)
			running = false
		}
	}
}

// an entry in the transfer metadata store
type transferStoreEntry struct {
	CompletionTime time.Time
	Descriptors    []map[string]any
	Spec           Specification
	Status         TransferStatus
}

func (s *storeState) newTransfer(spec Specification) (uuid.UUID, transferStoreEntry, error) {
	id := uuid.New()
	source, err := databases.NewDatabase(spec.Source)
	if err != nil {
		return id, transferStoreEntry{}, err
	}
	descriptors, err := source.Descriptors(spec.User.Orcid, spec.FileIds)
	if err != nil {
		return id, transferStoreEntry{}, err
	}
	slices.SortFunc(descriptors, func(a, b map[string]any) int {
		return cmp.Compare(a["id"].(string), b["id"].(string))
	})
	entry := transferStoreEntry{
		Descriptors: descriptors,
		Spec:        spec,
		Status: TransferStatus{
			NumFiles: len(spec.FileIds),
		},
	}

	var size uint64
	for _, descriptor := range entry.Descriptors {
		switch v := descriptor["bytes"].(type) {
		case int:
			size += uint64(v)
		case int64:
			size += uint64(v)
		default:
			return id, transferStoreEntry{}, fmt.Errorf("invalid 'bytes' field type in descriptor: %T", v)
		}
	}
	publish(Message{
		Description:    fmt.Sprintf("Created new transfer %s (%d file(s), %g GB)", id, entry.Status.NumFiles, float64(size)/float64(1024*1024*1024)),
		TransferId:     id,
		TransferStatus: entry.Status,
		Time:           time.Now(),
	})

	return id, entry, err
}
