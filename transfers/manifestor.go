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
	"strings"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/journal"
)

//------------
// Manifestor
//------------

// The manifestor generates a JSON manifest for each successful transfer and sends it to the
// transfer's destination. This manifest contains a Frictionless DataPackage containing all
// descriptors relevant to the transfer.
//
// The manifestor responds to requests from the mover to generate a manifest and transfer it to the
// transfer destination, updating the status of the manifest's transfer via the store as needed.
//
// The manifestor is started and stopped by the dispatcher.

// manifestor global state
var manifestor manifestorState

type manifestorState struct {
	Channels  manifestorChannels
	Endpoints map[string]endpoints.Endpoint
}

type manifestorChannels struct {
	RequestGeneration   chan uuid.UUID
	RequestCancellation chan uuid.UUID
	Error               chan error
	SaveAndStop         chan *gob.Encoder
}

func newManifestorChannels() manifestorChannels {
	return manifestorChannels{
		RequestGeneration:   make(chan uuid.UUID),
		RequestCancellation: make(chan uuid.UUID),
		Error:               make(chan error),
		SaveAndStop:         make(chan *gob.Encoder),
	}
}

func (channels *manifestorChannels) Close() {
	close(channels.RequestGeneration)
	close(channels.RequestCancellation)
	close(channels.Error)
	close(channels.SaveAndStop)
}

func (m *manifestorState) Start() error {
	m.Channels = newManifestorChannels()
	m.Endpoints = make(map[string]endpoints.Endpoint)
	go m.process(nil)
	return <-m.Channels.Error
}

func (m *manifestorState) Load(decoder *gob.Decoder) error {
	m.Channels = newManifestorChannels()
	m.Endpoints = make(map[string]endpoints.Endpoint)
	go m.process(decoder)
	return <-m.Channels.Error
}

// stops the manifestor goroutine
func (m *manifestorState) SaveAndStop(encoder *gob.Encoder) error {
	m.Channels.SaveAndStop <- encoder
	err := <-m.Channels.Error
	m.Channels.Close()
	return err
}

// starts generating a manifest for the given transfer, moving it subsequently to that transfer's
// destination
func (m *manifestorState) Generate(transferId uuid.UUID) error {
	m.Channels.RequestGeneration <- transferId
	return <-m.Channels.Error
}

// cancels the generation/transfer of a manifest
// destination
func (m *manifestorState) Cancel(transferId uuid.UUID) error {
	m.Channels.RequestCancellation <- transferId
	return <-m.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the manifestor's goroutine
//----------------------------------------------------

type manifestEntry struct {
	ManifestTransferId uuid.UUID
	Manifest           map[string]any
	Filename           string
}

// the goroutine itself (accepts optional decoder for loading saved data)
func (m *manifestorState) process(decoder *gob.Decoder) {
	// load or create transfer records
	var transfers map[uuid.UUID]manifestEntry
	if decoder != nil {
		if err := decoder.Decode(&transfers); err != nil {
			m.Channels.Error <- err
			return
		}
	} else {
		transfers = make(map[uuid.UUID]manifestEntry)
	}

	running := true
	pulse := clock.Subscribe()
	m.Channels.Error <- nil

	for running {
		select {
		case transferId := <-m.Channels.RequestGeneration:
			entry, err := m.generateAndSendManifest(transferId)
			if err == nil {
				transfers[transferId] = entry
			}
			m.Channels.Error <- err
		case transferId := <-m.Channels.RequestCancellation:
			if entry, found := transfers[transferId]; found {
				err := m.cancel(entry.ManifestTransferId)
				if err == nil {
					delete(transfers, transferId)
				}
				m.Channels.Error <- err
			} else {
				m.Channels.Error <- TransferNotFoundError{Id: transferId}
			}
		case <-pulse:
			// check the manifest transfers
			for transferId, entry := range transfers {
				completed, err := m.updateStatus(transferId, entry)
				if err != nil {
					slog.Error(err.Error())
					continue
				}
				if completed {
					os.Remove(entry.Filename)
					delete(transfers, transferId)
				}
			}
		case encoder := <-m.Channels.SaveAndStop:
			m.Channels.Error <- encoder.Encode(transfers)
			running = false
		}
	}
	clock.Unsubscribe()
}

func (m *manifestorState) generateAndSendManifest(transferId uuid.UUID) (manifestEntry, error) {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return manifestEntry{}, err
	}
	manifest, err := m.generateManifest(transferId, spec)
	if err != nil {
		return manifestEntry{}, err
	}

	filename := filepath.Join(config.Service.ManifestDirectory, fmt.Sprintf("manifest-%s.json", transferId.String()))
	if len(manifest["resources"].([]any)) > 0 {
		pkg, err := datapackage.New(manifest, ".")
		if err != nil {
			return manifestEntry{}, err
		}
		if err := pkg.SaveDescriptor(filename); err != nil {
			return manifestEntry{}, fmt.Errorf("creating manifest file: %s", err.Error())
		}
	} else {
		// if no resources were transferred, just create an empty file
		f, err := os.Create(filename)
		if err != nil {
			return manifestEntry{}, fmt.Errorf("creating manifest file: %s", err.Error())
		}
		f.Close()
	}

	// begin transferring the manifest
	source, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return manifestEntry{}, err
	}
	destination, err := determineDestinationEndpoint(spec.Destination)
	if err != nil {
		return manifestEntry{}, err
	}
	destinationFolder, err := determineDestinationFolder(transferId)
	if err != nil {
		return manifestEntry{}, err
	}
	manifestXferId, err := source.Transfer(destination, []FileTransfer{
		{
			SourcePath:      filename,
			DestinationPath: filepath.Join(destinationFolder, "manifest.json"),
		},
	})
	if err != nil {
		return manifestEntry{}, fmt.Errorf("transferring manifest file: %s", err.Error())
	}

	status, err := store.GetStatus(transferId)
	if err != nil {
		return manifestEntry{}, err
	}
	status.Code = TransferStatusFinalizing
	return manifestEntry{
		ManifestTransferId: manifestXferId,
		Manifest:           manifest,
		Filename:           filename,
	}, store.SetStatus(transferId, status)
}

// generates a manifest for the transfer with the given ID and begins transferring it to its
// destination
func (m *manifestorState) generateManifest(transferId uuid.UUID, spec Specification) (map[string]any, error) {
	descriptors, err := store.GetDescriptors(transferId)
	if err != nil {
		return nil, err
	}

	user := map[string]any{
		"id":    transferId.String(),
		"title": spec.User.Name,
		"role":  "author",
	}
	if spec.User.Organization != "" {
		user["organization"] = spec.User.Organization
	}
	if spec.User.Email != "" {
		user["email"] = spec.User.Email
	}

	// NOTE: for non-custom transfers, we embed the local username for the destination database in
	// this record in case it's useful (e.g. for the KBase staging service)
	var username string
	if _, err := endpoints.ParseCustomSpec(spec.Destination); err != nil { // custom transfer?
		destination, err := databases.NewDatabase(spec.Destination)
		if err != nil {
			return nil, err
		}
		username, err = destination.LocalUser(spec.User.Orcid)
		if err != nil {
			return nil, err
		}
	}

	jsonDescriptors := make([]any, len(descriptors))
	for i, descriptor := range descriptors {
		jsonDescriptors[i] = descriptor
	}

	packageDescriptor := map[string]any{
		"name":      "manifest",
		"resources": jsonDescriptors,
		"created":   time.Now().Format(time.RFC3339),
		"profile":   "data-package",
		"keywords":  []any{"dts", "manifest"},
		"contributors": []any{
			user,
		},
		"description":  spec.Description,
		"instructions": spec.Instructions,
		"username":     username,
	}

	return packageDescriptor, nil
}

// update the status of the manifest transfer with the given ID, returning true if the transfer has
// completed (successfully or unsuccessfully), false otherwise
func (m *manifestorState) updateStatus(transferId uuid.UUID, entry manifestEntry) (bool, error) {
	oldStatus, err := store.GetStatus(transferId)
	if err != nil {
		return false, err
	}
	newStatus := oldStatus

	source, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return false, err
	}
	manifestStatus, err := source.Status(entry.ManifestTransferId)
	if err != nil {
		return false, err
	}
	if manifestStatus.Code == TransferStatusSucceeded || manifestStatus.Code == TransferStatusFailed {
		newStatus.Code = manifestStatus.Code
		if newStatus.Code == TransferStatusSucceeded {
			newStatus.Message = fmt.Sprintf("Transfer %s: completed successfully", transferId.String())
		} else {
			newStatus.Message = fmt.Sprintf("Transfer %s: failed (%s)", transferId.String(), newStatus.Message)
		}
		if err := store.SetStatus(transferId, newStatus); err != nil {
			return true, err
		}

		// write a transfer record to the journal
		spec, err := store.GetSpecification(transferId)
		if err != nil {
			return true, err
		}
		size, err := store.GetPayloadSize(transferId)
		if err != nil {
			return true, err
		}
		var statusString string
		if strings.Contains(newStatus.Message, "success") {
			statusString = "succeeded"
		} else {
			statusString = "failed"
		}
		var pkg *datapackage.Package = nil
		if len(entry.Manifest["resources"].([]any)) > 0 {
			pkg, err = datapackage.New(entry.Manifest, ".")
			if err != nil {
				return false, err
			}
		}
		err = journal.RecordTransfer(journal.Record{
			Id:          transferId,
			Source:      spec.Source,
			Destination: spec.Destination,
			Orcid:       spec.User.Orcid,
			StartTime:   spec.TimeOfRequest,
			StopTime:    time.Now(),
			Status:      statusString,
			PayloadSize: size,
			NumFiles:    len(spec.FileIds),
			Manifest:    pkg,
		})
		if err != nil {
			slog.Error(err.Error())
		}
		publish(Message{
			Description:    newStatus.Message,
			TransferId:     transferId,
			TransferStatus: newStatus,
			Time:           time.Now(),
		})
		return true, nil
	} else {
		return false, nil
	}
}

func (m *manifestorState) cancel(manifestXferId uuid.UUID) error {
	endpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return err
	}
	return endpoint.Cancel(manifestXferId)
}
