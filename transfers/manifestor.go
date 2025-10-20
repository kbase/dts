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
	"path/filepath"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

//------------
// Manifestor
//------------

// The manifestor generates a JSON manifest for each successful transfer and sends it to the
// transfer's destination. The manifest contains a Frictionless DataPackage containing all
// descriptors relevant to the transfer.

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
	Stop                chan struct{}
}

func (channels *manifestorChannels) close() {
	close(channels.RequestGeneration)
	close(channels.RequestCancellation)
	close(channels.Error)
	close(channels.Stop)
}

// starts the mover
func (m *manifestorState) Start() error {
	m.Channels = manifestorChannels{
		RequestGeneration:   make(chan uuid.UUID, 32),
		RequestCancellation: make(chan uuid.UUID, 32),
		Error:               make(chan error, 32),
		Stop:                make(chan struct{}),
	}
	m.Endpoints = make(map[string]endpoints.Endpoint)
	go m.process()
	return nil
}

// stops the manifestor goroutine
func (m *manifestorState) Stop() error {
	m.Channels.Stop <- struct{}{}
	err := <-m.Channels.Error
	m.Channels.close()
	return err
}

// starts generating a manifest for the given transfer, moving it subsequently to that transfer's
// destination
func (m *manifestorState) Generate(transferId uuid.UUID) error {
	m.Channels.RequestGeneration <- transferId
	return <-mover.Channels.Error
}

// cancels the generation/transfer of a manifest
// destination
func (m *manifestorState) Cancel(transferId uuid.UUID) error {
	m.Channels.RequestCancellation <- transferId
	return <-mover.Channels.Error
}

//----------------------------------------------------
// everything past here runs in the mover's goroutine
//----------------------------------------------------

// the goroutine itself
func (m *manifestorState) process() {
	running := true
	manifestTransfers := make(map[uuid.UUID]uuid.UUID)
	pulse := clock.Subscribe()

	for running {
		select {
		case transferId := <-manifestor.Channels.RequestGeneration:
			manifestXferId, err := m.generateAndSendManifest(transferId)
			if err != nil {
				manifestor.Channels.Error <- err
			}
			manifestTransfers[transferId] = manifestXferId
		case transferId := <-manifestor.Channels.RequestCancellation:
			if manifestXferId, found := manifestTransfers[transferId]; found {
				err := m.cancel(manifestXferId)
				if err == nil {
					delete(manifestTransfers, transferId)
				}
				manifestor.Channels.Error <- err
			} else {
				manifestor.Channels.Error <- NotFoundError{Id: transferId}
			}
		case <-pulse:
			// check the manifest transfers
			for transferId, manifestXferId := range manifestTransfers {
				completed, err := m.updateStatus(transferId, manifestXferId)
				if err != nil {
					mover.Channels.Error <- err
					continue
				}
				if completed {
					delete(manifestTransfers, transferId)
				}
			}
		case <-manifestor.Channels.Stop:
			running = false
			manifestor.Channels.Error <- nil
		}
	}
	clock.Unsubscribe()
}

func (m *manifestorState) generateAndSendManifest(transferId uuid.UUID) (uuid.UUID, error) {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return uuid.UUID{}, err
	}
	manifest, err := m.generateManifest(transferId, spec)
	if err != nil {
		return uuid.UUID{}, err
	}

	manifestFile := filepath.Join(config.Service.ManifestDirectory, fmt.Sprintf("manifest-%s.json", transferId.String()))
	if err := manifest.SaveDescriptor(manifestFile); err != nil {
		return uuid.UUID{}, fmt.Errorf("creating manifest file: %s", err.Error())
	}

	// begin transferring the manifest
	source, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return uuid.UUID{}, err
	}
	destination, err := destinationEndpoint(spec.Destination)
	if err != nil {
		return uuid.UUID{}, err
	}
	manifestXferId, err := source.Transfer(destination, []FileTransfer{
		{
			SourcePath:      manifestFile,
			DestinationPath: filepath.Join(destinationFolder(spec.Destination), "manifest.json"),
		},
	})
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("transferring manifest file: %s", err.Error())
	}

	status, err := store.GetStatus(transferId)
	if err != nil {
		return uuid.UUID{}, err
	}
	status.Code = TransferStatusFinalizing
	return manifestXferId, store.SetStatus(transferId, status)
}

// generates a manifest for the transfer with the given ID and begins transferring it to its
// destination
func (m *manifestorState) generateManifest(transferId uuid.UUID, spec Specification) (*datapackage.Package, error) {
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

	packageDescriptor := map[string]any{
		"name":      "manifest",
		"resources": descriptors,
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

	return datapackage.New(packageDescriptor, ".")
}

// update the status of the manifest transfer with the given ID, returning true if the transfer has
// completed (successfully or unsuccessfully), false otherwise
func (m *manifestorState) updateStatus(transferId, manifestXferId uuid.UUID) (bool, error) {
	source, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return false, err
	}
	status, err := source.Status(manifestXferId)
	if err != nil {
		return false, err
	}
	store.SetStatus(transferId, status)
	return status.Code == TransferStatusSucceeded || status.Code == TransferStatusFailed, nil
}

func (m *manifestorState) cancel(manifestXferId uuid.UUID) error {
	endpoint, err := endpoints.NewEndpoint(config.Service.Endpoint)
	if err != nil {
		return err
	}
	return endpoint.Cancel(manifestXferId)
}
