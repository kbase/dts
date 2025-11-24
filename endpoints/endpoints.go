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

package endpoints

import (
	"sync"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
)

// this type holds all relevant information for the transfer of an individual
// file
type FileTransfer struct {
	// absolute source and destination paths on respective endpoints
	SourcePath, DestinationPath string
	// Hash and hash algorithm used to validate the file
	Hash, HashAlgorithm string
}

// this "enum" type encodes the status of a file transfer between endpoints
type TransferStatusCode int

const (
	TransferStatusUnknown    TransferStatusCode = iota
	TransferStatusStaging                       // files being staged
	TransferStatusActive                        // transfer in progress
	TransferStatusInactive                      // transfer suspended
	TransferStatusFinalizing                    // transfer manifest being generated
	TransferStatusSucceeded                     // transfer completed successfully
	TransferStatusFailed                        // transfer failed or was canceled
)

// this type conveys various information about a file transfer's status
type TransferStatus struct {
	// status code (see above)
	Code TransferStatusCode
	// message describing a failure status
	Message string
	// total number of files being transferred
	NumFiles int
	// number of files that have been transferred
	NumFilesTransferred int
	// number of files that are skipped for whatever reason
	NumFilesSkipped int
}

// This type represents an endpoint for transferring files.
type Endpoint interface {
	// Returns a string indicating the service provider for the endpoint.
	Provider() string
	// Returns the path on the file system that serves as the endpoint's root.
	Root() string
	// Returns true if the files associated with the given Frictionless
	// descriptors are staged at this endpoint AND are valid, false otherwise.
	FilesStaged(descriptors []map[string]any) (bool, error)
	// Returns a list of UUIDs for all transfers associated with this endpoint.
	Transfers() ([]uuid.UUID, error)
	// Begins a transfer task that moves the files identified by the FileTransfer
	// structs, returning a UUID that can be used to refer to this task. It is assumed that there
	// no duplicates in the list of files to be transfered.
	Transfer(dst Endpoint, files []FileTransfer) (uuid.UUID, error)
	// Retrieves the status for a transfer task identified by its UUID.
	Status(id uuid.UUID) (TransferStatus, error)
	// Cancels the transfer task with the given UUID (must return immediately,
	// even if an asynchronous cancellation has not been processed).
	Cancel(id uuid.UUID) error
}

// registers a database creation function under the given database name
// to allow for e.g. test database implementations
func RegisterEndpointProvider(provider string, createEp func(conf map[string]any) (Endpoint, error)) error {
	mu_.RLock()

	if _, found := createEndpointFuncs_[provider]; found {
		mu_.RUnlock()
		return &AlreadyRegisteredError{Provider: provider}
	} else {
		mu_.RUnlock()
		mu_.Lock()
		defer mu_.Unlock()
		createEndpointFuncs_[provider] = createEp
		return nil
	}
}

// returns whether an endpoint with the given name has been configured for use
func EndpointExists(endpointName string) bool {
	mu_.RLock()
	defer mu_.RUnlock()

	_, found := config.Endpoints[endpointName]
	return found
}

// creates an endpoint based on the configured type, or returns an existing
// instance
func NewEndpoint(endpointName string) (Endpoint, error) {
	var err error
	mu_.RLock()

	// do we have one of these already?
	endpoint, found := allEndpoints_[endpointName]
	if !found {
		// look in our configuration for the endpoint's provider
		if epConfig, epFound := config.Endpoints[endpointName]; epFound {
			provider, ok := epConfig["provider"].(string)
			if !ok {
				mu_.RUnlock()
				return nil, &InvalidProviderError{
					Name:     endpointName,
					Provider: "",
				}
			}
			// expand the credential from its name
			if credName, ok := epConfig["credential"].(string); ok {
				epConfig["credential"] = config.Credentials[credName]
			}
			if createEp, valid := createEndpointFuncs_[provider]; valid {
				endpoint, err = createEp(epConfig)
			} else { // invalid provider!
				err = InvalidProviderError{
					Name:     endpointName,
					Provider: provider,
				}
			}
		} else { // endpoint not found in config!
			err = NotFoundError{Name: endpointName}
		}

		mu_.RUnlock()

		// stash it
		if err == nil {
			mu_.Lock()
			defer mu_.Unlock()
			allEndpoints_[endpointName] = endpoint
		}
	} else {
		mu_.RUnlock()
	}
	return endpoint, err
}

// we maintain a table of endpoint instances, identified by their names
var allEndpoints_ map[string]Endpoint = make(map[string]Endpoint)

// here's a table of endpoint creation functions
var createEndpointFuncs_ = make(map[string]func(conf map[string]any) (Endpoint, error))

// global state protector
var mu_ sync.RWMutex
