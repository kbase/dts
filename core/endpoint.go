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

package core

import (
	"github.com/google/uuid"
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
	TransferStatusFailed                        // transfer failed
)

// this type conveys various information about a file transfer's status
type TransferStatus struct {
	// status code (see above)
	Code TransferStatusCode
	// total number of files being transferred
	NumFiles int
	// number of files that have been transferred
	NumFilesTransferred int
}

// This type represents an endpoint for transferring files.
type Endpoint interface {
	// returns true if the files associated with the given DataResources are
	// staged at this endpoint AND are valid, false otherwise
	FilesStaged(files []DataResource) (bool, error)
	// returns a list of UUIDs for all transfers associated with this endpoint
	Transfers() ([]uuid.UUID, error)
	// begins a transfer task that moves the files identified by the FileTransfer
	// structs, returning a UUID that can be used to refer to this task.
	Transfer(dst Endpoint, files []FileTransfer) (uuid.UUID, error)
	// retrieves the status for a transfer task identified by its UUID
	Status(id uuid.UUID) (TransferStatus, error)
	// cancels the transfer task with the given UUID
	Cancel(id uuid.UUID) error
}
