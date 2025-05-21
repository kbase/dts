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

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
)

// indicates that Start() has been called when tasks are being processed
type AlreadyRunningError struct{}

func (t AlreadyRunningError) Error() string {
	return fmt.Sprintf("Pipelines are already running and cannot be started again.")
}

// indicates that a transfer specification has an invalid source
type InvalidSourceError struct {
	Source string
}

func (t InvalidSourceError) Error() string {
	return fmt.Sprintf("Invalid source for requested transfer: %s", t.Source)
}

// indicates that a transfer specification has an invalid destination
type InvalidDestinationError struct {
	Destination string
}

func (t InvalidDestinationError) Error() string {
	return fmt.Sprintf("Invalid destination for requested transfer: %s", t.Destination)
}

// indicates that Stop() has been called when tasks are not being processed
type NotRunningError struct{}

func (t NotRunningError) Error() string {
	return fmt.Sprintf("Pipelines are not currently being processed.")
}

// indicates that a transfer has been requested with no files(!)
type NoFilesRequestedError struct{}

func (t NoFilesRequestedError) Error() string {
	return fmt.Sprintf("Requested transfer includes no file IDs!")
}

// indicates that a transfer is sought but not found
type NotFoundError struct {
	Id uuid.UUID
}

func (t NotFoundError) Error() string {
	return fmt.Sprintf("The transfer %s was not found.", t.Id.String())
}

// indicates that a transfer payload has been requested that is too large
type PayloadTooLargeError struct {
	Size float64 // size of the requested payload in gigabytes
}

func (e PayloadTooLargeError) Error() string {
	return fmt.Sprintf("Requested payload is too large: %g GB (limit is %g GB).",
		e.Size, config.Service.MaxPayloadSize)
}
