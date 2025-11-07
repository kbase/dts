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
	"fmt"

	"github.com/google/uuid"
)

// indicates that an endpoint is sought but not found
type NotFoundError struct {
	Name string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("The endpoint '%s' was not found.", e.Name)
}

// indicates that an endpoint has an invalid provider
type InvalidProviderError struct {
	Name, Provider string
}

func (e InvalidProviderError) Error() string {
	return fmt.Sprintf("The endpoint '%s' has an invalid provider: '%s'.",
		e.Name, e.Provider)
}

// indicates that a custom endpoint (string) specification is invalid
type InvalidCustomSpecError struct {
	String, Message string
}

func (e InvalidCustomSpecError) Error() string {
	return fmt.Sprintf("The custom endpoint specification '%s' is invalid: '%s'.",
		e.String, e.Message)
}

// indicates that an endpoint provider is already registered and an attempt has
// been made to register it again
type AlreadyRegisteredError struct {
	Provider string
}

func (e AlreadyRegisteredError) Error() string {
	return fmt.Sprintf("Cannot register endpoint provider '%s': already registered", e.Provider)
}

// indicates that a source endpoint has been asked to transfer files to an incompatible destination
type IncompatibleDestinationError struct {
	Source, SourceProvider           string
	Destination, DestinationProvider string
	Message                          string
}

func (e IncompatibleDestinationError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("The source endpoint '%s' (%s) cannot transfer files to the destination endpoint '%s' (%s): %s",
			e.Source, e.SourceProvider, e.Destination, e.DestinationProvider, e.Message)
	}
	return fmt.Sprintf("The source endpoint '%s' (%s) cannot transfer files to the destination endpoint '%s' (%s)",
		e.Source, e.SourceProvider, e.Destination, e.DestinationProvider)
}

// indicates that an invalid transfer id has been specified
type TransferNotFoundError struct {
	Id uuid.UUID
}

func (e TransferNotFoundError) Error() string {
	return fmt.Sprintf("The transfer ID '%s' was not found.", e.Id.String())
}
