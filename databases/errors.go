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

package databases

import (
	"fmt"
)

// This error type is returned when a database is sought but not found.
type NotFoundError struct {
	Database string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("The database '%s' was not found", e.Database)
}

// indicates that a user could not be authorized to access a database with their ORCID
type UnauthorizedError struct {
	Database, Message, User string
}

func (e UnauthorizedError) Error() string {
	if e.User != "" {
		return fmt.Sprintf("Unable to authorize user '%s' for database '%s': %s", e.User, e.Database, e.Message)
	} else {
		return fmt.Sprintf("Unable to authorize user for database '%s': %s", e.Database, e.Message)
	}
}

// indicates that a database exists but is currently unavailable
type UnavailableError struct {
	Database string
}

func (e UnavailableError) Error() string {
	return fmt.Sprintf("Cannot reach database '%s': unavailable", e.Database)
}

// This error type is returned when an invalid database-specific search
// parameter is specified
type InvalidSearchParameter struct {
	Database, Message string
}

func (e InvalidSearchParameter) Error() string {
	return fmt.Sprintf("Invalid search parameter for database '%s': %s", e.Database, e.Message)
}

// this error type is returned when a database's endpoint configuration is invalid
type InvalidEndpointsError struct {
	Database, Message string
}

func (e InvalidEndpointsError) Error() string {
	return fmt.Sprintf("Invalid endpoint configuration for database '%s': %s", e.Database, e.Message)
}

// this error type is returned when an endpoint associated with a resource is
// invalid
type InvalidResourceEndpointError struct {
	Database, ResourceId, Endpoint string
}

func (e InvalidResourceEndpointError) Error() string {
	return fmt.Sprintf("Invalid endpoint specified for resource '%s' in database '%s': %s",
		e.ResourceId, e.Database, e.Endpoint)
}

// this error type is returned when an operation requires a user ORCID but none
// is provided
type MissingOrcidError struct {
	Database string
}

func (e MissingOrcidError) Error() string {
	return fmt.Sprintf("Missing user ORCID for request to database '%s'", e.Database)
}

// this error type is returned when a resource is requested for which the requester
// does not have permission
type PermissionDeniedError struct {
	Database, ResourceId string
}

func (e PermissionDeniedError) Error() string {
	return fmt.Sprintf("Can't access resource '%s' in database '%s': permission denied",
		e.ResourceId, e.Database)
}

// this error type is returned when a resource is requested and is not found
type ResourceNotFoundError struct {
	Database, ResourceId string
}

func (e ResourceNotFoundError) Error() string {
	return fmt.Sprintf("Can't access file '%s' in database '%s': not found", e.ResourceId, e.Database)
}

// this error type is returned when an endpoint cannot be found for a file ID
type ResourceEndpointNotFoundError struct {
	Database, ResourceId string
}

func (e ResourceEndpointNotFoundError) Error() string {
	return fmt.Sprintf("Can't determine endpoint for resource '%s' in database '%s'", e.ResourceId, e.Database)
}

// this error type is emitted if an endpoint redirects an HTTPS request to an
// HTTP endpoint (it's NUTS that this can happen!)
type DowngradedRedirectError struct {
	Endpoint string
}

func (e DowngradedRedirectError) Error() string {
	return fmt.Sprintf("The endpoint %s is attempting to downgrade an HTTPS request to HTTP",
		e.Endpoint)
}
