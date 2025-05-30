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
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/databases/jdp"
	"github.com/kbase/dts/databases/kbase"
	"github.com/kbase/dts/databases/nmdc"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
	"github.com/kbase/dts/endpoints/local"
)

// useful type aliases
type Database = databases.Database
type Endpoint = endpoints.Endpoint
type FileTransfer = endpoints.FileTransfer

// this type holds a specification used to create a valid transfer
type Specification struct {
	// a Markdown description of the transfer task
	Description string
	// the name of destination database to which files are transferred (as
	// specified in the DTS config file)
	Destination string
	// machine-readable instructions for processing the payload at its destination
	Instructions map[string]any
	// an array of identifiers for files to be transferred from Source to
	// Destination
	FileIds []string
	// the name of source database from which files are transferred (as specified
	// in the DTS config file)
	Source string
	// information about the user requesting the task
	User auth.User
}

//----------
// Transfer
//----------

// This type tracks the lifecycle of a file transfer: the copying of files from
// a source database to a destination database. A transfer comprises one or
// more Tasks, depending on how many transfer endpoints are involved.
type Transfer struct {
	Canceled          bool
	CompletionTime    time.Time
	DataDescriptors   []any          // in-line data descriptors
	DestinationFolder string         // folder path to which files are transferred
	Id                uuid.UUID      // task identifier
	ManifestFile      string         // name of locally-created manifest file
	PayloadSize       float64        // size of payload (gigabytes)
	Specification     Specification  // from which this Transfer is created
	Status            TransferStatus // status of file transfer operation
	Tasks             []Task         // list of constituent tasks
}

type TransferStatusCode int

const (
	TransferStatusUnknown TransferStatusCode = iota
	TransferStatusNew
	TransferStatusStaging
	TransferStatusTransferring
	TransferStatusProcessing
	TransferStatusFinalizing
	TransferStatusCanceled
	TransferStatusSucceeded
	TransferStatusFailed
)

type TransferStatus struct {
	Code       TransferStatusCode
	ManifestId uuid.NullUUID // manifest generation UUID (if any)
	NumFiles   int
}

//------
// Task
//------

// A Task is an indivisible unit of work that is executed by stages in a pipeline.
type Task struct {
	TransferId          uuid.UUID  // ID of corresponding transfer
	Destination         string     // name of destination database (in config)
	DestinationEndpoint string     // name of destination database (in config)
	DestinationFolder   string     // folder path to which files are transferred
	Descriptors         []any      // Frictionless file descriptors
	Error               error      // indicates any error that has occurred
	Source              string     // name of source database (in config)
	SourceEndpoint      string     // name of source endpoint (in config)
	Status              TaskStatus // status of task
	User                auth.User  // info about user requesting transfer
}

type TaskStatusCode int

const (
	TaskStatusUnknown TaskStatusCode = iota
	TaskStatusNew
	TaskStatusStaging
	TaskStatusTransferring
	TaskStatusProcessing
	TaskStatusCanceled
	TaskStatusSucceeded
	TaskStatusFailed
)

type TaskStatus struct {
	Code           TaskStatusCode
	StagingId      uuid.NullUUID            // staging UUID (if any)
	StagingStatus  databases.StagingStatus  // staging status
	TransferId     uuid.NullUUID            // file transfer UUID (if any)
	TransferStatus endpoints.TransferStatus // status of file transfer operation
}

// singleton pipeline instance
var pipeline_ *Pipeline

// starts processing pipelines, returning an informative error if anything
// prevents it
func Start() error {
	if pipeline_ == nil {
		// register our built-in endpoint and database providers
		if err := endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint); err != nil {
			return err
		}
		if err := endpoints.RegisterEndpointProvider("local", local.NewEndpoint); err != nil {
			return err
		}
		if _, found := config.Databases["jdp"]; found {
			if err := databases.RegisterDatabase("jdp", jdp.NewDatabase); err != nil {
				return err
			}
		}
		if _, found := config.Databases["kbase"]; found {
			if err := databases.RegisterDatabase("kbase", kbase.NewDatabase); err != nil {
				return err
			}
		}
		if _, found := config.Databases["nmdc"]; found {
			if err := databases.RegisterDatabase("nmdc", nmdc.NewDatabase); err != nil {
				return err
			}
		}

		// do the necessary directories exist, and are they writable/readable?
		if err := validateDirectory("data", config.Service.DataDirectory); err != nil {
			return err
		}
		if err := validateDirectory("manifest", config.Service.ManifestDirectory); err != nil {
			return err
		}

		// can we access the local endpoint?
		if _, err := endpoints.NewEndpoint(config.Service.Endpoint); err != nil {
			return err
		}
		var err error
		pipeline_, err = CreatePipeline()
		if err != nil {
			return err
		}
	}
	return pipeline_.Start()
}

// Stops processing pipelines. Adding new pipelines and requesting statuses are
// disallowed in a stopped state.
func Stop() error {
	if pipeline_ != nil {
		return pipeline_.Stop()
	}
	return &NotRunningError{}
}

// Returns true if pipelines are currently being processed, false if not.
func Running() bool {
	if pipeline_ != nil {
		return pipeline_.Running()
	}
	return false
}

// Creates a new transfer associated with the user with the specified Orcid
// ID to the manager's set, returning a UUID for the task. The task is defined
// by specifying the names of the source and destination databases and a set of
// file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	if pipeline_ != nil {
		return pipeline_.CreateTransfer(spec)
	}
	return uuid.UUID{}, &NotRunningError{}
}

// Given a task UUID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(taskId uuid.UUID) (TransferStatus, error) {
	if pipeline_ != nil {
		return pipeline_.Status(taskId)
	}
	return TransferStatus{}, &NotRunningError{}
}

// Requests that the task with the given UUID be canceled. Clients should check
// the status of the task separately.
func Cancel(taskId uuid.UUID) error {
	if pipeline_ != nil {
		return pipeline_.Cancel(taskId)
	}
	return &NotRunningError{}
}
