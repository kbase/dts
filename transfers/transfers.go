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
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/databases/jdp"
	"github.com/kbase/dts/databases/kbase"
	"github.com/kbase/dts/databases/nmdc"
	s3db "github.com/kbase/dts/databases/s3"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
	"github.com/kbase/dts/endpoints/local"
	s3ep "github.com/kbase/dts/endpoints/s3"
	"github.com/kbase/dts/journal"
)

// useful type aliases
type Database = databases.Database
type Endpoint = endpoints.Endpoint
type FileTransfer = endpoints.FileTransfer
type TransferStatus = endpoints.TransferStatus
type TransferStatusCode = endpoints.TransferStatusCode

// useful constants
const (
	TransferStatusUnknown    = endpoints.TransferStatusUnknown
	TransferStatusStaging    = endpoints.TransferStatusStaging
	TransferStatusActive     = endpoints.TransferStatusActive
	TransferStatusFailed     = endpoints.TransferStatusFailed
	TransferStatusFinalizing = endpoints.TransferStatusFinalizing
	TransferStatusInactive   = endpoints.TransferStatusInactive
	TransferStatusSucceeded  = endpoints.TransferStatusSucceeded
)

// starts processing transfers according to the given configuration, returning an
// informative error if anything prevents this
func Start(conf config.Config) error {
	if global.Running {
		return &AlreadyRunningError{}
	}

	// if this is the first call to Start(), register our built-in endpoint and database providers
	if !global.Started {
		if err := registerEndpointProviders(); err != nil {
			return err
		}
		if err := registerDatabases(conf); err != nil {
			return err
		}
		global.Started = true
	}

	// do the necessary directories exist, and are they writable/readable?
	if err := validateDirectories(conf); err != nil {
		return err
	}

	// can we access the local endpoint?
	if _, err := endpoints.NewEndpoint(conf.Service.Endpoint); err != nil {
		return err
	}

	// fire up the transfer journal
	if err := journal.Init(); err != nil {
		return err
	}

	// start the dispatcher, which starts everything else
	if err := dispatcher.Start(); err != nil {
		return err
	}

	global.Running = true

	// subscribe to the transfer feed to log events
	subscription := Subscribe(32)
	go func() {
		for global.Running {
			message := <-subscription.Channel
			slog.Info(message.Description)
		}
	}()

	return nil
}

// Stops processing transfers. Adding new transfers and requesting transfer statuses are
// disallowed in a stopped state.
func Stop() error {
	var err error
	if global.Running {
		if err := dispatcher.Stop(); err != nil {
			return err
		}
		if err := unsubscribeAll(); err != nil {
			return err
		}
		if err = journal.Finalize(); err != nil {
			return err
		}
		global.Running = false
	} else {
		err = &NotRunningError{}
	}
	return err
}

// Returns true if transfers are currently being processed, false if not.
func Running() bool {
	return global.Running
}

// this type holds a specification used to create a valid transfer transfer
type Specification struct {
	// a Markdown description of the transfer transfer
	Description string
	// the name of destination database to which files are transferred (as specified in the config
	// file) OR a custom destination spec (<provider>:<id>:<credential>)
	Destination string
	// machine-readable instructions for processing the payload at its destination
	Instructions map[string]any
	// an array of identifiers for files to be transferred from Source to Destination
	FileIds []string
	// the name of source database from which files are transferred (as specified in the config file)
	Source string
	// the time at which the transfer is requested
	TimeOfRequest time.Time
	// information about the user requesting the transfer
	User auth.User
}

// Creates a new transfer associated with the user with the specified Orcid ID to the manager's set,
// returning a UUID for the transfer. The transfer is defined by specifying the names of the source
// and destination databases and a set of file IDs associated with the source.
func Create(spec Specification) (uuid.UUID, error) {
	// have we requested files to be transferred?
	if len(spec.FileIds) == 0 {
		return uuid.UUID{}, &NoFilesRequestedError{}
	}

	// verify the source and destination strings
	_, err := databases.NewDatabase(spec.Source) // source must refer to a database
	if err != nil {
		return uuid.UUID{}, err
	}

	// destination can be a database OR a custom location
	if _, err = databases.NewDatabase(spec.Destination); err != nil {
		if _, err = endpoints.ParseCustomSpec(spec.Destination); err != nil {
			return uuid.UUID{}, err
		}
	}

	spec.TimeOfRequest = time.Now()

	// create a new transfer and send it along for processing
	return dispatcher.CreateTransfer(spec)
}

// Given a transfer ID, returns its transfer status (or a non-nil error
// indicating any issues encountered).
func Status(transferId uuid.UUID) (TransferStatus, error) {
	return dispatcher.GetTransferStatus(transferId)
}

// Requests that the transfer with the given UUID be canceled. Clients should check
// the status of the transfer separately.
func Cancel(transferId uuid.UUID) error {
	return dispatcher.CancelTransfer(transferId)
}

//===========
// Internals
//===========

// globals
var global struct {
	Running, Started bool
}

//-----------------------------------------------
// Provider Registration and Resource Validation
//-----------------------------------------------

func registerEndpointProviders() error {
	// NOTE: it's okay if these endpoint providers have already been registered,
	// NOTE: as they can be used in testing
	endpointsToRegister := map[string]func(conf map[string]any) (endpoints.Endpoint, error){
		"globus": globus.EndpointConstructor,
		"local":  local.EndpointConstructor,
		"s3":     s3ep.EndpointConstructor,
	}
	for name, constructor := range endpointsToRegister {
		err := endpoints.RegisterEndpointProvider(name, constructor)
		if err != nil {
			// ignore AlreadyRegisteredError but propagate others
			if _, matches := err.(*endpoints.AlreadyRegisteredError); !matches {
				return err
			}
		}
	}
	return nil
}

var constructorMap map[string]func(config map[string]any) func() (databases.Database, error) = map[string]func(config map[string]any) func() (databases.Database, error){
	"jdp":   jdp.DatabaseConstructor,
	"kbase": kbase.DatabaseConstructor,
	"nmdc":  nmdc.DatabaseConstructor,
	"s3":    s3db.DatabaseConstructor,
}

// registers databases; if at least one database is available, no error is propagated
func registerDatabases(conf config.Config) error {
	for dbName, dbConf := range conf.Databases {
		// expand credentials within the config map
		if _, found := dbConf["credential"]; found {
			credString, ok := dbConf["credential"].(string)
			if !ok {
				return &InvalidDatabaseConfigError{
					Database: dbName,
					Message:  "credential field is not a string",
				}
			}
			if _, credFound := config.Credentials[credString]; !credFound {
				return &InvalidDatabaseConfigError{
					Database: dbName,
					Message:  fmt.Sprintf("credential '%s' not found", credString),
				}
			}
			dbConf["credential"] = config.Credentials[credString]
		}
		// add a transaction pruning time, if not specified
		if _, found := dbConf["delete_after"]; !found {
			dbConf["delete_after"] = conf.Service.DeleteAfter
		}
		constructor, ok := constructorMap[dbName]
		// if no constructor found, assume the database is already registered
		// (e.g., a test database)
		if ok {
			databases.RegisterDatabase(dbName, constructor(dbConf))
		}
	}
	// ensure at least one database is available
	if len(databases.RegisteredDatabases()) == 0 {
		return &NoDatabasesAvailable{}
	}
	return nil
}

func validateDirectories(conf config.Config) error {
	err := validateDirectory("data", conf.Service.DataDirectory)
	if err != nil {
		return err
	}
	return validateDirectory("manifest", conf.Service.ManifestDirectory)
}

// checks for the existence of a directory and whether it is readable/writeable, returning an error
// if these conditions are not met
func validateDirectory(dirType, dir string) error {
	if dir == "" {
		return fmt.Errorf("no %s directory was specified", dirType)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("%s is not a valid %s directory", dir, dirType),
		}
	}

	// can we write a file and read it?
	testFile := filepath.Join(dir, "test.txt")
	writtenTestData := []byte("test")
	err = os.WriteFile(testFile, writtenTestData, 0644)
	if err != nil {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("could not write to %s directory %s", dirType, dir),
		}
	}
	readTestData, err := os.ReadFile(testFile)
	if err == nil {
		os.Remove(testFile)
	}
	if err != nil || !bytes.Equal(readTestData, writtenTestData) {
		return &os.PathError{
			Op:   "validateDirectory",
			Path: dir,
			Err:  fmt.Errorf("could not read from %s directory %s", dirType, dir),
		}
	}
	return nil
}

// Returns a list of endpoints that can be used as sources or destinations
// for transfer with the given database.
func databaseEndpointNames(dbName string) ([]string, error) {
	db, err := databases.NewDatabase(dbName)
	if err != nil {
		return nil, err
	}
	return db.EndpointNames(), nil
}

//------------------------
// Transfer Orchestration
//------------------------

// The DTS orchestrates data transfer by requesting operations from service providers and monitoring
// their status. Transfers and status checks are handled by a family of goroutines that communicate
// with each other and the main goroutine via channels. These goroutines include:
//
// * dispatcher: handles all client requests, communicates with other goroutines as needed
// * stager: handles file staging by communicating with provider databases and endpoints
// * mover: handles file transfers by communicatig with provider databases and endpoints
// * manifestor: generates a transfer manifest after each transfer has completed and sends it to
//              the correct destination
// * store: maintains metadata records and status info for ongoing and completed transfers

// resolves the given destination (name) string, accounting for custom transfers
func determineDestinationEndpoint(destination string) (endpoints.Endpoint, error) {
	// everything's been validated at this point, so no need to check for errors
	if strings.Contains(destination, ":") { // custom transfer spec
		customSpec, err := endpoints.ParseCustomSpec(destination)
		if err != nil {
			return nil, err
		}
		endpointId, err := uuid.Parse(customSpec.Id)
		if err != nil {
			return nil, err
		}
		credential := config.Credentials[customSpec.Credential]
		clientId, err := uuid.Parse(credential.Id)
		if err != nil {
			return nil, err
		}
		conf := globus.Config{
			Name: fmt.Sprintf("Custom endpoint (%s)", endpointId.String()),
			Id:   endpointId.String(),
			Root: customSpec.Path,
			Credential: auth.Credential{
				Id:     clientId.String(),
				Secret: credential.Secret,
			},
		}
		return globus.NewEndpoint(conf)
	}
	endpts, err := databaseEndpointNames(destination)
	if err != nil {
		return nil, err
	}
	if len(endpts) != 1 {
		return nil, fmt.Errorf("cannot determine destination endpoint for database '%s'", destination)
	}
	return endpoints.NewEndpoint(endpts[0])
}

// resolves the folder at the given destination in which transferred files are deposited
func determineDestinationFolder(transferId uuid.UUID) (string, error) {
	spec, err := store.GetSpecification(transferId)
	if err != nil {
		return "", err
	}
	dtsFolder := "dts-" + transferId.String()
	if customSpec, err := endpoints.ParseCustomSpec(spec.Destination); err == nil { // custom transfer?
		return filepath.Join(customSpec.Path, dtsFolder), nil
	}
	destDb, err := databases.NewDatabase(spec.Destination)
	if err != nil {
		return "", err
	}
	username, err := destDb.LocalUser(spec.User.Orcid)
	if err != nil {
		return "", err
	}
	return filepath.Join(username, dtsFolder), nil
}

// given a set of Frictionless DataResource descriptors, returns a map mapping the name of each
// distinct endpoint to the descriptors associated with that endpoint; in the case of a single
// endpoint, all descriptors are assigned to it; in the case of more than one endpoint, the
// "endpoint" field is read from each descriptor, and an error is returned if a descriptor is
// encountered without this field
func descriptorsByEndpoint(spec Specification,
	descriptors []map[string]any) (map[string][]map[string]any, error) {
	descriptorsForEndpoint := make(map[string][]map[string]any)
	endpts, err := databaseEndpointNames(spec.Source)
	if err != nil {
		return nil, err
	}
	if len(endpts) == 0 {
		return nil, fmt.Errorf("no endpoints found for source database '%s'", spec.Source)
	}
	if len(endpts) > 1 { // more than one endpoint possible!
		distinctEndpoints := make(map[string]any)
		for _, descriptor := range descriptors {
			var endpoint string
			if key, found := descriptor["endpoint"]; found {
				endpoint = key.(string)
			} else {
				return nil, databases.ResourceEndpointNotFoundError{
					Database:   spec.Source,
					ResourceId: descriptor["id"].(string),
				}
			}
			if _, found := distinctEndpoints[endpoint]; !found {
				distinctEndpoints[endpoint] = struct{}{}
			}
		}

		for endpoint := range distinctEndpoints {
			endpointDescriptors := make([]map[string]any, 0)
			for _, descriptor := range descriptors {
				if endpoint == descriptor["endpoint"].(string) {
					endpointDescriptors = append(endpointDescriptors, descriptor)
				}
			}
			descriptorsForEndpoint[endpoint] = endpointDescriptors
		}
	} else { // assign all descriptors to the single endpoint
		descriptorsForEndpoint[endpts[0]] = descriptors
	}
	return descriptorsForEndpoint, nil
}
