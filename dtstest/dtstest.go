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

// This package contains testing utilities for the Data Transfer System.
package dtstest

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// Enables DEBUG log messages for DTS's structured log (slog).
func EnableDebugLogging() {
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelDebug)
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(h))
}

// Given a specified configuration that has been initialized, this function
// registers test fixtures for databases and endpoints that meet the following
// criteria:
// * a database contains the word "test" in its identifier (YAML key)
// * an endpoint contains the word "test" in its "provider" field
// Each endpoint test fixture is created with the given set of options and
// resources.
func RegisterTestFixturesFromConfig(endpointOptions EndpointOptions,
	resources map[string]frictionless.DataResource) error {
	// has config.Init() been called?
	if len(config.Endpoints) == 0 && len(config.Databases) == 0 {
		return fmt.Errorf(`No endpoints or databases were found in the configuration.
Did you call config.Init()?`)
	}

	// register endpoint fixtures
	for endpointName, endpointConfig := range config.Endpoints {
		if strings.Contains(endpointConfig.Provider, "test") {
			RegisterEndpoint(endpointName, endpointOptions)
		}
	}

	// register database fixtures
	for databaseName := range config.Databases {
		if strings.Contains(databaseName, "test") {
			RegisterDatabase(databaseName, resources)
		}
	}

	return nil
}

//------------------------
// Endpoint Test Fixtures
//------------------------

type transferInfo struct {
	Time   time.Time // transfer initiation time
	Status endpoints.TransferStatus
}

// This type contains options for Endpoint test fixtures
type EndpointOptions struct {
	// time it takes to "stage files"
	StagingDuration time.Duration
	// time it takes to "transfer files"
	TransferDuration time.Duration
}

// This type implements an Endpoint test fixture
type Endpoint struct {
	// database fixture attached to endpoint
	Database *Database
	// endpoint testing options
	Options EndpointOptions
	// a table of ongoing "file transfers"
	Xfers map[uuid.UUID]transferInfo
	// root path
	RootPath string
}

// Registers an endpoint test fixture with the given name in the configuration,
// assigning it options that govern its testing behavior, and assigning it the
// given set of Frictionless DataResources as "test files."
func RegisterEndpoint(endpointName string, options EndpointOptions) error {
	slog.Debug(fmt.Sprintf("Registering test endpoint %s...", endpointName))
	newEndpointFunc := func(name string) (endpoints.Endpoint, error) {
		return &Endpoint{
			Options:  options,
			Xfers:    make(map[uuid.UUID]transferInfo),
			RootPath: config.Endpoints[endpointName].Root,
		}, nil
	}
	provider := config.Endpoints[endpointName].Provider
	return endpoints.RegisterEndpointProvider(provider, newEndpointFunc)
}

func (ep *Endpoint) Root() string {
	return ep.RootPath
}

func (ep *Endpoint) FilesStaged(files []frictionless.DataResource) (bool, error) {
	if ep.Database != nil {
		// are there any unrecognized files?
		for _, file := range files {
			if _, found := ep.Database.resources[file.Id]; !found {
				return false, fmt.Errorf("Unrecognized file: %s\n", file.Id)
			}
		}
		// the source endpoint should report true for the staged files as long
		// as the source database has had time to stage them
		for _, req := range ep.Database.Staging {
			if time.Now().Sub(req.Time) < ep.Options.StagingDuration {
				return false, nil
			}
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	xfers := make([]uuid.UUID, 0)
	for xferId := range ep.Xfers {
		xfers = append(xfers, xferId)
	}
	return xfers, nil
}

func (ep *Endpoint) Transfer(dst endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	xferId := uuid.New()
	ep.Xfers[xferId] = transferInfo{
		Time: time.Now(),
		Status: endpoints.TransferStatus{
			Code:                endpoints.TransferStatusActive,
			NumFiles:            len(files),
			NumFilesTransferred: 0,
		},
	}
	return xferId, nil
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	if info, found := ep.Xfers[id]; found {
		if info.Status.Code != endpoints.TransferStatusSucceeded &&
			time.Now().Sub(info.Time) >= ep.Options.TransferDuration { // update if needed
			info.Status.Code = endpoints.TransferStatusSucceeded
			ep.Xfers[id] = info
		}
		return info.Status, nil
	}
	return endpoints.TransferStatus{}, fmt.Errorf("Invalid transfer ID: %s", id.String())
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	return nil
}

//------------------------
// Database Test Fixtures
//------------------------

type stagingRequest struct {
	FileIds []string
	Time    time.Time
}

// This type implements a databases.Database test fixture
type Database struct {
	Endpt     endpoints.Endpoint
	resources map[string]frictionless.DataResource
	Staging   map[uuid.UUID]stagingRequest
}

// Registers a database test fixture with the given name in the configuration.
func RegisterDatabase(databaseName string, resources map[string]frictionless.DataResource) error {
	slog.Debug(fmt.Sprintf("Registering test database %s...", databaseName))
	newDatabaseFunc := func() (databases.Database, error) {
		endpoint, err := endpoints.NewEndpoint(config.Databases[databaseName].Endpoint)
		if err != nil {
			return nil, err
		}
		db := Database{
			Endpt:     endpoint,
			resources: resources,
			Staging:   make(map[uuid.UUID]stagingRequest),
		}
		if testEndpoint, isTestEndpoint := db.Endpt.(*Endpoint); isTestEndpoint {
			testEndpoint.Database = &db
		}
		return &db, nil
	}
	return databases.RegisterDatabase(databaseName, newDatabaseFunc)
}

func (db Database) SpecificSearchParameters() map[string]interface{} {
	return map[string]interface{}{
		"happy": false, // can also be true--single value indicates all values valid
		"day":   []string{"sunday", "monday"},
	}
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
	// look for file IDs in the search query
	results := databases.SearchResults{
		Resources: make([]frictionless.DataResource, 0),
	}
	for fileId, resource := range db.resources {
		if strings.Contains(params.Query, fileId) {
			results.Resources = append(results.Resources, resource)
		}
	}
	return results, nil
}

func (db *Database) Resources(orcid string, fileIds []string) ([]frictionless.DataResource, error) {
	resources := make([]frictionless.DataResource, 0)
	for _, fileId := range fileIds {
		if resource, found := db.resources[fileId]; found {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	id := uuid.New()
	db.Staging[id] = stagingRequest{
		FileIds: fileIds,
		Time:    time.Now(),
	}
	return id, nil
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	if info, found := db.Staging[id]; found {
		endpoint := db.Endpt.(*Endpoint)
		if time.Now().Sub(info.Time) >= endpoint.Options.StagingDuration { // FIXME: not always so!
			return databases.StagingStatusSucceeded, nil
		}
		return databases.StagingStatusActive, nil
	}
	return databases.StagingStatusUnknown, nil
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) Endpoint() (endpoints.Endpoint, error) {
	return db.Endpt, nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	return "testuser", nil
}

func (db *Database) Save() (databases.DatabaseSaveState, error) {
	return databases.DatabaseSaveState{}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	return nil
}
