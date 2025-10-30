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

// These tests must be run serially, since tasks are coordinated by a
// single instance.

package transfers

import (
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	//"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/dtstest"
)

// this runner runs all tests for all the singletons in this package
func TestRunner(t *testing.T) {
	/*
		dispatcherTests := DispatcherTests{Test: t}
		print("dispatcher start/stop\n")
		dispatcherTests.TestStartAndStop()
	*/

	transfers := TransferTests{Test: t}
	transfers.TestStartAndStop()
	transfers.TestCreate()
}

// We attach the tests to this type, which runs them one by one.
type TransferTests struct{ Test *testing.T }

func (t *TransferTests) TestStartAndStop() {
	log.Print("=== TestStartAndStop ===")
	assert := assert.New(t.Test)

	assert.False(Running())
	err := Start(conf)
	assert.Nil(err)
	assert.True(Running())
	err = Stop()
	assert.Nil(err)
	assert.False(Running())
}

func (t *TransferTests) TestCreate() {
	log.Print("=== TestCreate ===")
	assert := assert.New(t.Test)

	// start clean -- remove any existing save file
	saveFilename := filepath.Join(conf.Service.DataDirectory, "dts.gob")
	os.Remove(saveFilename)

	// subscribe to the transfer mechanism's feed and collect messages describing the task's journey
	// through the aether
	var messages []Message
	subscription := Subscribe(32)
	go func() {
		var finished bool
		for !finished {
			messages = append(messages, <-subscription.Channel)
		}
	}()

	err := Start(conf)
	assert.Nil(err)
	assert.True(Running())

	transferId, err := Create(Specification{
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2", "file3"},
		Source:      "test-source",
	})
	assert.Nil(err)
	assert.NotEqual(uuid.UUID{}, transferId)

	status, err := Status(transferId)
	assert.Nil(err)
	assert.GreaterOrEqual(status.Code, TransferStatusUnknown)
	assert.Equal(3, status.NumFiles)

	// wait for the (local) transfers to complete
	time.Sleep(2 * time.Second)

	status, err = Status(transferId)
	assert.Nil(err)
	assert.Equal(status.Code, TransferStatusSucceeded)
	assert.Equal(3, status.NumFiles)

	err = Stop()
	assert.Nil(err)
	assert.False(Running())

	// make sure we hit all the desired statuses and none of the undesired (values not used)
	for _, occurred := range []TransferStatusCode{
		TransferStatusUnknown,
		TransferStatusStaging,
		TransferStatusActive,
		TransferStatusFinalizing,
		TransferStatusSucceeded,
	} {
		assert.True(slices.ContainsFunc(messages, func(message Message) bool {
			return message.TransferStatus.Code == occurred
		}))
	}
	for _, didntOccur := range []TransferStatusCode{TransferStatusFailed, TransferStatusInactive} {
		assert.False(slices.ContainsFunc(messages, func(message Message) bool {
			return message.TransferStatus.Code == didntOccur
		}))
	}

	// restart and check the status of the completed transfer
	err = Start(conf)
	assert.Nil(err)
	assert.True(Running())

	status, err = Status(transferId)
	assert.Nil(err)
	assert.Equal(status.Code, TransferStatusSucceeded)
	assert.Equal(3, status.NumFiles)

	// clean up
	err = Stop()
	assert.Nil(err)
	os.Remove(saveFilename)
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()

	log.Print("Creating testing directory...\n")
	var err error
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// read in the config file with SOURCE_ROOT and DESTINATION_ROOT replaced
	myConfig := strings.ReplaceAll(transfersConfig, "TESTING_DIR", TESTING_DIR)
	err = config.Init([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't initialize configuration: %s", err)
	}
	conf, err = config.NewConfig([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't create config instance: %s", err)
	}

	// register test databases/endpoints referred to in config file
	dtstest.RegisterTestFixturesFromConfig(endpointOptions, testDescriptors)

	// Create the data and manifest directories
	os.Mkdir(conf.Service.DataDirectory, 0755)
	os.Mkdir(conf.Service.ManifestDirectory, 0755)
}

// this function gets called after all tests have been run
func breakdown() {
	if TESTING_DIR != "" {
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// temporary testing directory
var TESTING_DIR string

// endpoint testing options
var endpointOptions = dtstest.EndpointOptions{
	StagingDuration:  time.Duration(150) * time.Millisecond,
	TransferDuration: time.Duration(500) * time.Millisecond,
}

// configuration
const transfersConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 50  # milliseconds
  data_dir: TESTING_DIR/data
  manifest_dir: TESTING_DIR/manifests
  delete_after: 2    # seconds
  endpoint: local-endpoint
databases:
  test-source:
    name: Source Test Database
    organization: The Source Company
    endpoint: source-endpoint
  test-destination:
    name: Destination Test Database
    organization: Fabulous Destinations, Inc.
    endpoint: destination-endpoint
endpoints:
  local-endpoint:
    name: Local endpoint
    id: 8816ec2d-4a48-4ded-b68a-5ab46a4417b6
    provider: test
  source-endpoint:
    name: Endpoint 1
    id: 26d61236-39f6-4742-a374-8ec709347f2f
    provider: test
    root: SOURCE_ROOT
  destination-endpoint:
    name: Endpoint 2
    id: f1865b86-2c64-4b8b-99f3-5aaa945ec3d9
    provider: test
    root: DESTINATION_ROOT
`

var testDescriptors map[string]map[string]any = map[string]map[string]any{
	"file1": {
		"id":       "file1",
		"name":     "file1.dat",
		"path":     "dir1/file1.dat",
		"format":   "text",
		"bytes":    1024,
		"hash":     "d91f97974d06563cab48d4d43a17e08a",
		"endpoint": "source-endpoint",
	},
	"file2": {
		"id":       "file2",
		"name":     "file2.dat",
		"path":     "dir2/file2.dat",
		"format":   "text",
		"bytes":    2048,
		"hash":     "d91f9e974d0e563cab48d4d43a17e08a",
		"endpoint": "source-endpoint",
	},
	"file3": {
		"id":       "file3",
		"name":     "file3.dat",
		"path":     "dir3/file3.dat",
		"format":   "text",
		"bytes":    4096,
		"hash":     "e91f9e974d0e563cab48d4d43a17e08e",
		"endpoint": "source-endpoint",
	},
}

// configuration instance
var conf config.Config
