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

package tasks

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/dtstest"
)

// runs all tests serially
func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	tester.TestStartAndStop()
	tester.TestCreateTask()
	tester.TestCancelTask()
	tester.TestStopAndRestart()
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
	myConfig := strings.ReplaceAll(tasksConfig, "TESTING_DIR", TESTING_DIR)
	err = config.Init([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't initialize configuration: %s", err)
	}

	// create test resources
	file1, err := datapackage.NewResource(
		map[string]interface{}{ // descriptor
			"id":       "file1",
			"name":     "file1.dat",
			"path":     "dir1/file1.dat",
			"format":   "text",
			"bytes":    1024,
			"hash":     "d91f97974d06563cab48d4d43a17e08a",
			"endpoint": "source-endpoint",
		}, validator.MustInMemoryRegistry())
	if err != nil {
		log.Panicf("Couldn't initialize file1: %s", err)
	}
	file2, err := datapackage.NewResource(
		map[string]interface{}{
			"id":       "file2",
			"name":     "file2.dat",
			"path":     "dir2/file2.dat",
			"format":   "text",
			"bytes":    2048,
			"hash":     "d91f9e974d0e563cab48d4d43a17e08a",
			"endpoint": "source-endpoint",
		}, validator.MustInMemoryRegistry())
	file3, err := datapackage.NewResource(
		map[string]interface{}{
			"id":       "file3",
			"name":     "file3.dat",
			"path":     "dir3/file3.dat",
			"format":   "text",
			"bytes":    4096,
			"hash":     "e91f9e974d0e563cab48d4d43a17e08e",
			"endpoint": "source-endpoint",
		}, validator.MustInMemoryRegistry())
	testResources = map[string]*datapackage.Resource{
		"file1": file1,
		"file2": file2,
		"file3": file3,
	}

	// register test databases/endpoints referred to in config file
	dtstest.RegisterTestFixturesFromConfig(endpointOptions, testResources)

	// Create the data and manifest directories
	os.Mkdir(config.Service.DataDirectory, 0755)
	os.Mkdir(config.Service.ManifestDirectory, 0755)
}

// this function gets called after all tests have been run
func breakdown() {
	if TESTING_DIR != "" {
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// To run the tests serially, we attach them to a SerialTests type and
// have them run by a a single test runner.
type SerialTests struct{ Test *testing.T }

func (t *SerialTests) TestStartAndStop() {
	assert := assert.New(t.Test)

	assert.False(Running())
	err := Start()
	assert.Nil(err)
	assert.True(Running())
	err = Stop()
	assert.Nil(err)
	assert.False(Running())
}

func (t *SerialTests) TestCreateTask() {
	assert := assert.New(t.Test)

	err := Start()
	assert.Nil(err)

	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second

	// queue up a transfer task between two phony databases
	taskId, err := Create(Specification{
		User: auth.User{
			Name:  "Joe-bob",
			Orcid: "1234-5678-9012-3456",
		},
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2"},
	})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})

	// the initial status of the task should be Unknown
	status, err := Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusUnknown, status.Code)

	// make sure the status switches to staging or active
	time.Sleep(pause + pollInterval)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.True(status.Code == TransferStatusStaging || status.Code == TransferStatusActive)

	// wait for the staging to complete and then check its status
	// again (should be actively transferring)
	time.Sleep(pause + endpointOptions.StagingDuration)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.Equal(TransferStatusActive, status.Code)

	// wait again for the transfer to complete and then check its status
	// (should be finalizing or have successfully completed)
	time.Sleep(pause + endpointOptions.TransferDuration)
	status, err = Status(taskId)
	assert.Nil(err)
	assert.True(status.Code == TransferStatusFinalizing || status.Code == TransferStatusSucceeded)

	// if the transfer was finalizing, check once more for completion
	if status.Code != TransferStatusSucceeded {
		time.Sleep(pause + endpointOptions.TransferDuration)
		status, err = Status(taskId)
		assert.Nil(err)
		assert.Equal(TransferStatusSucceeded, status.Code)
	}

	// now wait for the task to age out and make sure it's not found
	time.Sleep(pause + deleteAfter)
	status, err = Status(taskId)
	assert.NotNil(err)

	err = Stop()
	assert.Nil(err)
}

func (t *SerialTests) TestCancelTask() {
	assert := assert.New(t.Test)

	err := Start()
	assert.Nil(err)

	pollInterval := time.Duration(config.Service.PollInterval) * time.Millisecond

	// queue up a transfer task between two phony databases
	taskId, err := Create(Specification{
		User: auth.User{
			Name:  "Joe-bob",
			Orcid: "1234-5678-9012-3456",
		},
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2"},
	})
	assert.Nil(err)
	assert.True(taskId != uuid.UUID{})

	// get things going and make sure we can check its status
	time.Sleep(pause + pollInterval)
	_, err = Status(taskId)
	assert.Nil(err)

	// cancel the thing
	err = Cancel(taskId)
	assert.Nil(err)

	// wait for the task to complete
	status, err := Status(taskId)
	for {
		if status.Code == TransferStatusSucceeded ||
			status.Code == TransferStatusFailed {
			break
		}
		time.Sleep(pause + pollInterval)
		status, err = Status(taskId)
		assert.Nil(err)
	}

	err = Stop()
	assert.Nil(err)
}

func (t *SerialTests) TestStopAndRestart() {
	assert := assert.New(t.Test)

	// start up, add a bunch of tasks, then immediately close
	err := Start()
	assert.Nil(err)
	numTasks := 10
	taskIds := make([]uuid.UUID, numTasks)
	for i := 0; i < numTasks; i++ {
		taskId, _ := Create(Specification{
			User: auth.User{
				Name:  "Joe-bob",
				Orcid: "1234-5678-9012-3456",
			},
			Source:      "test-source",
			Destination: "test-destination",
			FileIds:     []string{"file1", "file2"},
		})
		taskIds[i] = taskId
	}
	time.Sleep(100 * time.Millisecond) // let things settle
	err = Stop()
	assert.Nil(err)

	// now restart the task manager and make sure all the tasks are there
	err = Start()
	assert.Nil(err)
	for i := 0; i < numTasks; i++ {
		_, err := Status(taskIds[i])
		assert.Nil(err)
	}

	err = Stop()
	assert.Nil(err)
}

// temporary testing directory
var TESTING_DIR string

// a directory in which the task manager can read/write files
var dataDirectory string

// endpoint testing options
var endpointOptions = dtstest.EndpointOptions{
	StagingDuration:  time.Duration(150) * time.Millisecond,
	TransferDuration: time.Duration(500) * time.Millisecond,
}

// a pause to give the task manager a bit of time
var pause time.Duration = time.Duration(25) * time.Millisecond

// configuration
const tasksConfig string = `
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

// file test metadata
var testResources map[string]*datapackage.Resource
