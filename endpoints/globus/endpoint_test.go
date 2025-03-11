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

package globus

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
)

// we test our Globus endpoint implementation using two endpoints:
// * Source: A read-only source endpoint provided by Globus for ESnet customers
//   (https://fasterdata.es.net/performance-testing/DTNs/)
// * Destination: A test endpoint specified by UUID via the environment variable
//   DTS_GLOBUS_TEST_ENDPOINT

const (
	sourceEndpointName = "ESnet Sunnyvalue DTN (Anonymous read-only testing)"
	sourceEndpointId   = "8409a10b-de09-4670-a886-2c0b33f0fe25"
)

// source database files by ID (on above read-only source endpoint)
var sourceFilesById = map[string]string{
	"1": "5MB-in-tiny-files/a/a/a-a-1KB.dat",
	"2": "5MB-in-tiny-files/b/b/b-b-1KB.dat",
	"3": "5MB-in-tiny-files/c/c/c-c-1KB.dat",
}

var globusConfig string = fmt.Sprintf(`
endpoints:
  source:
    name: %s
    id: %s
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  destination:
    name: DTS Globus Test Endpoint
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  not-globus-jdp:
    name: lalala
    id: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
    provider: not-globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`, sourceEndpointName, sourceEndpointId)

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()

	if _, ok := os.LookupEnv("DTS_GLOBUS_TEST_ENDPOINT"); !ok {
		print("DTS_GLOBUS_TEST_ENDPOINT environment variable not set. Skipping Globus unit tests.\n")
		os.Exit(0)
	}
	config.Init([]byte(globusConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestGlobusConstructor(t *testing.T) {
	assert := assert.New(t)

	endpoint, err := NewEndpoint("source")
	assert.NotNil(endpoint)
	assert.Nil(err)
}

func TestBadGlobusConstructor(t *testing.T) {
	assert := assert.New(t)

	endpoint, err := NewEndpoint("not-globus-jdp")
	assert.Nil(endpoint)
	assert.NotNil(err)
}

func TestGlobusTransfers(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")
	// this is just a smoke test--we don't check the contents of the result
	xfers, err := endpoint.Transfers()
	assert.NotNil(xfers) // empty or non-empty slice
	assert.Nil(err)
}

func TestGlobusFilesStaged(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")

	// provide an empty slice of filenames, which should return true
	staged, err := endpoint.FilesStaged([]*datapackage.Resource{})
	assert.True(staged)
	assert.Nil(err)

	// provide a file that's known to be on the source endpoint, which
	// should return true
	resources := make([]*datapackage.Resource, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		res, err := datapackage.NewResource(
			map[string]interface{}{ // descriptor
				"id":   id,
				"path": sourceFilesById[id],
			}, validator.MustInMemoryRegistry())
		assert.Nil(err)
		resources = append(resources, res)
	}
	staged, err = endpoint.FilesStaged(resources)
	assert.True(staged)
	assert.Nil(err)

	// provide a nonexistent file, which should return false
	res, err := datapackage.NewResource(
		map[string]interface{}{ // descriptor
			"id":   "yadda",
			"path": "yaddayadda/yadda/yaddayadda/yaddayaddayadda.xml",
		}, validator.MustInMemoryRegistry())
	assert.Nil(err)
	resources = []*datapackage.Resource{res}
	staged, err = endpoint.FilesStaged(resources)
	assert.False(staged)
	assert.Nil(err)
}

// This function generates a unique name for a directory on the destination
// endpoint to receive files
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func destDirName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestGlobusTransfer(t *testing.T) {
	assert := assert.New(t)
	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	fileXfers := make([]endpoints.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)

		fileXfers = append(fileXfers, endpoints.FileTransfer{
			SourcePath:      sourceFilesById[id],
			DestinationPath: path.Join(destDirName(16), path.Base(sourceFilesById[id])),
		})
	}
	taskId, err := source.Transfer(destination, fileXfers)
	assert.Nil(err)

	// wait for the task to register in the system
	for {
		_, err = source.Status(taskId)
		if err == nil {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	assert.Nil(err)

	// now wait for it to complete
	var status endpoints.TransferStatus
	for {
		status, err = source.Status(taskId)
		assert.Nil(err)
		if status.Code == endpoints.TransferStatusSucceeded ||
			status.Code == endpoints.TransferStatusFailed {
			break
		} else { // not yet finished
			time.Sleep(1 * time.Second)
		}
	}
	assert.Equal(endpoints.TransferStatusSucceeded, status.Code)
}

func TestUnknownGlobusStatus(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")

	// make up a bogus transfer UUID and check its status
	taskId := uuid.New()
	status, err := endpoint.Status(taskId)
	assert.Equal(endpoints.TransferStatusUnknown, status.Code)
	assert.NotNil(err)
}

func TestGlobusTransferCancellation(t *testing.T) {
	assert := assert.New(t)
	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	fileXfers := make([]endpoints.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)

		fileXfers = append(fileXfers, endpoints.FileTransfer{
			SourcePath:      sourceFilesById[id],
			DestinationPath: path.Join(destDirName(16), path.Base(sourceFilesById[id])),
		})
	}
	taskId, err := source.Transfer(destination, fileXfers)
	assert.Nil(err)

	// wait for the task to show up
	for {
		_, err = source.Status(taskId)
		if err == nil {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	assert.Nil(err)

	err = source.Cancel(taskId)
	assert.Nil(err)
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
