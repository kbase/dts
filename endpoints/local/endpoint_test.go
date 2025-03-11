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

package local

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/endpoints"
)

var tempRoot string
var sourceRoot string
var destinationRoot string
var destinationRootCancel string

// source database files by ID
var sourceFilesById = map[string]string{
	"1": "file1.txt",
	"2": "file2.txt",
	"3": "file3.txt",
}

const localConfig string = `
endpoints:
  source:
    name: Source Endpoint
    id: 2ee69538-10d5-4d1e-a890-1127b5e42003
    provider: local
    root: SOURCE_ROOT
  destination:
    name: Destination Endpoint
    id: b925d96e-7e39-473b-a658-714f8c243b1c
    provider: local
    root: DESTINATION_ROOT
  destination-cancel:
    name: Destination Endpoint for cancellation
    id: b925d96e-7e39-473b-a658-714f8c243b1c
    provider: local
    root: DESTINATION_CANCEL
`

// this function gets called at the begіnning of a test session
func setup() {
	// create source/destination directories
	var err error
	tempRoot, err = os.MkdirTemp(os.TempDir(), "dts-local-endpoints")
	if err != nil {
		panic(err)
	}
	sourceRoot = filepath.Join(tempRoot, "source")
	err = os.Mkdir(sourceRoot, 0700)
	if err != nil {
		panic(err)
	}
	destinationRoot = filepath.Join(tempRoot, "destination")
	err = os.Mkdir(destinationRoot, 0700)
	if err != nil {
		panic(err)
	}
	destinationRootCancel = filepath.Join(tempRoot, "destination-cancel")
	err = os.Mkdir(destinationRootCancel, 0700)
	if err != nil {
		panic(err)
	}
	// create source files
	for i := 1; i <= 3; i++ {
		err = os.WriteFile(filepath.Join(sourceRoot, fmt.Sprintf("file%d.txt", i)),
			[]byte(fmt.Sprintf("This is the content of file %d.", i)), 0600)
		if err != nil {
			panic(err)
		}
	}

	// read in the config file with variable paths substituted
	myConfig := strings.ReplaceAll(localConfig, "SOURCE_ROOT", sourceRoot)
	myConfig = strings.ReplaceAll(myConfig, "DESTINATION_ROOT", destinationRoot)
	myConfig = strings.ReplaceAll(myConfig, "DESTINATION_CANCEL", destinationRootCancel)
	fmt.Printf(myConfig)
	err = config.Init([]byte(myConfig))
	if err != nil {
		panic(err)
	}
}

// this function gets called after all tests have been run
func breakdown() {
	os.RemoveAll(tempRoot)
}

func TestLocalConstructor(t *testing.T) {
	assert := assert.New(t)

	endpoint, err := NewEndpoint("source")
	assert.NotNil(endpoint)
	assert.Nil(err)
}

func TestBadLocalConstructor(t *testing.T) {
	assert := assert.New(t)

	endpoint, err := NewEndpoint("nonexistent-endpoint")
	assert.Nil(endpoint)
	assert.NotNil(err)
}

func TestLocalTransfers(t *testing.T) {
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

	// provide files that are known to be on the source endpoint
	resources := make([]*datapackage.Resource, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		res, err := datapackage.NewResource(
			map[string]interface{}{ // descriptor
				"id":   id,
				"path": sourceFilesById[id],
			},
			validator.MustInMemoryRegistry())
		assert.Nil(err)
		resources = append(resources, res)
	}
	staged, err = endpoint.FilesStaged(resources)
	assert.True(staged)
	assert.Nil(err)

	// provide a nonexistent file, which should return false
	nonexistent, _ := datapackage.NewResource(
		map[string]interface{}{ // descriptor
			"id":   "yadda",
			"path": "yaddayadda/yadda/yaddayadda/yaddayaddayadda.xml",
		},
		validator.MustInMemoryRegistry())
	resources = []*datapackage.Resource{nonexistent}
	staged, err = endpoint.FilesStaged(resources)
	assert.False(staged)
	assert.Nil(err)
}

func TestLocalTransfer(t *testing.T) {
	assert := assert.New(t)

	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	fileXfers := make([]endpoints.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)

		fileXfers = append(fileXfers, endpoints.FileTransfer{
			SourcePath:      sourceFilesById[id],
			DestinationPath: sourceFilesById[id],
		})
	}
	_, err := source.Transfer(destination, fileXfers)
	assert.Nil(err)
}

func TestBadLocalTransfer(t *testing.T) {
	assert := assert.New(t)
	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination")

	// ask for some nonexistent files
	fileXfers := make([]endpoints.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		fileXfers = append(fileXfers, endpoints.FileTransfer{
			SourcePath:      sourceFilesById[id] + "_with_bad_suffix",
			DestinationPath: sourceFilesById[id] + "_with_bad_suffix",
		})
	}
	_, err := source.Transfer(destination, fileXfers)
	assert.NotNil(err)
}

func TestUnknownLocalStatus(t *testing.T) {
	assert := assert.New(t)
	endpoint, _ := NewEndpoint("source")

	// make up a bogus transfer UUID and check its status
	taskId := uuid.New()
	status, err := endpoint.Status(taskId)
	assert.Equal(endpoints.TransferStatusUnknown, status.Code)
	assert.NotNil(err)
}

func TestLocalTransferCancellation(t *testing.T) {
	assert := assert.New(t)

	source, _ := NewEndpoint("source")
	destination, _ := NewEndpoint("destination-cancel")

	fileXfers := make([]endpoints.FileTransfer, 0)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)

		fileXfers = append(fileXfers, endpoints.FileTransfer{
			SourcePath:      sourceFilesById[id],
			DestinationPath: sourceFilesById[id],
		})
	}
	id, err := source.Transfer(destination, fileXfers)
	assert.Nil(err)
	err = source.Cancel(id)
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
