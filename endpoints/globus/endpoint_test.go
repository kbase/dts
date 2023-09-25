package globus

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"dts/config"
	"dts/core"
)

const globusConfig string = `
endpoints:
  globus-jdp:
    name: NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// this function gets called at the begіnning of a test session
func setup() {
	config.Init([]byte(globusConfig))
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestGlobusConstructor(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	endpoint, err := NewEndpoint("globus-jdp")
	assert.NotNil(endpoint)
	assert.Nil(err)
}

func TestGlobusTransfers(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	endpoint, _ := NewEndpoint("globus-jdp")
	// this is just a smoke test--we don't check the contents of the result
	xfers, err := endpoint.Transfers()
	assert.NotNil(xfers) // empty or non-empty slice
	assert.Nil(err)
}

func TestGlobusFilesStaged(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	endpoint, _ := NewEndpoint("globus-jdp")

	// provide an empty slice of filenames, which should return true
	staged, err := endpoint.FilesStaged([]core.DataResource{})
	assert.True(staged)
	assert.Nil(err)

	// provide a (probably) nonexistent file, which should return false
	resources := []core.DataResource{
		core.DataResource{
			Id:   "yadda",
			Path: "yaddayadda/yadda/yaddayadda/yaddayaddayadda.xml",
		},
	}
	staged, err = endpoint.FilesStaged(resources)
	assert.False(staged)
	assert.Nil(err)
}

func TestGlobusStatus(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	endpoint, _ := NewEndpoint("globus-jdp")

	// make up a transfer UUID and check its status
	taskId := uuid.New()
	_, err := endpoint.Status(taskId)
	assert.NotNil(err)
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
