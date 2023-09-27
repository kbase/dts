package endpoints

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	"dts/config"
)

const globusConfig string = `
endpoints:
  globus:
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

func TestNewGlobusEndpoint(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	ep, err := NewEndpoint("globus")
	assert.NotNil(ep, "Globus endpoint not created")
	assert.Nil(err, "Globus endpoint creation encountered an error")
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
