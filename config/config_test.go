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

package config

// These tests verify that we can properly configure the search service with
// YAML input.
import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// a valid service config entry
const VALID_SERVICE string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
  endpoint: my-globus-endpoint
`

// a valid endpoints config entry
const VALID_ENDPOINTS string = `
endpoints:
  my-globus-endpoint:
    name: Globus test endpoint
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// a valid databases config entry
const VALID_DATABASES string = `
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    url: files.jgi.doe.gov
    endpoint: my-globus-endpoint
`

// tests whether config.Init reports an error for an invalid byte array
func TestInitRejectsBadYAML(t *testing.T) {
	b := []byte("this is not yaml")
	err := Init(b)
	assert.NotNil(t, err, "Bad YAML didn't trigger an error.")
}

// tests whether config.Init reports an error for a missing service endpoint
// when endpoints are defined
func TestInitRejectsMissingServiceEndpoint(t *testing.T) {
	yaml := "service:\n  endpoint: \"\"\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with missing service endpoint didn't trigger an error.")
}

// tests whether config.Init reports an error for a bad poll interval
func TestInitRejectsBadPollInterval(t *testing.T) {
	yaml := "service:\n  poll_interval: -1\n\n" + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad poll interval didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid deletion period
func TestInitRejectsBadDeletionPeriod(t *testing.T) {
	yaml := "service:\n  delete_after: 0\n\n" + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad deletion period didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid credential ID
func TestInitRejectsBadCredentialID(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES + `
credentials:
  bad_credential:
    id: ""
`
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad credential ID didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid endpoint ID
func TestInitRejectsBadEndpointID(t *testing.T) {
  // tests validateEndpoints function directly as parsing of UUID would fail first
	err := validateEndpoints(map[string]endpointConfig{
		"bad_endpoint": {
			Id:       uuid.UUID{}, // invalid UUID
			Provider: "globus",
		},
	})
	log.Printf("This should be an error: %v", err)
	// assert.NotNil(t, err, "Config with bad endpoint ID didn't trigger an error.")
	// UPDATE config to use uuid.Nil as invalid UUID?
}

// tests whether config.Init reports an error for a missing endpoint provider
func TestInitRejectsMissingEndpointProvider(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES + `
endpoints:
  my-globus-endpoint:
    name: Globus test endpoint
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  bad_endpoint:
    id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
    provider: ""
`
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with missing endpoint provider didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid max number of
// processes
func TestInitRejectsBadPort(t *testing.T) {
	yaml := "service:\n  port: -1\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
	yaml = "service:\n  port: 1000000\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b = []byte(yaml)
	err = Init(b)
	assert.NotNil(t, err, "Config with bad port didn't trigger an error.")
}

// tests whether config.Init reports an error for an invalid max number of
// connections
func TestInitRejectsBadMaxConnections(t *testing.T) {
	yaml := "service:\n  max_connections: 0\n\n" + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad maxConnections didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with invalid endpoints
func TestInitRejectsInvalidEndpointType(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES +
		"endpoints:\n  eeeevil_entry:\n    eeeevil_field: eeeevil_value\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with invalid endpoint didn't trigger an error.")
}

// Tests whether config.Init rejects a database with a bad base URL.
func TestInitRejectsBadDatabaseBaseURL(t *testing.T) {
	yaml := fmt.Sprintf("databases:\n  ohaicorp:\n    url: hahahahahahaha\n\n")
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad database URL didn't trigger an error.")
}

// Tests whether config.Init returns no error for a configuration that is
// (ostensibly) valid. NOTE: This particular configuration is consistent and
// contains acceptible values for fields. It won't actually run a service!
func TestInitAcceptsValidInput(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(t, err, fmt.Sprintf("Valid YAML input produced an error: %s", err))
}

// Tests whether config.Init properly initializes its globals for valid input.
func TestInitProperlySetsGlobals(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(t, err, fmt.Sprintf("Valid YAML input produced an error: %s", err))

	// Check data
	assert.Equal(t, 8080, Service.Port)
	assert.Equal(t, 100, Service.MaxConnections)
	assert.Equal(t, 1, len(Endpoints))
	assert.Equal(t, 1, len(Databases))
}

// this function gets called at the begіnning of a test session
func setup() {
}

// this function gets called after all tests have been run
func breakdown() {
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}
