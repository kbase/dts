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
	"os"
	"testing"

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

// helper function replaces embedded environment variables in yaml strings
// when they don't exist in the environment
func setTestEnvVars(yaml string) string {
	testVars := map[string]string{
		"DTS_GLOBUS_TEST_ENDPOINT": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"DTS_GLOBUS_CLIENT_ID":     "fake_client_id",
		"DTS_GLOBUS_CLIENT_SECRET": "fake_client_secret",
	}

	// check for existence of each variable. when not present, replace
	// instances of it in the yaml string with a test value
	for key, value := range testVars {
		if os.Getenv(key) == "" {
			yaml = os.Expand(yaml, func(yamlVar string) string {
				if yamlVar == key {
					return value
				}
				return "${" + yamlVar + "}"
			})
		}
	}

	return yaml
}

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
	// omits the endpoint ID so it is initialized to the zero value
	yaml := VALID_SERVICE + `` + VALID_DATABASES + `
endpoints:
  my-globus-endpoint:
    name: Globus test endpoint
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  bad_endpoint:
    name: Bad endpoint
    id: 00000000-0000-0000-0000-000000000000
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with bad endpoint ID didn't trigger an error.")
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
	yaml = setTestEnvVars(yaml)
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

// tests whether config.Init rejects a configuration with no databases
func TestInitRejectsNoDatabases(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + `
databases: {}
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with no databases didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with a database that is
// missing both endpoint and endpoints
func TestInitRejectsDatabaseMissingEndpoint(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + `
databases:
  bad_database:
    name: Bad Database
    endpoint: ""
    endpoints: {}
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with database missing endpoint didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with a database that has
// both endpoint and endpoints defined
func TestInitRejectsDatabaseWithBothEndpointAndEndpoints(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + `
databases:
  bad_database:
    name: Bad Database
    endpoint: my-globus-endpoint
    endpoints:
      my-globus-endpoint: "my-globus-endpoint"
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with database with both endpoint and endpoints didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with a database that has
// an endpoints entry that is not present in the endpoints section
func TestInitRejectsDatabaseWithInvalidEndpointEntry(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + `
databases:
  bad_database:
    name: Bad Database
    endpoint: endpoint-that-does-not-exist
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with database with invalid endpoint didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with a database that has
// an endpoints entry that is not present in the endpoints section
func TestInitRejectsDatabaseWithInvalidFunctionalEndpointsEntry(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + `
databases:
  bad_database:
    name: Bad Database
    endpoints:
      globus: "my-globus-endpoint"
      bad-endpoint: "endpoint-that-does-not-exist"
`
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with database with invalid endpoint didn't trigger an error.")
}

// Tests whether config.Init returns no error for a configuration that is
// (ostensibly) valid. NOTE: This particular configuration is consistent and
// contains acceptible values for fields. It won't actually run a service!
func TestInitAcceptsValidInput(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	yaml = setTestEnvVars(yaml)
	b := []byte(yaml)
	err := Init(b)
	assert.Nil(t, err, fmt.Sprintf("Valid YAML input produced an error: %s", err))
}

// Tests whether config.Init properly initializes its globals for valid input.
func TestInitProperlySetsGlobals(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS + VALID_DATABASES
	yaml = setTestEnvVars(yaml)
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
