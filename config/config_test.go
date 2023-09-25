package config

// These tests verify that we can properly configure the search service with
// YAML input.
import (
	"fmt"
	"os"

	"github.com/stretchr/testify/assert"
	"testing"
)

// a valid service config entry
const VALID_SERVICE string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
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
    endpoint: globus-jdp # local file transfer endpoint
`

// tests whether config.Init reports an error for blank input
func TestInitRejectsBlankInput(t *testing.T) {
	b := []byte("")
	err := Init(b)
	assert.NotNil(t, err, "Blank config didn't trigger an error.")
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

// tests whether config.Init rejects a configuration with no endpoints
func TestInitRejectsNoEndpointsDefined(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with no endpoints didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with invalid endpoints
func TestInitRejectsInvalidEndpointType(t *testing.T) {
	yaml := VALID_SERVICE + VALID_DATABASES +
		"endpoints:\n  eeeevil_entry:\n    eeeevil_field: eeeevil_value\n\n"
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with invalid endpoint didn't trigger an error.")
}

// tests whether config.Init rejects a configuration with no databases
func TestInitRejectsNoDatabasesDefined(t *testing.T) {
	yaml := VALID_SERVICE + VALID_ENDPOINTS
	b := []byte(yaml)
	err := Init(b)
	assert.NotNil(t, err, "Config with no databases didn't trigger an error.")
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
