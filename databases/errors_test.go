package databases

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNotFoundError(t *testing.T) {
	err := NotFoundError{Database: "testdb"}
	assert.Equal(t, "The database 'testdb' was not found", err.Error())
}

func TestAlreadyRegisteredError(t *testing.T) {
	err := AlreadyRegisteredError{Database: "testdb"}
	assert.Equal(t, "Cannot register database 'testdb': already registered", err.Error())
}

func TestUnauthorizedError(t *testing.T) {
	errWithUser := UnauthorizedError{
		Database: "testdb",
		Message:  "access denied",
		User:     "jdoe",
	}
	assert.Equal(t, "Unable to authorize user 'jdoe' for database 'testdb': access denied", errWithUser.Error())

	errWithoutUser := UnauthorizedError{
		Database: "testdb",
		Message:  "access denied",
	}
	assert.Equal(t, "Unable to authorize user for database 'testdb': access denied", errWithoutUser.Error())
}

func TestUnavailableError(t *testing.T) {
	err := UnavailableError{Database: "testdb"}
	assert.Equal(t, "Cannot reach database 'testdb': unavailable", err.Error())
}

func TestInvalidSearchParameter(t *testing.T) {
	err := InvalidSearchParameter{
		Database: "testdb",
		Message:  "invalid parameter 'foo'",
	}
	assert.Equal(t, "Invalid search parameter for database 'testdb': invalid parameter 'foo'", err.Error())
}

func TestInvalidEndpointsError(t *testing.T) {
	err := InvalidEndpointsError{
		Database: "testdb",
		Message:  "no endpoints provided",
	}
	assert.Equal(t, "Invalid endpoint configuration for database 'testdb': no endpoints provided", err.Error())
}

func TestInvalidResourceEndpointError(t *testing.T) {
	err := InvalidResourceEndpointError{
		Database:   "testdb",
		ResourceId: "resource1",
		Endpoint:   "http://invalid-url",
	}
	assert.Equal(t, "Invalid endpoint specified for resource 'resource1' in database 'testdb': http://invalid-url", err.Error())
}

func TestMissingOrcidError(t *testing.T) {
	err := MissingOrcidError{Database: "testdb"}
	assert.Equal(t, "Missing user ORCID for request to database 'testdb'", err.Error())
}

func TestPermissionDeniedError(t *testing.T) {
	err := PermissionDeniedError{
		Database:   "testdb",
		ResourceId: "some-resource",
	}
	assert.Equal(t, "Can't access resource 'some-resource' in database 'testdb': permission denied", err.Error())
}

func TestResourcesNotFoundError(t *testing.T) {
	err := ResourcesNotFoundError{
		Database:    "testdb",
		ResourceIds: []string{"res1", "res2"},
	}
	assert.Equal(t, "The following resources in database 'testdb' were not found: res1, res2", err.Error())
}

func TestResourceEndpointNotFoundError(t *testing.T) {
	err := ResourceEndpointNotFoundError{
		Database:   "testdb",
		ResourceId: "res1",
	}
	assert.Equal(t, "Can't determine endpoint for resource 'res1' in database 'testdb'", err.Error())
}

func TestDowngradedRedirectError(t *testing.T) {
	err := DowngradedRedirectError{
		Endpoint: "https://secure-endpoint",
	}
	assert.Equal(t, "The endpoint https://secure-endpoint is attempting to downgrade an HTTPS request to HTTP", err.Error())
}
