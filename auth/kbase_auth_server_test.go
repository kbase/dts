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

// These tests verify that we can connect to the KBase authentication server
// and access a user's ORCID credential(s). The tests require the following
// environment variables to be set:
//
// * DTS_KBASE_DEV_TOKEN: a valid unencoded KBase developer token
// * DTS_KBASE_TEST_ORCID: a valid ORCID identifier for a KBase user
package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humamux"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

var mockKBaseServer *httptest.Server

func setupKBaseAuthServerTests() {
	// spin up a mock KBase server for testing
	mockKBaseServer = createMockKBaseServer()
}

func breakdownKBaseAuthServerTests() {
	if mockKBaseServer != nil {
		mockKBaseServer.Close()
	}
}

// Response structure for mock KBase Auth Server
type KBaseUserResponse struct {
	Body kbaseUser `json:"body"`
}

// create a mock KBase Auth Server for testing without hitting the real server
func createMockKBaseServer() *httptest.Server {
	router := mux.NewRouter()
	api := humamux.New(router, huma.DefaultConfig("Mock KBase Auth Server", "2.0.0"))

	// me endpoint
	huma.Register(api, huma.Operation{
		OperationID: "getMe",
		Method:      http.MethodGet,
		Path:        "/services/auth/api/V2/me",
		Security:    []map[string][]string{{"bearerAuth": {}}},
	}, func(ctx context.Context, input *struct {
		Authorization string `header:"Authorization"`
	}) (*KBaseUserResponse, error) {

		switch input.Authorization {
		case "valid_token":
			// Return valid user data
			return &KBaseUserResponse{
				Body: kbaseUser{
					Username: "testuser",
					Display:  "Test User",
					Email:    "test@email.com",
					Idents: []struct {
						Provider string `json:"provider"`
						UserName string `json:"provusername"`
					}{
						{Provider: "OrcID", UserName: "testuser"},
					},
				},
			}, nil
		case "no_idents_token":
			// Return user data with no identifiers
			return &KBaseUserResponse{
				Body: kbaseUser{
					Username: "noidentuser",
					Display:  "No Ident User",
					Email:    "noident@email.com",
				},
			}, nil
		case "no_orcid_token":
			// Return user data with identifiers but no OrcID
			return &KBaseUserResponse{
				Body: kbaseUser{
					Username: "noorciduser",
					Display:  "No OrcID User",
					Email:    "noorcid@email.com",
					Idents: []struct {
						Provider string `json:"provider"`
						UserName string `json:"provusername"`
					}{
						{Provider: "Google", UserName: "noorciduser"},
					},
				},
			}, nil
		default:
			// Invalid token
			return nil, huma.NewError(http.StatusUnauthorized, "Unauthorized")
		}
	})

	return httptest.NewServer(router)
}

// tests whether a proxy for the KBase authentication server can be
// constructed
func TestNewKBaseAuthServer(t *testing.T) {
	assert := assert.New(t)

	// this test requires a valid developer token
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	if len(devToken) > 0 {
		server, err := NewKBaseAuthServer(devToken)
		assert.NotNil(server, "Authentication server not created")
		assert.Nil(err, "Authentication server constructor triggered an error")
	}

	// test with the mock server
	server, err := NewKBaseAuthServer("valid_token",
		func(cfg *KBaseAuthServerConfig) {
			cfg.BaseURL = mockKBaseServer.URL
		})
	assert.NotNil(server, "Authentication server not created with valid token")
	assert.Nil(err, "Authentication server constructor triggered an error with valid token")
}

// tests whether an invalid KBase token prevents a proxy for the auth server
// from being constructed
func TestInvalidToken(t *testing.T) {
	assert := assert.New(t)

	// test against the real server
	devToken := "INVALID_KBASE_TOKEN"
	server, err := NewKBaseAuthServer(devToken)
	assert.Nil(server, "Authentication server created with invalid token")
	assert.NotNil(err, "Invalid token for authentication server triggered no error")

	// test with the mock server
	server, err = NewKBaseAuthServer("invalid_token",
		func(cfg *KBaseAuthServerConfig) {
			cfg.BaseURL = mockKBaseServer.URL
		})
	assert.Nil(server, "Authentication server created with invalid token")
	assert.NotNil(err, "Invalid token for authentication server triggered no error")
}

// tests that the proxy handles missing identifiers correctly
func TestNoIdentifiers(t *testing.T) {
	assert := assert.New(t)

	// test with the mock server
	server, err := NewKBaseAuthServer("no_idents_token",
		func(cfg *KBaseAuthServerConfig) {
			cfg.BaseURL = mockKBaseServer.URL
		})
	assert.Nil(server, "Authentication server created for access token with no identifiers")
	assert.NotNil(err, "Access token with no identifiers for authentication server triggered no error")
}

// tests that the proxy handles missing OrcID identifiers correctly
func TestNoOrcID(t *testing.T) {
	assert := assert.New(t)

	// test with the mock server
	server, err := NewKBaseAuthServer("no_orcid_token",
		func(cfg *KBaseAuthServerConfig) {
			cfg.BaseURL = mockKBaseServer.URL
		})
	assert.Nil(server, "Authentication server created for access token with no ORCID")
	assert.NotNil(err, "Access token with no ORCID for authentication server triggered no error")
}

// tests whether the authentication server can return information for the
// client (the user associated with the specified developer token)
func TestClient(t *testing.T) {
	assert := assert.New(t)

	// this test requires a valid developer token with an associated ORCID
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	if len(devToken) > 0 {
		server, _ := NewKBaseAuthServer(devToken)
		assert.NotNil(server)
		client, err := server.Client()
		assert.Nil(err)

		assert.True(len(client.Username) > 0)
		assert.True(len(client.Email) > 0)
		assert.Equal(os.Getenv("DTS_KBASE_TEST_ORCID"), client.Orcid)
	}

	// test with the mock server
	server, _ := NewKBaseAuthServer("valid_token",
		func(cfg *KBaseAuthServerConfig) {
			cfg.BaseURL = mockKBaseServer.URL
		})
	assert.NotNil(server, "Authentication server not created with valid token")
	client, err := server.Client()
	assert.Nil(err, "Client() triggered an error with valid token")

	assert.Equal("testuser", client.Username)
	assert.Equal("Test User", client.Name)
	assert.Equal("test@email.com", client.Email)
	assert.Equal("testuser", client.Orcid)
}
