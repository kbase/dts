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
	"encoding/json"
	"fmt"
	"os"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var mockKBaseServer *httptest.Server

// create a mock KBase Auth Server for testing without hitting the real server
func createMockKBaseServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// get the auth header
		authHeader := r.Header.Get("Authorization")
		
		switch {
		case r.URL.Path == "/services/auth/api/V2/me" && r.Method == "GET":
			handleMeEndpoint(w, r, authHeader)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"httpcode": 404, "error": "Not Found", "message": "Unknown endpoint"}`)
		}
	}))
}

// mock response for the /me endpoint
func handleMeEndpoint(w http.ResponseWriter, r *http.Request, authToken string) {
	w.Header().Set("Content-Type", "application/json")

	switch authToken {
	case "valid_token":
		// Return valid user data
		user := kbaseUser{
			Username: "testuser",
			Display: "Test User",
			Email:    "test@email.com",
			Idents: []struct {
				Provider string `json:"provider"`
				UserName string `json:"provusername"`
			}{
				{Provider: "OrcID", UserName: "testuser"},
			},
		}
		json.NewEncoder(w).Encode(user)
	default:
		// Invalid token
		w.WriteHeader(http.StatusUnauthorized)
		errorResp := kbaseAuthErrorResponse{
			HttpCode:  http.StatusUnauthorized,
			HttpStatus: 401,
			AppCode:   0,
			AppError:  "Unauthorized",
			Message:   "Invalid token",
			CallId:    0,
			Time:      0,
		}
		json.NewEncoder(w).Encode(errorResp)
	}
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
