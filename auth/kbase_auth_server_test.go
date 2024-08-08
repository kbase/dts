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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// tests whether a proxy for the KBase authentication server can be
// constructed
func TestNewKBaseAuthServer(t *testing.T) {
	assert := assert.New(t)
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	server, err := NewKBaseAuthServer(devToken)
	assert.NotNil(server, "Authentication server not created")
	assert.Nil(err, "Authentication server constructor triggered an error")
}

// tests whether the authentication server can return information for the
// user associated with the specified developer token
func TestUserInfo(t *testing.T) {
	assert := assert.New(t)
	devToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
	server, _ := NewKBaseAuthServer(devToken)
	assert.NotNil(server)
	userInfo, err := server.UserInfo()
	assert.Nil(err)

	assert.True(len(userInfo.Username) > 0)
	assert.True(len(userInfo.Email) > 0)
	assert.Equal(os.Getenv("DTS_KBASE_TEST_ORCID"), userInfo.Orcid)
}
