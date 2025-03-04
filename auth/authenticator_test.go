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

// These tests verify that we can connect to the DTS authenticator, which
// matches a user's DTS access token to a record in an encrypted tab-separated
// variable (TSV) file.
package auth

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/fernet/fernet-go"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/dtstest"
)

// runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}

// runs all tests serially
func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	tester.TestNewAuthenticator()
	tester.TestInvalidDataDirectory()
	tester.TestGetUser()
}

// Fernet encryption/decryption key
var TestKey fernet.Key

// temporary testing directory
var TestDir string

// testing access token
var TestAccessToken string

// test user
var TestUser = User{
	Name:         "Josiah Carberry",
	Email:        "jsc@example.com",
	Orcid:        "0000-0002-1825-0097",
	Organization: "Brown University",
}

func setup() {
	dtstest.EnableDebugLogging()

	log.Print("Creating testing directory...\n")
	var err error
	TestDir, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err.Error())
	}
	config.Service.DataDirectory = TestDir

	err = TestKey.Generate()
	if err != nil {
		log.Panicf("Couldn't generate encryption key: %s", err.Error())
	}
	config.Service.Secret = TestKey.Encode()

	TestAccessToken = "7029c1877e9c2dd3dab814cc0f2763af"

	// write an access TSV file and encrypt it with a secret
	// (fictitious orcid record: https://orcid.org/0000-0002-1825-0097)
	plaintext := fmt.Sprintf("# Name | Email | Orcid | Organization | Token\n"+
		"%s\t%s\t%s\t%s\t%s\n",
		TestUser.Name, TestUser.Email, TestUser.Orcid,
		TestUser.Organization, TestAccessToken)
	token, err := fernet.EncryptAndSign([]byte(plaintext), &TestKey)
	if err != nil {
		log.Panicf("Couldn't encrypt test access data: %s", err.Error())
	}

	output, err := os.Create(filepath.Join(TestDir, "access.dat"))
	if err != nil {
		log.Panicf("Couldn't open test access data file: %s", err.Error())
	}
	defer output.Close()
	_, err = output.Write(token)
	if err != nil {
		log.Panicf("Couldn't write test access data file: %s", err.Error())
	}
}

// To run the tests serially, we attach them to a SerialTests type and
// have them run by a a single test runner.
type SerialTests struct{ Test *testing.T }

// tests whether a proxy for the KBase authentication server can be
// constructed
func (t *SerialTests) TestNewAuthenticator() {
	assert := assert.New(t.Test)
	auth, err := NewAuthenticator()
	assert.NotNil(auth, "Authenticator not created")
	assert.Nil(err, "Authenticator constructor triggered an error")
}

// tests the case in which a directory without an encrpyted access.dat file has
// been configured for the authenticator
func (t *SerialTests) TestInvalidDataDirectory() {
	assert := assert.New(t.Test)
	config.Service.DataDirectory = os.Getenv("HOME")
	auth, err := NewAuthenticator()
	assert.Nil(auth, "Authenticator created with invalid data directory")
	assert.NotNil(err, "Invalid data directory for authenticator triggered no error")
	config.Service.DataDirectory = TestDir
}

// tests whether the authenticator server can return information for the
// the user associated with a valid ORCID
func (t *SerialTests) TestGetUser() {
	assert := assert.New(t.Test)
	auth, err := NewAuthenticator()
	assert.NotNil(auth)
	assert.Nil(err)

	accessToken := TestAccessToken
	user, err := auth.GetUser(accessToken)
	assert.Nil(err)

	assert.Equal(TestUser.Name, user.Name)
	assert.Equal(TestUser.Email, user.Email)
	assert.Equal(TestUser.Orcid, user.Orcid)
	assert.Equal(TestUser.Organization, user.Organization)
}

// tests whether the authentication server can return information for a
// user with an ORCID not in the access file
// (fictitious ORCID: https://orcid.org/0000-0001-5109-3700)
func (t *SerialTests) TestGetInvalidUser() {
	assert := assert.New(t.Test)
	auth, err := NewAuthenticator()
	badAccessToken := "c5683570c1412b77eabcb9d6eb0aae2a"
	_, err = auth.GetUser(badAccessToken)
	assert.NotNil(err)
}

func breakdown() {
	if TestDir != "" {
		log.Printf("Deleting testing directory %s...\n", TestDir)
		os.RemoveAll(TestDir)
	}
}
