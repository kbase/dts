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

package auth

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// this type represents a proxy for the KBase Auth2 server
// (https://github.com/kbase/auth2)
type KBaseAuthServer struct {
	// path to server
	URL string
	// API version
	ApiVersion int
	// OAuth2 access token
	AccessToken string
}

// constructs or retrieves a proxy to the KBase authentication server using the
// given OAuth2 access token (corresponding to the current user), or returns a
// non-nil error explaining any issue encountered
func NewKBaseAuthServer(accessToken string) (*KBaseAuthServer, error) {

	// check our list of KBase auth server instances for this access token
	if instances == nil {
		instances = make(map[string]*KBaseAuthServer)
	}
	if server, found := instances[accessToken]; found {
		return server, nil
	} else {
		server := KBaseAuthServer{
			URL:         fmt.Sprintf("%s/services/auth", kbaseURL),
			ApiVersion:  2,
			AccessToken: accessToken,
		}

		// verify that the access token works (i.e. that the client is logged in)
		kbaseUser, err := server.kbaseUser()
		if err != nil {
			return nil, err
		}

		// register the local username under all its ORCIDs with our KBase user
		// federation mechanism
		for _, pid := range kbaseUser.Idents {
			if pid.Provider == "OrcID" {
				orcid := pid.UserName
				err = SetKBaseLocalUsernameForOrcid(orcid, kbaseUser.Username)
				if err != nil {
					break
				}
			}
		}

		if err == nil {
			// register this instance of the auth server
			instances[accessToken] = &server
		}
		return &server, err
	}
}

// returns a normalized user record for the current KBase user
func (server KBaseAuthServer) Client() (Client, error) {
	kbUser, err := server.kbaseUser()
	if err != nil {
		return Client{}, err
	}
	client := Client{
		Name:     kbUser.Display,
		Username: kbUser.Username,
		Email:    kbUser.Email,
	}
	for _, pid := range kbUser.Idents {
		// grab the first ORCID associated with the user
		if pid.Provider == "OrcID" {
			client.Orcid = pid.UserName
			break
		}
	}
	return client, nil
}

//-----------
// Internals
//-----------

const (
	kbaseURL = "https://kbase.us"
)

// a record containing information about a user logged into the KBase Auth2
// server
type kbaseUser struct {
	// KBase username
	Username string `json:"user"`
	// KBase user display name
	Display string `json:"display"`
	// User email address
	Email string `json:"email"`
	// Identities with associated providers
	Idents []struct {
		Provider string `json:"provider"`
		UserName string `json:"provusername"`
	} `json:"idents"`
}

// here's how KBase represents errors in responses to API calls
type kbaseAuthErrorResponse struct {
	HttpCode   int           `json:"httpcode"`
	HttpStatus int           `json:"httpstatus"`
	AppCode    int           `json:"appcode"`
	AppError   string        `json:"apperror"`
	Message    string        `json:"message"`
	CallId     int64         `json:"callid"`
	Time       time.Duration `json:"time"`
}

// here's a set of instances to the KBase auth server, mapped by OAuth2
// access token
var instances map[string]*KBaseAuthServer

// emits an error representing the error in a response to the auth server
func kbaseAuthError(response *http.Response) error {
	// read the error message from the response body
	var err error
	body, mErr := io.ReadAll(response.Body)
	if mErr == nil {
		var result kbaseAuthErrorResponse
		mErr = json.Unmarshal(body, &result)
		if mErr == nil {
			if len(result.Message) > 0 {
				err = fmt.Errorf("KBase Auth error (%d): %s", response.StatusCode,
					result.Message)
			} else {
				err = fmt.Errorf("KBase Auth error: %d", response.StatusCode)
			}
		}
	}
	return err
}

// constructs a new request to the auth server with the correct headers, etc
// * method can be http.MethodGet, http.MethodPut, http.MethodPost, etc
// * resource is the name of the desired endpoint/resource
// * body can be http.NoBody
func (server KBaseAuthServer) newRequest(method, resource string,
	body io.Reader) (*http.Request, error) {

	req, err := http.NewRequest(method,
		fmt.Sprintf("%s/api/V%d/%s", server.URL, server.ApiVersion, resource),
		body,
	)
	if err != nil {
		return nil, err
	}
	// the required authorization header contains only the unencoded access token
	req.Header.Add("Authorization", server.AccessToken)
	return req, nil
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (server KBaseAuthServer) get(resource string) (*http.Response, error) {
	req, err := server.newRequest(http.MethodGet, resource, http.NoBody)
	if err != nil {
		return nil, err
	}
	var client http.Client
	return client.Do(req)
}

// returns information for the current KBase user accessing the auth server
func (server KBaseAuthServer) kbaseUser() (kbaseUser, error) {
	var user kbaseUser
	resp, err := server.get("me")
	if err != nil {
		return user, err
	}
	if resp.StatusCode != 200 {
		err = kbaseAuthError(resp)
		if err != nil {
			return user, err
		}
	}
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return user, err
	}
	err = json.Unmarshal(body, &user)

	// make sure we have at least one ORCID for this user
	if len(user.Idents) < 1 {
		return user, fmt.Errorf("KBase Auth2: No providers associated with this user!")
	}
	foundOrcid := false
	for _, pid := range user.Idents {
		if pid.Provider == "OrcID" {
			foundOrcid = true
			break
		}
	}
	if !foundOrcid {
		return user, fmt.Errorf("KBase Auth2: No ORCID IDs associated with this user!")
	}
	return user, err
}
