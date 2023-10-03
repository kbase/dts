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

const (
	kbaseURL = "https://kbase.us"
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

		// verify that the access token works (i.e. that the user is logged in)
		resp, err := server.get("me")
		defer resp.Body.Close()
		if err != nil {
			return nil, err
		} else if resp.StatusCode != 200 {
			err = kbaseAuthError(resp)
		}

		instances[accessToken] = &server
		return &server, err
	}
}

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
func (server *KBaseAuthServer) newRequest(method, resource string,
	body io.Reader) (*http.Request, error) {

	req, err := http.NewRequest(method,
		fmt.Sprintf("%s/api/V%d/%s", server.URL, server.ApiVersion, resource),
		body,
	)
	if err != nil {
		return nil, err
	}
	// The required authorization header contains only the unencoded access token.
	// Unfortunately, this is a non-RFC-compliant Authorization header, and Go
	// validates its request headers, so it can't send non-compliant ones, and I
	// don't want to spend time trying to work around this issue when the right
	// answer is to update the KBase auth server to be RFC-compliant. Why not use
	// the Bearer method?

	// Anyway the following won't work till this is fixed, so we can't rely on
	// the KBase auth server for now
	req.Header.Set("Authorization", server.AccessToken)
	return req, nil
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (server *KBaseAuthServer) get(resource string) (*http.Response, error) {
	req, err := server.newRequest(http.MethodGet, resource, http.NoBody)
	if err != nil {
		return nil, err
	}
	var client http.Client
	return client.Do(req)
}

// returns the current KBase user's registered ORCID identifiers (and/or an error)
// a user can have 0, 1, or many associated ORCID identifiers
func (server *KBaseAuthServer) Orcids() ([]string, error) {
	resp, err := server.get("me")
	var orcidIds []string
	if err == nil {
		if resp.StatusCode != 200 {
			err = kbaseAuthError(resp)
		} else {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				var result struct {
					Idents []struct {
						Provider string `json:"provider"`
						UserName string `json:"provusername"`
					} `json:"idents"`
				}
				err = json.Unmarshal(body, &result)
				if err == nil {
					if len(result.Idents) < 1 {
						return nil, fmt.Errorf("No ORCID IDs associated with this user!")
					}
					orcidIds = make([]string, 0)
					for _, pid := range result.Idents {
						if pid.Provider == "OrcID" {
							orcidIds = append(orcidIds, pid.UserName)
						}
					}
				}
			}
		}

	}
	return orcidIds, err
}
