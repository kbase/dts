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

package globus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

const (
	globusTransferBaseURL    = "https://transfer.api.globusonline.org"
	globusTransferApiVersion = "v0.10"
)

// this error type is returned when a Globus operation fails for any reason
type GlobusError struct {
	Code    string `json:"code"`
	Message string `json:"message"`

	// ConsentRequired error field
	RequiredScopes []string `json:"required_scopes"`
}

func (e GlobusError) Error() string {
	return fmt.Sprintf("%s (%s)", e.Message, e.Code)
}

// this type satisfies the endpoints.Endpoint interface for Globus endpoints
type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// root directory for endpoint
	RootDir string
	// HTTP client that caches queries
	Client http.Client
	// OAuth2 access token
	AccessToken string
	// access scopes
	Scopes []string

	// authentication stuff
	ClientId     uuid.UUID
	ClientSecret string
}

// creates a new Globus endpoint using the information supplied in the
// DTS configuration file under the given endpoint name
func NewEndpoint(endpointName string) (endpoints.Endpoint, error) {
	epConfig, found := config.Endpoints[endpointName]
	if !found {
		return nil, fmt.Errorf("'%s' is not an endpoint", endpointName)
	}
	if epConfig.Provider != "globus" {
		return nil, fmt.Errorf("'%s' is not a Globus endpoint", endpointName)
	}

	defaultScopes := []string{"urn:globus:auth:scope:transfer.api.globus.org:all"}
	ep := &Endpoint{
		Name:         epConfig.Name,
		Id:           epConfig.Id,
		Scopes:       defaultScopes,
		ClientId:     epConfig.Auth.ClientId,
		ClientSecret: epConfig.Auth.ClientSecret,
	}

	// if needed, authenticate to obtain a Globus Transfer API access token
	var zeroId uuid.UUID
	if ep.ClientId != zeroId {
		err := ep.authenticate()
		if err != nil {
			return ep, err
		}
	}

	// if present, the root entry overrides the endpoint's root, and is expressed
	// as a path relative to it
	if epConfig.Root != "" {
		ep.RootDir = epConfig.Root
	} else {
		ep.RootDir = "/"
	}
	slog.Debug(fmt.Sprintf("Endpoint %s: root directory is %s",
		endpointName, epConfig.Root))

	return ep, nil
}

func (ep *Endpoint) Root() string {
	return ep.RootDir
}

func (ep *Endpoint) FilesStaged(files []frictionless.DataResource) (bool, error) {
	// find all the directories in which these files reside
	filesInDir := make(map[string][]string)
	for _, resource := range files {
		dir, file := filepath.Split(resource.Path)
		dir = filepath.Join(ep.RootDir, dir)
		if _, found := filesInDir[dir]; !found {
			filesInDir[dir] = make([]string, 0)
		}
		filesInDir[dir] = append(filesInDir[dir], file)
	}

	// for each directory, check for its existence and that its files are present
	// (https://docs.globus.org/api/transfer/file_operations/#list_directory_contents)
	for dir, files := range filesInDir {
		values := url.Values{}
		values.Add("path", dir)
		values.Add("orderby", "name ASC")
		resource := fmt.Sprintf("operation/endpoint/%s/ls", ep.Id.String())
		body, err := ep.get(resource, values)
		if err != nil {
			switch lsErr := err.(type) {
			case *GlobusError:
				switch lsErr.Code {
				case "ClientError.NotFound":
					// it's okay if the directory doesn't exist -- it might need to be staged
					return false, nil
				default:
					// propagate the error
					return false, err
				}
			default:
				// propagate all other error types
				return false, err
			}
		}

		// https://docs.globus.org/api/transfer/file_operations/#dir_listing_response
		type DirListingResponse struct {
			Data []struct {
				Name string `json:"name"`
			} `json:"DATA"`
		}
		var response DirListingResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return false, err
		}
		filesPresent := make(map[string]bool)
		for _, data := range response.Data {
			filesPresent[data.Name] = true
		}
		for _, file := range files {
			if _, present := filesPresent[file]; !present {
				return false, nil
			}
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task/#get_task_list
	values := url.Values{}
	values.Add("fields", "task_id")
	values.Add("filter", "status:ACTIVE,INACTIVE/label:DTS")
	values.Add("limit", "1000")
	values.Add("orderby", "name ASC")

	body, err := ep.get("task_list", url.Values{})
	if err != nil {
		return nil, err
	}
	type TaskListResponse struct {
		Length int `json:"length"`
		Limit  int `json:"limіt"`
		Data   []struct {
			TaskId uuid.UUID `json:"task_id"`
		} `json:"DATA"`
	}
	var response TaskListResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	taskIds := make([]uuid.UUID, len(response.Data))
	for i, data := range response.Data {
		taskIds[i] = data.TaskId
	}
	return taskIds, nil
}

func (ep *Endpoint) Transfer(destination endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	// NOTE: We don't check whether files are staged here, because the endpoint
	// NOTE: itself doesn't always have a reliable staging check (e.g. JDP's
	// NOTE: private data is invisible to Globus directory listings).
	// NOTE: Consequently, we assume that files are staged by the time this
	// NOTE: function is called.

	// obtain a submission ID
	submissionId, err := ep.getSubmissionId()
	if err != nil {
		return uuid.UUID{}, err
	}

	// now, submit the transfer task itself
	return ep.submitTransfer(destination, submissionId, files)
}

// mapping of Globus status code strings to DTS status codes
var statusCodesForStrings = map[string]endpoints.TransferStatusCode{
	"ACTIVE":    endpoints.TransferStatusActive,
	"INACTIVE":  endpoints.TransferStatusInactive,
	"SUCCEEDED": endpoints.TransferStatusSucceeded,
	"FAILED":    endpoints.TransferStatusFailed,
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	resource := fmt.Sprintf("task/%s", id.String())
	body, err := ep.get(resource, url.Values{})
	if err != nil {
		return endpoints.TransferStatus{}, err
	}
	if responseIsError(body) {
		var globusErr GlobusError
		err := json.Unmarshal(body, &globusErr)
		if err == nil {
			err = &globusErr
		}
		return endpoints.TransferStatus{}, err
	}
	type TaskResponse struct {
		Files                      int    `json:"files"`
		FilesSkipped               int    `json:"files_skipped"`
		FilesTransferred           int    `json:"files_transferred"`
		IsPaused                   bool   `json:"is_paused"`
		NiceStatus                 string `json:"nice_status"`
		NiceStatusShortDescription string `json:"nice_status_short_description"`
		Status                     string `json:"status"`
	}
	var response TaskResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return endpoints.TransferStatus{}, err
	}
	// check for an error condition in NiceStatus
	if response.NiceStatus != "" && response.NiceStatus != "OK" && response.NiceStatus != "Queued" {
		// get the event list for this task
		resource := fmt.Sprintf("task/%s/event_list", id.String())
		body, err := ep.get(resource, url.Values{})
		if err != nil {
			// fine, we'll just use the "nice status"
			return endpoints.TransferStatus{}, fmt.Errorf(response.NiceStatusShortDescription)
		}
		type Event struct {
			DataType    string `json:"DATA_TYPE"`
			Code        string `json:"code"`
			IsError     bool   `json:"is_error"`
			Description string `json:"description"`
			Details     string `json:"details"`
			Time        string `json:"time"`
		}
		type EventList struct {
			Data []Event `json:"DATA"`
		}
		var eventList EventList
		json.Unmarshal(body, &eventList)
		if response.NiceStatus == "AUTH" {
			for _, event := range eventList.Data {
				if event.IsError {
					slog.Debug(fmt.Sprintf("Globus task %s: status check failed with AUTH error below (probably bogus, ignoring): ", id.String()))
					slog.Debug(fmt.Sprintf("Globus task %s: %s (%s):\n%s", id.String(), event.Description, event.Code, event.Details))
				}
			}
		} else {
			// it's probably real
			// find the first error event
			for _, event := range eventList.Data {
				if event.IsError {
					// sometimes Globus throws an AUTH error here during a network burp, so we
					// ignore it and report a failed status check (after all, we can't get here
					// without AUTHing successfully!)
					return endpoints.TransferStatus{},
						fmt.Errorf("%s (%s):\n%s", event.Description, event.Code,
							event.Details)
				}
			}
			// fall back to the "nice status"
			return endpoints.TransferStatus{}, fmt.Errorf(response.NiceStatusShortDescription)
		}
	}
	return endpoints.TransferStatus{
		Code:                statusCodesForStrings[response.Status],
		NumFiles:            response.Files,
		NumFilesSkipped:     response.FilesSkipped,
		NumFilesTransferred: response.FilesTransferred,
	}, nil
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	// Because cancellation requests can't be honored under all circumstances,
	// this Globus call is asynchronous. Nevertheless, the Globus documentation
	// (https://docs.globus.org/api/transfer/task/#cancel_task_by_id) claims the
	// call can take up to 10 seconds before returning, which doesn't meet the
	// needs of the DTS. The possible outcomes of the call are identified with
	// these response codes:
	// 1. "Canceled", indicating that the task has been canceled
	// 2. "CancelAccepted", indicating that the cancellation request has been
	//    acknowledged but not yet processed
	// 3. "TaskComplete", indicating that the task is complete and not able to
	//    be canceled.
	//
	// We live with the 10-second wait for now, since our polling interval is
	// large.
	type CancellationResponse struct {
		Code      string `json:"code"` // should be "Canceled"
		Message   string `json:"message"`
		RequestId string `json:"request_id"`
		Resource  string `json:"resource"`
	}
	resource := fmt.Sprintf("task/%s/cancel", id.String())
	_, err := ep.post(resource, nil) // can take up to 10 ѕeconds!
	// FIXME: if this ^^^ becomes an issue, we can dispatch the POST to a
	// FIXME: persistent goroutine to handle the cancellation
	if err != nil {
		if globusError, ok := err.(*GlobusError); ok {
			switch globusError.Code {
			case "Canceled", "CancelAccepted", "TaskComplete": // it worked!
				err = nil
			}
		}
	}
	return err
}

//-----------
// Internals
//-----------

// returns true if a Globus response body matches an error
func responseIsError(body []byte) bool {
	bodyStr := string(body)
	return strings.Contains(bodyStr, "\"code\"") &&
		!strings.Contains(bodyStr, "\"code\": \"Accepted\"") &&
		strings.Contains(string(body), "\"message\"")
}

// (re)authenticates with Globus using its client ID and secret to obtain an
// access token with consents for its relevant list of scopes
// (https://docs.globus.org/api/auth/reference/#client_credentials_grant)
func (ep *Endpoint) authenticate() error {
	authUrl := "https://auth.globus.org/v2/oauth2/token"
	data := url.Values{}
	data.Set("scope", strings.Join(ep.Scopes, "+"))
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, authUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return err
	}
	req.SetBasicAuth(ep.ClientId.String(), ep.ClientSecret)
	req.Header.Add("Content-Type", "application-x-www-form-urlencoded")

	// send the request
	resp, err := ep.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("Couldn't authenticate via Globus Auth API (%d)", resp.StatusCode)
	}

	// read and unmarshal the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	type AuthResponse struct {
		AccessToken    string `json:"access_token"`
		Scope          string `json:"scope"`
		ResourceServer string `json:"resource_server"`
		ExpiresIn      int    `json:"expires_in"`
		TokenType      string `json:"token_type"`
	}
	var authResponse AuthResponse
	err = json.Unmarshal(body, &authResponse)
	if err != nil {
		return err
	}

	// FIXME: check the scopes to see if they match our requested ones?

	// stash the access token
	ep.AccessToken = authResponse.AccessToken

	return nil
}

// This helper sends the given HTTP request, parsing the response for
// Globus-style error codes/messages and handling the ones that can be
// handled automatically (e.g. consent/scope related errors). In any case,
// it returns a byte slice containing the body of the response or an
// error indicating failure.
func (ep *Endpoint) sendRequest(request *http.Request) ([]byte, error) {
	// send the initial request and read its contents
	resp, err := ep.Client.Do(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	// check the response for a Globus-style error code / message
	if responseIsError(body) {
		var errResp GlobusError
		err = json.Unmarshal(body, &errResp)
		if err != nil {
			return nil, err
		}
		if errResp.Code == "ConsentRequired" || errResp.Code == "AuthenticationFailed" {
			// our token has expired or we're missing a required scope,
			// so reauthenticate
			ep.Scopes = errResp.RequiredScopes
			err = ep.authenticate()
			if err != nil {
				return nil, err
			}
			// try the request again
			resp, err = ep.Client.Do(request)
			if err != nil {
				return nil, err
			}
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		} else {
			// other errors are propagated
			err = &errResp
		}
	}
	return body, err
}

// Performs a GET request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) get(resource string, values url.Values) ([]byte, error) {
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("%s/%s", globusTransferApiVersion, resource)
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ep.AccessToken))

	return ep.sendRequest(req)
}

// Performs a POST request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) post(resource string, body io.Reader) ([]byte, error) {
	u, err := url.ParseRequestURI(globusTransferBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("%s/%s", globusTransferApiVersion, resource)
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("POST: %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", ep.AccessToken))
	req.Header.Set("Content-Type", "application/json")

	return ep.sendRequest(req)
}

// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
func (ep *Endpoint) getSubmissionId() (uuid.UUID, error) {
	var id uuid.UUID
	body, err := ep.get("submission_id", url.Values{})
	if err != nil {
		return id, err
	}
	type SubmissionIdResponse struct {
		Value uuid.UUID `json:"value"`
	}
	var response SubmissionIdResponse
	err = json.Unmarshal(body, &response)
	return response.Value, err
}

// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
// https://docs.globus.org/api/transfer/task_submit/#transfer_item_fields
func (ep *Endpoint) submitTransfer(destination endpoints.Endpoint,
	submissionId uuid.UUID, files []endpoints.FileTransfer) (uuid.UUID, error) {
	var xferId uuid.UUID

	type TransferItem struct {
		DataType          string `json:"DATA_TYPE"` // "transfer_item"
		SourcePath        string `json:"source_path"`
		DestinationPath   string `json:"destination_path"`
		ExternalChecksum  string `json:"external_checksum,omitempty"`
		ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
	}
	type SubmissionRequest struct {
		DataType            string         `json:"DATA_TYPE"` // "transfer"
		Id                  string         `json:"submission_id"`
		Label               string         `json:"label"` // "DTS"
		Data                []TransferItem `json:"DATA"`
		DestinationEndpoint string         `json:"destination_endpoint"`
		SourceEndpoint      string         `json:"source_endpoint"`
		SyncLevel           int            `json:"sync_level"`
		VerifyChecksum      bool           `json:"verify_checksum"`
		FailOnQuotaErrors   bool           `json:"fail_on_quota_errors"`
	}
	xferItems := make([]TransferItem, len(files))
	for i, file := range files {
		xferItems[i] = TransferItem{
			DataType:          "transfer_item",
			SourcePath:        filepath.Join(ep.RootDir, file.SourcePath),
			DestinationPath:   file.DestinationPath,
			ExternalChecksum:  file.Hash,
			ChecksumAlgorithm: file.HashAlgorithm,
		}
	}

	// the destination is a Globus endpoint, right?
	gDestination, ok := destination.(*Endpoint)
	if !ok {
		return xferId, fmt.Errorf("The destination is not a Globus endpoint.")
	}

	data, err := json.Marshal(SubmissionRequest{
		DataType:            "transfer",
		Id:                  submissionId.String(),
		Label:               "DTS",
		Data:                xferItems,
		DestinationEndpoint: gDestination.Id.String(),
		SourceEndpoint:      ep.Id.String(),
		SyncLevel:           3, // transfer only if checksums don't match
		VerifyChecksum:      true,
		FailOnQuotaErrors:   true,
	})
	if err != nil {
		return xferId, err
	}
	body, err := ep.post("transfer", bytes.NewReader(data))
	if err != nil {
		return xferId, err
	}
	if responseIsError(body) {
		var globusErr GlobusError
		err = json.Unmarshal(body, &globusErr)
		if err == nil {
			err = &globusErr
		}
		return xferId, err
	}
	type SubmissionResponse struct {
		TaskId uuid.UUID `json:"task_id"`
	}

	var gResp SubmissionResponse
	err = json.Unmarshal(body, &gResp)
	if err != nil {
		return xferId, err
	}
	xferId = gResp.TaskId
	slog.Debug(fmt.Sprintf("Initiated Globus transfer task %s (%d files)",
		xferId.String(), len(files)))
	return xferId, nil
}
