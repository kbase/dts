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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/endpoints"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

// this error type is returned when a Globus transfer operation fails for any reason
type GlobusTransferError struct {
	Code    string `json:"code"`
	Message string `json:"message"`

	// ConsentRequired error field
	RequiredScopes []string `json:"required_scopes"`
}

func (e GlobusTransferError) Error() string {
	return fmt.Sprintf("%s (%s)", e.Message, e.Code)
}

// this error type is returned when a non-transfer Globus operation fails for any reason
type GlobusGenericError struct {
	Message string
}

func (e GlobusGenericError) Error() string {
	return e.Message
}

// this error indicates that a Globus endpoint has no associated HTTPS server
type GlobusHttpsClientNotAvailableError struct {
	Endpoint uuid.UUID
}

func (e GlobusHttpsClientNotAvailableError) Error() string {
	return fmt.Sprintf("no HTTPS Server is not available for endpoint %s",
		e.Endpoint.String())
}

// this error indicates that the Globus Connect Server Manager client is not available for the
// endpoint in question
type GlobusConnectServerManagerNotAvailableError struct {
	Endpoint uuid.UUID
}

func (e GlobusConnectServerManagerNotAvailableError) Error() string {
	return fmt.Sprintf("the Globus Connect Manager Server API is not available for endpoint %s",
		e.Endpoint.String())
}

type GlobusEndpointInfo struct {
	DisableVerify           bool   `json:"disable_verify"`       // true if checksums are not available
	EntityType              string `json:"entity_type"`          // indicates type of Globus endpoint server
	ForceVerify             bool   `json:"force_verify"`         // true if checksums must be available
	GCSManagerUrl           string `json:"gcs_manager_url"`      // non-blank if GCS Manager operations are supported
	HighAssurance           bool   `json:"high_assurance"`       // true if endpoint is a connector
	HttpsServer             string `json:"https_server"`         // non-blank if HTTPS transfers are supported
	MappedCollectionId      string `json:"mapped_collection_id"` // non-blank if GCS Manager operations are supported
	NonFunctionalEndpointId string `json:"non_functional_endpoint_id"`
}

type GlobusTransferStatus struct {
	Files                      int    `json:"files"`
	FilesSkipped               int    `json:"files_skipped"`
	FilesTransferred           int    `json:"files_transferred"`
	IsPaused                   bool   `json:"is_paused"`
	NiceStatus                 string `json:"nice_status"`
	NiceStatusShortDescription string `json:"nice_status_short_description"`
	Status                     string `json:"status"`
}

// Globus Transfer API
// https://docs.globus.org/api/transfer/
type GlobusTransferClient struct {
	AccessToken string
	Auth        *GlobusAuthClient
	Scopes      []string
	EndpointId  uuid.UUID
	Info        GlobusEndpointInfo
}

// Globus Auth API
// https://docs.globus.org/api/auth/
type GlobusAuthClient struct {
	Credential auth.Credential
	Url        string
}

// Globus HTTPS upload client
// https://docs.globus.org/globus-connect-server/v5/https-access-collections
type GlobusHttpsClient struct {
	AccessToken   string
	Scopes        []string
	Url, DataPath string
}

type GlobusStorageGateway struct {
	ConnectorId uuid.UUID
	Id          uuid.UUID
	Providers   []string // "s3", etc
}

// Globus Connect Server Manager API
// https://docs.globus.org/globus-connect-server/v5.4/api/
type GlobusConnectServerManagerClient struct {
	AccessToken     string
	ClientId        string // credential ID that granted access token
	EndpointId      uuid.UUID
	Scopes          []string
	Url             string
	StorageGateways []GlobusStorageGateway
}

func NewGlobusTransferClient(credential auth.Credential, endpointId uuid.UUID) (GlobusTransferClient, error) {
	auth, err := NewGlobusAuthClient(credential)
	if err != nil {
		return GlobusTransferClient{}, err
	}
	t := GlobusTransferClient{
		Auth:       auth,
		EndpointId: endpointId,
		Scopes: []string{
			"urn:globus:auth:scope:transfer.api.globus.org:all",
		},
	}
	if t.AccessToken, err = t.Auth.Authenticate(t.Scopes); err != nil {
		return GlobusTransferClient{}, err
	}
	if t.Info, err = t.getEndpointInfo(endpointId); err != nil {
		return GlobusTransferClient{}, err
	}
	return t, nil
}

func (t GlobusTransferClient) HttpsClient(endpointId uuid.UUID) (GlobusHttpsClient, error) {
	if t.Info.HttpsServer == "" {
		return GlobusHttpsClient{}, &GlobusHttpsClientNotAvailableError{t.EndpointId}
	}
	h := GlobusHttpsClient{
		Scopes: []string{
			fmt.Sprintf("https://auth.globus.org/scopes/%s/https", endpointId.String()),
		},
		Url: t.Info.HttpsServer,
	}
	var err error
	if h.AccessToken, err = t.Auth.Authenticate(h.Scopes); err != nil {
		return GlobusHttpsClient{}, err
	}
	return h, nil
}

func (t GlobusTransferClient) ConnectServerManagerClient() (GlobusConnectServerManagerClient, error) {
	if t.Info.GCSManagerUrl == "" {
		return GlobusConnectServerManagerClient{}, &GlobusConnectServerManagerNotAvailableError{Endpoint: t.EndpointId}
	}
	scopes := []string{fmt.Sprintf("urn:globus:auth:scope:%s:manage_collections", t.EndpointId.String())}
	//if !t.Info.HighAssurance {
	//	scopes[0] += fmt.Sprintf("[*:https://auth.globus.org/scopes/%s/data_access]", t.EndpointId)
	//}
	m := GlobusConnectServerManagerClient{
		ClientId:   t.Auth.Credential.Id,
		EndpointId: t.EndpointId,
		Scopes:     scopes,
		Url:        t.Info.GCSManagerUrl,
	}
	var err error
	if m.AccessToken, err = t.Auth.Authenticate(m.Scopes); err != nil {
		return GlobusConnectServerManagerClient{}, err
	}

	// get the storage gateway ID for this endpoint / collection
	m.getStorageGatewayInfo()

	return m, nil
}

// creates a new Globus endpoint using the given information
func NewGlobusAuthClient(credential auth.Credential) (*GlobusAuthClient, error) {
	return &GlobusAuthClient{
		Credential: credential,
		Url:        "https://auth.globus.org/v2/oauth2/token",
	}, nil
}

// (re)authenticates with Globus using its client ID and secret to obtain an
// access token with consents for its relevant list of scopes
// (https://docs.globus.org/api/auth/reference/#client_credentials_grant)
// returns an access token corresponding to the given set of scopes
func (c GlobusAuthClient) Authenticate(scopes []string) (string, error) {
	authUrl := "https://auth.globus.org/v2/oauth2/token"
	data := url.Values{}
	data.Set("scope", strings.Join(scopes, " "))
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, authUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(c.Credential.Id, c.Credential.Secret)
	req.Header.Add("Content-Type", "application-x-www-form-urlencoded")

	// send the request using a fresh HTTP client
	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		// fish specifics out of the response
		type AuthError struct {
			Error       string `json:"error"`
			Description string `json:"error_description"`
			URI         string `json:"error_uri"`
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		var authError AuthError
		err = json.Unmarshal(body, &authError)
		if err != nil {
			// report the authentication error without details
			return "", fmt.Errorf("couldn't authenticate via Globus Auth API (%d)", resp.StatusCode)
		}
		switch authError.Error {
		case "unknown_scope_error":
			return "", fmt.Errorf("couldn't authenticate via Globus Auth API: unknown scope(s) requested: %v (%d)",
				scopes, resp.StatusCode)
		case "invalid_scope_error":
			return "", fmt.Errorf("couldn't authenticate via Globus Auth API: invalid scope(s) requested: %v (%d)",
				scopes, resp.StatusCode)
		default:
			if len(authError.Description) > 0 {
				return "", fmt.Errorf("couldn't authenticate via Globus Auth API: %s; %s (%d)",
					authError.Error, authError.Description, resp.StatusCode)
			}
			return "", fmt.Errorf("couldn't authenticate via Globus Auth API: %s (%d)",
				authError.Error, resp.StatusCode)
		}
	}

	// read and unmarshal the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
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
		return "", err
	}

	// FIXME: check the scopes to see if they match our requested ones?

	// stash the access token
	return authResponse.AccessToken, nil
}

// https://docs.globus.org/api/transfer/file_operations/#dir_listing_response
// (https://docs.globus.org/api/transfer/file_operations/#list_directory_contents)
func (c *GlobusTransferClient) FilesInDirectory(dir string) ([]string, error) {
	values := url.Values{}
	values.Add("path", dir)
	values.Add("orderby", "name ASC")
	body, err := c.get(fmt.Sprintf("operation/endpoint/%s/ls", c.EndpointId), values)
	if err != nil {
		return nil, err
	}

	type DirListingResponse struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"DATA"`
	}
	var response DirListingResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	files := make([]string, len(response.Data))
	for i, datum := range response.Data {
		files[i] = datum.Name
	}
	return files, nil
}

func (c *GlobusTransferClient) TransferTasks() ([]uuid.UUID, error) {
	// https://docs.globus.org/api/transfer/task/#get_task_list
	values := url.Values{}
	values.Add("fields", "task_id")
	values.Add("filter", "status:ACTIVE,INACTIVE/label:DTS")
	values.Add("limit", "1000")
	values.Add("orderby", "name ASC")

	body, err := c.get("task_list", url.Values{})
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

// Transfers files from the given source endpoint to the given destination endpoint.
// NOTE: file paths are relative to the root of the Globus collection, NOT its
// NOTE: "data directory"
func (c *GlobusTransferClient) Transfer(credential auth.Credential, sourceId, destinationId uuid.UUID, files []endpoints.FileTransfer) (uuid.UUID, error) {
	// obtain a submission ID
	submissionId, err := c.getSubmissionId()
	if err != nil {
		return uuid.UUID{}, err
	}

	// Occasionally, Globus returns a zero-valued UUID (uuid.Nil) and no error (network burp?).
	// So we pause and resubmit in this case
	for submissionId == uuid.Nil {
		time.Sleep(time.Second)
		submissionId, err = c.getSubmissionId()
		if err != nil {
			return uuid.UUID{}, err
		}
	}

	// now, submit the transfer task itself
	return c.submitTransfer(credential, sourceId, destinationId, submissionId, files)
}

func (c *GlobusTransferClient) getEndpointInfo(id uuid.UUID) (GlobusEndpointInfo, error) {
	// query the endpoint for its capabilities
	body, err := c.get(fmt.Sprintf("endpoint/%s", id.String()), url.Values{})
	if err != nil {
		return GlobusEndpointInfo{}, err
	}
	var info GlobusEndpointInfo
	err = json.Unmarshal(body, &info)
	return info, err
}

// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
func (c GlobusTransferClient) getSubmissionId() (uuid.UUID, error) {
	var id uuid.UUID
	body, err := c.get("submission_id", url.Values{})
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

// https://docs.globus.org/api/transfer/endpoints_and_collections/#get_endpoint_or_collection_by_id
// https://docs.globus.org/api/transfer/task_submit/#submit_transfer_task
// https://docs.globus.org/api/transfer/task_submit/#transfer_item_fields
func (c GlobusTransferClient) submitTransfer(credential auth.Credential, sourceId, destinationId, submissionId uuid.UUID,
	files []endpoints.FileTransfer) (uuid.UUID, error) {
	var xferId uuid.UUID

	// are the source and destination endpoints configured in a conflicting way?
	destinationInfo, err := c.getEndpointInfo(destinationId)
	if err != nil {
		return xferId, err
	}
	if c.Info.ForceVerify && destinationInfo.DisableVerify { // not allowed!
		return xferId, &endpoints.IncompatibleDestinationError{
			Source:              sourceId.String(),
			SourceProvider:      "globus",
			Destination:         destinationId.String(),
			DestinationProvider: "globus",
			Message:             "Source endpoint forces checksum verification, but destination disables it.",
		}
	}

	// configure checksum settings based on destination endpoint info
	var verifyChecksum bool = true
	var syncLevel int = 3              // transfer only if checksums don't match
	if destinationInfo.DisableVerify { // checksum verification disabled on endpoint
		verifyChecksum = false
		syncLevel = 2 // transfer if source file is newer than destination file
	}

	type TransferItem struct {
		DataType          string `json:"DATA_TYPE"` // "transfer_item"
		SourcePath        string `json:"source_path"`
		DestinationPath   string `json:"destination_path"`
		ExternalChecksum  string `json:"external_checksum,omitempty"`
		ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
	}
	xferItems := make([]TransferItem, len(files))
	for i, file := range files {
		var checksum, checksumAlgorithm string
		if verifyChecksum {
			checksum = file.Hash
			checksumAlgorithm = file.HashAlgorithm
		}
		xferItems[i] = TransferItem{
			DataType:          "transfer_item",
			SourcePath:        file.SourcePath,
			DestinationPath:   file.DestinationPath,
			ExternalChecksum:  checksum,
			ChecksumAlgorithm: checksumAlgorithm,
		}
	}

	// submit the transfer request
	type SubmissionRequest struct {
		DataType             string         `json:"DATA_TYPE"` // "transfer"
		Id                   string         `json:"submission_id"`
		Label                string         `json:"label"` // "DTS"
		Data                 []TransferItem `json:"DATA"`
		DestinationEndpoint  string         `json:"destination_endpoint"`
		DestinationLocalUser string         `json:"destination_local_user,omitempty"`
		SourceEndpoint       string         `json:"source_endpoint"`
		SyncLevel            int            `json:"sync_level"`
		VerifyChecksum       bool           `json:"verify_checksum"`
		FailOnQuotaErrors    bool           `json:"fail_on_quota_errors"`
	}
	data, err := json.Marshal(SubmissionRequest{
		DataType:             "transfer",
		Id:                   submissionId.String(),
		Label:                "DTS",
		Data:                 xferItems,
		DestinationEndpoint:  destinationId.String(),
		DestinationLocalUser: credential.Username,
		SourceEndpoint:       sourceId.String(),
		SyncLevel:            syncLevel,
		VerifyChecksum:       verifyChecksum,
		FailOnQuotaErrors:    true,
	})
	if err != nil {
		return xferId, err
	}

	body, err := c.post("transfer", bytes.NewReader(data))
	if err != nil {
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

func (c *GlobusTransferClient) TaskStatus(taskId uuid.UUID) (GlobusTransferStatus, error) {
	body, err := c.get(fmt.Sprintf("task/%s", taskId.String()), url.Values{})
	if err != nil {
		return GlobusTransferStatus{}, err
	}
	var response GlobusTransferStatus
	err = json.Unmarshal(body, &response)
	return response, err
}

func (c *GlobusTransferClient) TaskEvents(taskId uuid.UUID) ([]GlobusEvent, error) {
	body, err := c.get(fmt.Sprintf("task/%s/event_list", taskId.String()), url.Values{})
	if err != nil {
		return nil, err
	}
	type EventList struct {
		Data []GlobusEvent `json:"DATA"`
	}
	var eventList EventList
	if err = json.Unmarshal(body, &eventList); err != nil {
		return nil, err
	}
	return eventList.Data, nil
}

func (c *GlobusTransferClient) Cancel(taskId uuid.UUID) error {
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
	_, err := c.post(fmt.Sprintf("task/%s/cancel", taskId.String()), nil)
	// NOTE: if this ^^^ becomes an issue, we can dispatch the POST to a
	// NOTE: persistent goroutine to handle the cancellation
	if err != nil {
		if globusError, ok := err.(*GlobusTransferError); ok {
			switch globusError.Code {
			case "Canceled", "CancelAccepted", "TaskComplete": // it worked!
				err = nil
			}
		}
	}
	return err
}

// Uploads a file to the given (absolute) path on the HTTPS server.
func (c GlobusHttpsClient) PutFile(path string, body io.Reader) error {
	resourcePath := c.Url + "/" + path
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus HTTPS PUT: %s", res))
	req, err := http.NewRequest(http.MethodPut, res, body)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return errorFromGlobusResponse(resp, respBody)
}

// https://docs.globus.org/globus-connect-server/v5.4/api/openapi_User_Credentials/#postUserCredential
func (c GlobusConnectServerManagerClient) AddOrUpdateUserCredential(user auth.User, provider string) (auth.Credential, error) {
	for connectionProvider, credential := range user.ConnectionCredentials {
		if connectionProvider == provider && provider == "s3" {
			return c.addOrUpdateS3UserCredential(user, credential)
		}
	}
	return auth.Credential{}, fmt.Errorf("unsupported user credential provider: %s", provider)
}

//-----------
// Internals
//-----------

// This method sends the given HTTP request, parsing the response for Globus-style error
// codes/messages and handling the ones that can be handled automatically (e.g. consent/scope
// related errors) by reauthenticating as needed and retrying the operation. See
// https://docs.globus.org/api/flows/working-with-consents for details on Globus scopes and
// consents. Returns a byte slice containing the body of the response.
func (c *GlobusTransferClient) sendRequest(request *http.Request) ([]byte, error) {
	// send the initial request with a fresh HTTP client
	var client http.Client
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	// check the response for a Globus-style error code / message
	err = errorFromGlobusResponse(resp, body)
	if err != nil {
		if xferErr, ok := err.(*GlobusTransferError); ok {
			if xferErr.Code == "ConsentRequired" || xferErr.Code == "AuthenticationFailed" {
				// our token has expired or we're missing a required scope,
				// so reauthenticate
				if len(xferErr.RequiredScopes) > 0 {
					c.Scopes = xferErr.RequiredScopes
				}
				if c.AccessToken, err = c.Auth.Authenticate(c.Scopes); err != nil {
					return nil, err
				}
				// try the request again using the new access token
				request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
				if request.GetBody != nil { // e.g. recreate POST body
					if request.Body, err = request.GetBody(); err != nil {
						return nil, err
					}
				}
				if resp, err = client.Do(request); err != nil {
					return nil, err
				}
				body, err = io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					return nil, err
				}
				return body, errorFromGlobusResponse(resp, body)
			} else {
				// other transfer errors are propagated
				return body, err
			}
		}
	}
	return body, err
}

// Performs a GET request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
func (c *GlobusTransferClient) get(resource string, values url.Values) ([]byte, error) {
	resourcePath := globusTransferApiBaseUrl + fmt.Sprintf("/%s/%s", globusTransferApiVersion, resource)
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus Transfer API: GET %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	return c.sendRequest(req)
}

// Performs a POST request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (c *GlobusTransferClient) post(resource string, body io.Reader) ([]byte, error) {
	resourcePath := globusTransferApiBaseUrl + fmt.Sprintf("/%s/%s", globusTransferApiVersion, resource)
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus Transfer API: POST %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	req.Header.Set("Content-Type", "application/json")

	return c.sendRequest(req)
}

// returns an error capturing any Globus-related error in a response body, or nil if the response
// doesn't appear to be an error
func errorFromGlobusResponse(response *http.Response, body []byte) error {
	bodyStr := string(body)

	// Transfer API error
	if strings.Contains(bodyStr, "\"code\"") &&
		!strings.Contains(bodyStr, "\"code\": \"Accepted\"") &&
		strings.Contains(string(body), "\"message\"") {
		var globusErr GlobusTransferError
		err := json.Unmarshal(body, &globusErr)
		if err == nil {
			return &globusErr
		}
	}

	// Generic error
	if strings.Contains(bodyStr, "GlobusError") {
		return &GlobusGenericError{Message: bodyStr}
	}

	// Check the status code
	switch response.StatusCode {
	case 200, 201:
		return nil
	default:
		return &GlobusGenericError{Message: bodyStr}
	}
}

type GlobusEvent struct {
	DataType    string `json:"DATA_TYPE"`
	Code        string `json:"code"`
	IsError     bool   `json:"is_error"`
	Description string `json:"description"`
	Details     string `json:"details"`
	Time        string `json:"time"`
}

func (c GlobusConnectServerManagerClient) get(resource string, values url.Values) ([]byte, error) {
	resourcePath := c.Url + "/" + resource
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus Connect Server Manager API: GET %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c GlobusConnectServerManagerClient) post(resource string, body io.Reader) ([]byte, error) {
	resourcePath := c.Url + "/" + resource
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus Connect Server Manager API: POST %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	req.Header.Set("Content-Type", "application/json")

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c GlobusConnectServerManagerClient) patch(resource string, body io.Reader) ([]byte, error) {
	resourcePath := c.Url + "/" + resource
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("Globus Connect Server Manager API: PATCH %s", res))
	req, err := http.NewRequest(http.MethodPatch, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.AccessToken))
	req.Header.Set("Content-Type", "application/json")

	var client http.Client
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

type GlobusManagerApiResult_1_1_0 struct {
	DataType string `json:"DATA_TYPE"` // always `result#1.0.0`
	//AuthorizationParameters any `json:"authorization_parameters"`
	Code string          `json:"code"`
	Data json.RawMessage `json:"data"`
	//Detail any `json:"detail"`
	//HasNextPage bool `json:"has_next_page"`
	HttpResponseCode int `json:"http_response_code"`
	//Marker string `json:"marker"`
	Message string `json:"message"`
}

type GlobusS3KeysPrefixPaths_1_0_0 struct {
	PathPrefixes []string `json:"path_prefixes"`
	S3KeyId      string   `json:"s3_key_id"`
	S3SecretKey  string   `json:"s3_secret_key"`
}
type GlobusS3UserCredentialPolicies_1_2_0 struct {
	DataType        string                          `json:"DATA_TYPE"` // always `s3_user_credential_policies#1.2.0`
	S3KeyId         string                          `json:"s3_key_id"`
	S3MultiKeys     []GlobusS3KeysPrefixPaths_1_0_0 `json:"s3_multi_keys,omitempty"`
	S3RequesterPays bool                            `json:"s3_requester_pays,omitempty"`
	S3SecretKey     string                          `json:"s3_secret_key"`
}
type GlobusUserCredentialRecord struct {
	DataType         string            `json:"DATA_TYPE"` // always `user_credential#1.0.0`
	ConnectorId      string            `json:"connector_id"`
	Deleted          bool              `json:"deleted"`
	DisplayName      string            `json:"display_name"`
	Id               string            `json:"id"`
	IdentityId       string            `json:"identity_id"`
	Invalid          bool              `json:"invalid"`
	Policies         []json.RawMessage `json:"policies"`
	Provisioned      bool              `json:"provisioned"`
	StorageGatewayId string            `json:"storage_gateway_id"`
	Username         string            `json:"username"`
}

func (m *GlobusConnectServerManagerClient) getStorageGatewayInfo() error {
	body, err := m.get("api/storage_gateways/", url.Values{})
	if err != nil {
		return err
	}
	var response GlobusManagerApiResult_1_1_0
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	slog.Debug(fmt.Sprintf("GCS Manager API response: %s (%s)", response.Message, response.Code))
	if response.HttpResponseCode != http.StatusOK {
		return errors.New(response.Message)
	}
	type GlobusStorageGateway_1_3_0 struct {
		ConnectorId string            `json:"connector_id"`
		DataType    string            `json:"DATA_TYPE"` // always `s3_storage_gateway#1.3.0`
		Id          string            `json:"id"`
		Policies    []json.RawMessage `json:"policies"`
	}
	var gateways []GlobusStorageGateway_1_3_0
	if err := json.Unmarshal(response.Data, &gateways); err != nil {
		return err
	}
	for _, g := range gateways {
		slog.Debug(fmt.Sprintf("Found storage gateway %s", g.Id))
		var gateway GlobusStorageGateway
		gateway.ConnectorId = uuid.MustParse(g.ConnectorId)
		gateway.Id = uuid.MustParse(g.Id)
		for p := range g.Policies {
			type GlobusS3StoragePolicies_1_3_0 struct {
				DataType   string `json:"DATA_TYPE"` // always `s3_storage_policies#1.3.0`
				S3Buckets  string `json:"s3_buckets"`
				S3Endpoint string `json:"s3_endpoint"`
			}
			var policy []GlobusS3StoragePolicies_1_3_0
			if err = json.Unmarshal(g.Policies[p], &policy); err != nil {
				continue
			}
			slog.Debug(fmt.Sprintf("Found S3 storage policy %s", g.Id))
			gateway.Providers = append(gateway.Providers, "s3")
		}
		m.StorageGateways = append(m.StorageGateways, gateway)
	}
	return nil
}

// NOTE: For now, we only allow a single S3 credential per user to be registered with a Globus
// NOTE: endpoint per user, using the user's ORCID.
func (m GlobusConnectServerManagerClient) addOrUpdateS3UserCredential(user auth.User, credential auth.Credential) (auth.Credential, error) {
	var record GlobusUserCredentialRecord
	var response GlobusManagerApiResult_1_1_0
	var found bool
	var payload, body []byte
	var err error

	if record, found, _ = m.findUserCredentialRecord(user); found {
		// Update the record with a new S3 policy, leaving other policies intact
		foundS3Policy := false
		slog.Debug("Looking for user S3 credential...")
		for i, policy := range record.Policies {
			var s3Policy GlobusS3UserCredentialPolicies_1_2_0
			err := json.Unmarshal(policy, &s3Policy)
			if err != nil { // not an S3 policy, move along
				continue
			}
			if s3Policy.S3KeyId == credential.Id && s3Policy.S3SecretKey == credential.Secret {
				// S3 policy is up to date -- nothing to do
				slog.Debug("BINGO")
				return credential, nil
			}

			// update the S3 policy in place
			s3Policy.S3KeyId = credential.Id
			s3Policy.S3SecretKey = credential.Secret
			if record.Policies[i], err = json.Marshal(s3Policy); err != nil {
				return auth.Credential{}, err
			}
			break
		}

		// If we didn't find an S3 policy attached to this record, append it.
		if !foundS3Policy {
			slog.Debug("S3 credential not found. Registering.")
			var newS3Policy []byte
			if newS3Policy, err = json.Marshal(GlobusS3UserCredentialPolicies_1_2_0{
				DataType:    "s3_user_credential_policies#1.2.0",
				S3KeyId:     credential.Id,
				S3SecretKey: credential.Secret,
			}); err != nil {
				return auth.Credential{}, err
			}
			record.Policies = append(record.Policies, newS3Policy)
		}

		if payload, err = json.Marshal(record); err != nil {
			return auth.Credential{}, err
		}
		if body, err = m.patch("api/user_credentials", bytes.NewReader(payload)); err != nil {
			return auth.Credential{}, err
		}
	} else {
		// No existing record -- create a new one.
		var newS3Policy []byte
		if newS3Policy, err = json.Marshal(GlobusS3UserCredentialPolicies_1_2_0{
			DataType:    "s3_user_credential_policies#1.2.0",
			S3KeyId:     credential.Id,
			S3SecretKey: credential.Secret,
		}); err != nil {
			return auth.Credential{}, err
		}

		// Find our S3-powered storage gateway.
		var connectorId, storageGatewayId uuid.UUID
		for _, gateway := range m.StorageGateways {
			for _, provider := range gateway.Providers {
				if provider == "s3" {
					storageGatewayId = gateway.Id
					connectorId = gateway.ConnectorId
					break
				}
			}
			if storageGatewayId != uuid.Nil {
				break
			}
		}

		record = GlobusUserCredentialRecord{
			DataType:         "user_credential#1.0.0",
			ConnectorId:      connectorId.String(),
			DisplayName:      user.Name,
			Id:               uuid.New().String(),
			IdentityId:       user.Orcid, // NOTE: user's ORCID is the credential identifier
			Policies:         []json.RawMessage{newS3Policy},
			Provisioned:      true, // NOTE: credential is fully provisioned programmatically
			StorageGatewayId: storageGatewayId.String(),
			Username:         credential.Username,
		}
		if payload, err = json.Marshal(record); err != nil {
			return auth.Credential{}, err
		}
		if body, err = m.post("api/user_credentials", bytes.NewReader(payload)); err != nil {
			return auth.Credential{}, err
		}
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return auth.Credential{}, err
	}
	if response.HttpResponseCode != http.StatusOK && response.HttpResponseCode != http.StatusCreated {
		return auth.Credential{}, errors.New(response.Message)
	}
	return credential, nil
}

func (m GlobusConnectServerManagerClient) findUserCredentialRecord(user auth.User) (GlobusUserCredentialRecord, bool, error) {
	for _, gateway := range m.StorageGateways {
		var response GlobusManagerApiResult_1_1_0
		values := url.Values{}
		values.Add("include", "all")
		values.Add("storage_gateway", gateway.Id.String())
		body, err := m.get(fmt.Sprintf("api/user_credential/%s", user.Orcid), url.Values{})
		if err != nil {
			return GlobusUserCredentialRecord{}, false, err
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return GlobusUserCredentialRecord{}, false, err
		}
		if response.HttpResponseCode != http.StatusOK && response.HttpResponseCode != http.StatusCreated {
			return GlobusUserCredentialRecord{}, false, errors.New(response.Message)
		}
		var existingCreds []GlobusUserCredentialRecord
		if err := json.Unmarshal(response.Data, &existingCreds); err != nil {
			return GlobusUserCredentialRecord{}, false, err
		}
		for _, existingCred := range existingCreds {
			if existingCred.IdentityId == user.Orcid {
				return existingCred, true, nil
			}
		}
	}
	return GlobusUserCredentialRecord{}, false, nil
}

const (
	globusTransferApiBaseUrl = "https://transfer.api.globusonline.org"
	globusTransferApiVersion = "v0.10"
)
