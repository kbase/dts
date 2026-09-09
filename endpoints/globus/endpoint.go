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
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/endpoints"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

const (
	globusTransferApiBaseUrl = "https://transfer.api.globusonline.org"
	globusTransferApiVersion = "v0.10"
)

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
	return fmt.Sprintf("%s", e.Message)
}

type GlobusUserCredential struct {
	// Authenticated DTS user for whom Globus credential is (temporarily) registered
	User auth.User
	// (S3) Bucket associated with user transfer
	Bucket string
	// Globus unique credential identifier
	Id uuid.UUID
}

// this type satisfies the endpoints.Endpoint interface for Globus endpoints
type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID

	Paths struct {
		Base string
		Data string
	}

	// access tokens for Globus API
	AccessTokens struct {
		Transfers     string
		Https         string
		ServerManager string
	}

	// authentication stuff
	ClientId     uuid.UUID
	ClientSecret string

	// endpoint configuration
	Info EndpointInfo

	// registered user credentials (on behalf on which DTS performs transfers)
	// NOTE: keys are ORCIDs
	UserCredentials map[string]GlobusUserCredential
}

// configuration struct for Globus endpoints
type Config struct {
	Name       string          `yaml:"name"`
	Id         string          `yaml:"id"`
	Credential auth.Credential `yaml:"credential"`
	BasePath   string          `yaml:"base_path,omitempty" mapstructure:"base_path,omitempty"`
	DataPath   string          `yaml:"data_path,omitempty" mapstructure:"data_path,omitempty"`
}

// creates a new Globus endpoint using the given information
func NewEndpoint(config Config) (endpoints.Endpoint, error) {
	clientId, err := uuid.Parse(config.Credential.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid Globus client ID for credential '%s': %s (must be UUID)",
			config.Name, config.Credential.Id)
	}
	id, err := uuid.Parse(config.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID specified for Globus endpoint: %s", config.Id)
	}
	ep := &Endpoint{
		Name:         config.Name,
		Id:           id,
		ClientId:     clientId,
		ClientSecret: config.Credential.Secret,
	}

	// if needed, authenticate to obtain a Globus Transfer API access token
	var zeroId uuid.UUID
	if ep.ClientId != zeroId {
		ep.AccessTokens.Transfers, err = ep.authenticate(defaultXferScopes_)
		if err != nil {
			return ep, err
		}
	}

	if config.BasePath != "" {
		ep.Paths.Base = config.BasePath
	} else {
		ep.Paths.Base = "/"
	}
	ep.Paths.Data = config.DataPath

	// query the endpoint for its capabilities
	ep.Info, err = ep.getEndpointInfo(ep.Id)

	// if HTTPS PUT operations are supported, authenticate to obtain an HTTPS-specific access token
	if ep.Info.HttpsServer != "" {
		scope := fmt.Sprintf("https://auth.globus.org/scopes/%s/https", ep.Id.String())
		ep.AccessTokens.Https, err = ep.authenticate([]string{scope})
		if err != nil {
			return ep, err
		}
	}

	// Access the Globus Connect Server Manager API if it's available. This allows us to create
	// user credentials for premium connectors (e.g. S3).
	if ep.Info.GCSManagerUrl != "" {
		scope := "endpoint:administrator" // fancy!
		ep.AccessTokens.ServerManager, err = ep.authenticate([]string{scope})
		if err != nil {
			return ep, err
		}
	}

	return ep, err
}

// constructs a Globus endpoint from a configuration map
func EndpointConstructor(conf map[string]any) (endpoints.Endpoint, error) {
	// marshal the config map into JSON
	var globusConfig Config
	if err := mapstructure.Decode(conf, &globusConfig); err != nil {
		return nil, err
	}
	return NewEndpoint(globusConfig)
}

func (ep *Endpoint) Provider() string {
	return "globus"
}

func (ep *Endpoint) BasePath() string {
	return ep.Paths.Base
}

func (ep *Endpoint) DataPath() string {
	return ep.Paths.Data
}

func (ep *Endpoint) RegisterUser(user auth.User) error {
	if ep.Info.GCSManagerUrl == "" { // we're not authorized to access the server manager API
		return nil
	}
	// see https://docs.globus.org/globus-connect-server/v5.4/api/openapi_User_Credentials/#postUserCredential
	for provider, credential := range user.Credentials {
		switch provider {
		case "s3":
			return ep.registerS3UserCredential(user, credential)
		default:
		}
	}
	return nil
}

func (ep *Endpoint) DeregisterUser(user auth.User) error {
}

func (ep *Endpoint) FilesStaged(descriptors []map[string]any) (bool, error) {
	// find all the directories in which these files reside
	filesInDir := make(map[string][]string)
	for _, descriptor := range descriptors {
		dir, file := filepath.Split(descriptor["path"].(string))
		dir = filepath.Join(ep.DataPath(), dir)
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
		resourcePath := ep.globusTransferApiResource(fmt.Sprintf("operation/endpoint/%s/ls", ep.Id.String()))
		body, err := ep.get(resourcePath, values, &ep.AccessTokens.Transfers)
		if err != nil {
			switch lsErr := err.(type) {
			case *GlobusTransferError:
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

	resourcePath := ep.globusTransferApiResource("task_list")
	body, err := ep.get(resourcePath, url.Values{}, &ep.AccessTokens.Transfers)
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
	// NOTE: We don't check whether files are staged here, because the endpoint itself doesn't always
	// have a reliable staging check (e.g. JDP's private data is invisible to Globus directory
	// listings). Consequently, we assume that files are staged by the time this function is called.

	// obtain a submission ID
	submissionId, err := ep.getSubmissionId()
	if err != nil {
		return uuid.UUID{}, err
	}

	// Occasionally, Globus returns a zero-valued UUID (uuid.Nil) and no error (network burp?).
	// So we pause and resubmit in this case
	for submissionId == uuid.Nil {
		time.Sleep(time.Second)
		submissionId, err = ep.getSubmissionId()
		if err != nil {
			return uuid.UUID{}, err
		}
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
	resourcePath := ep.globusTransferApiResource(fmt.Sprintf("task/%s", id.String()))
	body, err := ep.get(resourcePath, url.Values{}, &ep.AccessTokens.Transfers)
	if err != nil {
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
		resourcePath := ep.globusTransferApiResource(fmt.Sprintf("task/%s/event_list", id.String()))
		body, err := ep.get(resourcePath, url.Values{}, &ep.AccessTokens.Transfers)
		if err != nil {
			// fine, we'll just use the "nice status"
			return endpoints.TransferStatus{}, errors.New(response.NiceStatusShortDescription)
		}
		var eventList EventList
		json.Unmarshal(body, &eventList)
		if response.NiceStatus == "AUTH" {
			// sometimes Globus throws an AUTH error here during a network burp, so we
			// ignore it and report a failed status check (after all, we can't get here
			// without AUTHing successfully!)
			for _, event := range eventList.Data {
				if event.IsError {
					slog.Debug(fmt.Sprintf("Globus task %s: status check failed with AUTH error below (probably bogus, ignoring): ", id.String()))
					slog.Debug(fmt.Sprintf("Globus task %s: %s (%s):\n%s", id.String(), event.Description, event.Code, event.Details))
				}
			}
		} else {
			// it's probably real, so traverse the event list
			return endpoints.TransferStatus{
				Code:                endpoints.TransferStatusFailed,
				Message:             descriptionFromEventList(eventList, response.NiceStatusShortDescription),
				NumFiles:            response.Files,
				NumFilesSkipped:     response.FilesSkipped,
				NumFilesTransferred: response.FilesTransferred,
			}, nil
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
	resourcePath := ep.globusTransferApiResource(fmt.Sprintf("task/%s/cancel", id.String()))
	_, err := ep.post(resourcePath, nil, &ep.AccessTokens.Transfers) // can take up to 10 ѕeconds!
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

// Performs an HTTPS PUT request on the endpoint, uploading the content of the given reader as
// the request body. Only supported if the Globus endpoint has an associated HTTPS server.
func (ep *Endpoint) PutFromReader(resource string, body io.Reader) error {
	if ep.Info.HttpsServer == "" {
		return fmt.Errorf("Globus endpoint '%s' does not support HTTPS operations", ep.Id.String())
	}
	httpsPath := ep.Info.HttpsServer + filepath.Join(ep.Paths.Base, ep.Paths.Data, resource)
	_, err := ep.put(httpsPath, body, &ep.AccessTokens.Https)
	return err
}

//-----------
// Internals
//-----------

// default client credentials grant scopes
var defaultXferScopes_ = []string{"urn:globus:auth:scope:transfer.api.globus.org:all"}

// returns an error capturing any Globus-related error in a response body, or nil if the response
// doesn't appear to be an error
func errorFromGlobusResponse(body []byte) error {
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

	return nil
}

func (ep Endpoint) globusTransferApiResource(resourceName string) string {
	return globusTransferApiBaseUrl + fmt.Sprintf("/%s/%s", globusTransferApiVersion, resourceName)
}

func (ep Endpoint) globusServerManagerApiResource(resourceName string) string {
	return ep.Info.GCSManagerUrl + fmt.Sprintf("/%s", resourceName)
}

// (re)authenticates with Globus using its client ID and secret to obtain an
// access token with consents for its relevant list of scopes
// (https://docs.globus.org/api/auth/reference/#client_credentials_grant)
// returns an access token corresponding to the given set of scopes
func (ep *Endpoint) authenticate(scopes []string) (string, error) {
	authUrl := "https://auth.globus.org/v2/oauth2/token"
	data := url.Values{}
	data.Set("scope", strings.Join(scopes, " "))
	data.Set("grant_type", "client_credentials")
	req, err := http.NewRequest(http.MethodPost, authUrl, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(ep.ClientId.String(), ep.ClientSecret)
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
		if len(authError.Description) > 0 {
			return "", fmt.Errorf("couldn't authenticate via Globus Auth API: %s; %s (%d)",
				authError.Error, authError.Description, resp.StatusCode)
		}
		return "", fmt.Errorf("couldn't authenticate via Globus Auth API: %s (%d)",
			authError.Error, resp.StatusCode)
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

// This helper sends the given HTTP request, parsing the response for
// Globus-style error codes/messages and handling the ones that can be
// handled automatically (e.g. consent/scope related errors). In any case,
// it returns a byte slice containing the body of the response or an
// error indicating failure.
func (ep *Endpoint) sendRequest(request *http.Request, accessToken *string) ([]byte, error) {
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
	err = errorFromGlobusResponse(body)
	if err != nil {
		if xferErr, ok := err.(*GlobusTransferError); ok {
			if xferErr.Code == "ConsentRequired" || xferErr.Code == "AuthenticationFailed" {
				// our token has expired or we're missing a required scope,
				// so reauthenticate
				var newAccessToken string
				if len(xferErr.RequiredScopes) > 0 {
					newAccessToken, err = ep.authenticate(xferErr.RequiredScopes)
				} else {
					newAccessToken, err = ep.authenticate(defaultXferScopes_)
				}
				if err != nil {
					return nil, err
				}
				*accessToken = newAccessToken
				// try the request again using the new access token
				request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", *accessToken))
				resp, err = client.Do(request)
				if err != nil {
					return nil, err
				}
				body, err = io.ReadAll(resp.Body)
				resp.Body.Close()
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
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) get(resourcePath string, values url.Values, accessToken *string) ([]byte, error) {
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", *accessToken))

	return ep.sendRequest(req, accessToken)
}

// Performs a PUT request on the given Globus resource with the given payload, handling any
// obvious errors and returning a byte slice containing the body of the response,
// and/or any unhandled error. Handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) put(resourcePath string, body io.Reader, accessToken *string) ([]byte, error) {
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("PUT: %s", res))
	req, err := http.NewRequest(http.MethodPut, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s&ep.AccessTokens.ServerManager)", *accessToken))

	return ep.sendRequest(req, accessToken)
}

// Performs a POST request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) post(resourcePath string, body io.Reader, accessToken *string) ([]byte, error) {
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("POST: %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", *accessToken))
	req.Header.Set("Content-Type", "application/json")

	return ep.sendRequest(req, accessToken)
}

// Performs a DELETE request on the given Globus resource, handling any obvious
// errors and returning a byte slice containing the body of the response,
// and/or any unhandled error.
// This method handles scope-related errors by reauthenticating as needed and
// retrying the operation. See https://docs.globus.org/api/flows/working-with-consents/
// for details on Globus scopes and consents.
func (ep *Endpoint) delete(resourcePath string, accessToken *string) ([]byte, error) {
	u, err := url.ParseRequestURI(resourcePath)
	if err != nil {
		return nil, err
	}
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("DELETE: %s", res))
	req, err := http.NewRequest(http.MethodDelete, res, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", *accessToken))
	req.Header.Set("Content-Type", "application/json")

	return ep.sendRequest(req, accessToken)
}

// https://docs.globus.org/api/transfer/task_submit/#get_submission_id
func (ep *Endpoint) getSubmissionId() (uuid.UUID, error) {
	var id uuid.UUID
	resourcePath := ep.globusTransferApiResource("submission_id")
	body, err := ep.get(resourcePath, url.Values{}, &ep.AccessTokens.Transfers)
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
func (ep *Endpoint) submitTransfer(destination endpoints.Endpoint,
	submissionId uuid.UUID, files []endpoints.FileTransfer) (uuid.UUID, error) {
	var xferId uuid.UUID

	// are the source and destination endpoints configured in a conflicting way?
	globusDestination := destination.(*Endpoint)
	if ep.Info.ForceVerify && globusDestination.Info.DisableVerify { // not allowed!
		return xferId, &endpoints.IncompatibleDestinationError{
			Source:              ep.Name,
			SourceProvider:      "globus",
			Destination:         globusDestination.Name,
			DestinationProvider: "globus",
			Message:             "Source endpoint forces checksum verification, but destination disables it.",
		}
	}

	// configure checksum settings based on destination endpoint info
	var verifyChecksum bool = true
	var syncLevel int = 3                     // transfer only if checksums don't match
	if globusDestination.Info.DisableVerify { // checksum verification disabled on endpoint
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
			SourcePath:        filepath.Join(ep.DataPath(), file.SourcePath),
			DestinationPath:   file.DestinationPath,
			ExternalChecksum:  checksum,
			ChecksumAlgorithm: checksumAlgorithm,
		}
	}

	// the destination is a Globus endpoint, right?
	gDestination, ok := destination.(*Endpoint)
	if !ok {
		return xferId, &endpoints.IncompatibleDestinationError{
			Source:              ep.Name,
			SourceProvider:      "globus",
			Destination:         "???",
			DestinationProvider: destination.Provider(),
			Message:             "destination is not a Globus endpoint",
		}
	}

	// submit the transfer request
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
	data, err := json.Marshal(SubmissionRequest{
		DataType:            "transfer",
		Id:                  submissionId.String(),
		Label:               "DTS",
		Data:                xferItems,
		DestinationEndpoint: gDestination.Id.String(),
		SourceEndpoint:      ep.Id.String(),
		SyncLevel:           syncLevel,
		VerifyChecksum:      verifyChecksum,
		FailOnQuotaErrors:   true,
	})
	if err != nil {
		return xferId, err
	}

	resourcePath := ep.globusTransferApiResource("transfer")
	body, err := ep.post(resourcePath, bytes.NewReader(data), &ep.AccessTokens.Transfers)
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

type EndpointInfo struct {
	DisableVerify bool   `json:"disable_verify"`  // true if checksums are not available
	ForceVerify   bool   `json:"force_verify"`    // true if checksums must be available
	HttpsServer   string `json:"https_server"`    // non-blank if HTTPS transfers are supported
	GCSManagerUrl string `json:"gcs_manager_url"` // non-blank if Manager operations are supported
}

func (ep *Endpoint) getEndpointInfo(id uuid.UUID) (EndpointInfo, error) {
	// query the endpoint for its capabilities
	resourcePath := ep.globusTransferApiResource(fmt.Sprintf("submission_id/%s", id.String()))
	body, err := ep.get(fmt.Sprintf(resourcePath, id), url.Values{}, &ep.AccessTokens.Transfers)
	if err != nil {
		return EndpointInfo{}, err
	}
	var endpointInfo EndpointInfo
	err = json.Unmarshal(body, &endpointInfo)
	return endpointInfo, err
}

type EventList struct {
	Data []Event `json:"DATA"`
}

type Event struct {
	DataType    string `json:"DATA_TYPE"`
	Code        string `json:"code"`
	IsError     bool   `json:"is_error"`
	Description string `json:"description"`
	Details     string `json:"details"`
	Time        string `json:"time"`
}

// traverses a Globus event list, producing an appropriate description of errors encountered,
// falling back to the given description if nothing can be gleaned
func descriptionFromEventList(events EventList, fallback string) string {
	missing_files := make(map[string]bool)
	inaccessible_files := make(map[string]bool)
	for _, event := range events.Data {
		if event.IsError {
			switch event.Code {
			case "FILE_NOT_FOUND", "PERMISSION_DENIED":
				type Details struct {
					Context []struct {
						Operation string `json:"operation,omitempty"`
						Path      string `json:"path,omitempty"`
					} `json:"context"`
					Error struct {
						Body     string `json:"body,omitempty"`
						Code     int    `json:"code,omitempty"`
						Endpoint string `json:"endpoint,omitempty"`
						Type     string `json:"type,omitempty"`
					}
				}
				var details Details
				if err := json.Unmarshal([]byte(event.Details), &details); err == nil {
					if len(details.Context) > 0 {
						if event.Code == "FILE_NOT_FOUND" {
							missing_files[details.Context[0].Path] = true
						} else { // PERMISSION_DENIED
							inaccessible_files[details.Context[0].Path] = true
						}
					}
				}
			default: // not sure what this is -- skip for now
			}
		}
	}

	// summarize events
	var message string
	if len(missing_files) > 0 {
		var files []string
		for file := range missing_files {
			files = append(files, file)
		}
		message += fmt.Sprintf("files not found: %s", strings.Join(files, ", "))
	}
	if len(inaccessible_files) > 0 {
		var files []string
		for file := range inaccessible_files {
			files = append(files, file)
		}
		message += fmt.Sprintf("permisssion denied: %s", strings.Join(files, ", "))
	}
	if len(message) > 0 {
		return message
	}
	return fallback
}

type ManagerApiResult_1_1_0 struct {
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

func (ep Endpoint) registerS3UserCredential(user auth.User, credential auth.Credential) error {
	globusCredential := GlobusUserCredential{
		User: user,
		Id:   uuid.New(),
	}

	// get the storage gateway ID for this endpoint / collection
	resourcePath := ep.globusServerManagerApiResource(fmt.Sprintf("api/collections/%s", ep.Id.String()))
	body, err := ep.get(resourcePath, url.Values{}, &ep.AccessTokens.ServerManager)
	if err != nil {
		return err
	}
	var response ManagerApiResult_1_1_0
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	if response.HttpResponseCode != http.StatusOK || response.HttpResponseCode != http.StatusCreated {
		return errors.New(response.Message)
	}
	type CollectionData struct {
		ConnectorId      uuid.UUID `json:"connector_id"`
		StorageGatewayId uuid.UUID `json:"storage_gateway_id"`
	}
	var collection CollectionData
	if err := json.Unmarshal(response.Data, &collection); err != nil {
		return err
	}

	// now request the creation of a user credential
	type S3KeysPrefixPaths_1_0_0 struct {
		PathPrefixes []string `json:"path_prefixes"`
		S3KeyId      string   `json:"s3_key_id"`
		S3SecretKey  string   `json:"s3_secret_key"`
	}
	type S3UserCredentialPolicies_1_2_0 struct {
		DataType        string                    `json:"DATA_TYPE"` // always `s3_user_credential_policies#1.2.0`
		S3KeyId         string                    `json:"s3_key_id"`
		S3MultiKeys     []S3KeysPrefixPaths_1_0_0 `json:"s3_multi_keys"`
		S3RequesterPays bool                      `json:"s3_requester_pays"`
		S3SecretKey     string                    `json:"s3_secret_key"`
	}
	type CreateS3CredentialRequestBody struct {
		DataType         string                           `json:"DATA_TYPE"` // always `user_credential#1.0.0`
		ConnectorId      string                           `json:"connector_id"`
		Deleted          bool                             `json:"deleted"`
		DisplayName      string                           `json:"display_name"`
		Id               string                           `json:"id"`
		IdentityId       string                           `json:"identity_id"`
		Invalid          bool                             `json:"invalid"`
		Policies         []S3UserCredentialPolicies_1_2_0 `json:"policies"`
		Provisioned      bool                             `json:"provisioned"`
		StorageGatewayId string                           `json:"storage_gateway_id"`
		Username         string                           `json:"username"`
	}
	data, err := json.Marshal(CreateS3CredentialRequestBody{
		DataType:    "user_credential#1.0.0",
		ConnectorId: collection.ConnectorId.String(),
		DisplayName: user.Name,
		Id:          globusCredential.Id.String(),
		IdentityId:  ep.ClientId.String(), // NOTE: DTS masquerades as the user for this transfer
		Policies: []S3UserCredentialPolicies_1_2_0{
			{
				DataType:    "s3_user_credential_policies#1.2.0",
				S3KeyId:     credential.Id,
				S3SecretKey: credential.Secret,
			},
		},
		Provisioned:      true, // NOTE: credential is fully provisioned programmatically
		StorageGatewayId: collection.StorageGatewayId.String(),
		Username:         credential.Username,
	})
	resourcePath = ep.globusServerManagerApiResource("api/user_credentials")
	body, err = ep.post(resourcePath, bytes.NewReader(data), &ep.AccessTokens.ServerManager)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	if response.HttpResponseCode != http.StatusOK || response.HttpResponseCode != http.StatusCreated {
		return errors.New(response.Message)
	}
	return nil
}

func (ep Endpoint) deregisterUserCredential(user auth.User, globusCredentialId uuid.UUID) error {
	resourcePath := ep.globusServerManagerApiResource(fmt.Sprintf("api/user_credentials/%s", globusCredentialId.String()))
	body, err := ep.delete(resourcePath, &ep.AccessTokens.ServerManager)
	if err != nil {
		return err
	}
	var response ManagerApiResult_1_1_0
	if err := json.Unmarshal(body, &response); err != nil {
		return err
	}
	if response.HttpResponseCode != http.StatusOK || response.HttpResponseCode != http.StatusCreated {
		return errors.New(response.Message)
	}
	delete(ep.UserCredentials, globusCredentialId.String())
	return nil
}
