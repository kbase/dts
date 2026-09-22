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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/endpoints"
)

// This file implements a Globus endpoint. It uses the Globus Transfer API
// described at https://docs.globus.org/api/transfer/.

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
	Id_ uuid.UUID
	// Globus client
	Globus GlobusTransferClient

	Paths struct {
		Base string
		Data string
	}

	provider string
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
	id, err := uuid.Parse(config.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID specified for Globus endpoint: %s", config.Id)
	}
	globus, err := NewGlobusTransferClient(config.Credential, id)
	if err != nil {
		return nil, err
	}
	ep := &Endpoint{
		Name:   config.Name,
		Id_:    id,
		Globus: globus,
	}

	if config.BasePath != "" {
		ep.Paths.Base = config.BasePath
	} else {
		ep.Paths.Base = "/"
	}
	ep.Paths.Data = config.DataPath

	if ep.provider, err = ep.determineProvider(); err != nil {
		return nil, err
	}

	return ep, nil
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

func (ep Endpoint) Id() uuid.UUID {
	return ep.Id_
}

func (ep Endpoint) Provider() string {
	// A Globus endpoint can have a different provider via Globus Premium Connectors.
	return ep.provider
}

func (ep Endpoint) BasePath() string {
	return ep.Paths.Base
}

func (ep Endpoint) DataPath() string {
	return ep.Paths.Data
}

func (ep Endpoint) ConnectsWith(provider string) bool {
	switch provider {
	case "globus", "s3":
		return true
	default:
		return false
	}
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
	for dir, files := range filesInDir {
		globusFiles, err := ep.Globus.FilesInDirectory(dir)
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
		filesPresent := make(map[string]bool)
		for _, file := range globusFiles {
			filesPresent[file] = true
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
	return ep.Globus.TransferTasks()
}

func (ep *Endpoint) Transfer(user auth.User, destination endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	if _, isGlobus := destination.(*Endpoint); !isGlobus {
		return uuid.UUID{}, &endpoints.IncompatibleDestinationError{
			Source:              ep.Id().String(),
			SourceProvider:      ep.Provider(),
			Destination:         destination.Id().String(),
			DestinationProvider: destination.Provider(),
			Message:             "a premium Globus connector may be required",
		}
	}

	// NOTE: We don't check whether files are staged here, because the endpoint itself doesn't always
	// have a reliable staging check (e.g. JDP's private data is invisible to Globus directory
	// listings). Consequently, we assume that files are staged by the time this function is called.

	filesWithFullPath := make([]endpoints.FileTransfer, len(files))
	for i, file := range files {
		filesWithFullPath[i] = endpoints.FileTransfer{
			SourcePath:      filepath.Join(ep.DataPath(), file.SourcePath),
			DestinationPath: file.DestinationPath,
			Hash:            file.Hash,
			HashAlgorithm:   file.HashAlgorithm,
		}
	}

	// If this is a transfer between endpoints with different providers, register or fetch the
	// credential that allows them to connect.
	var credential auth.Credential
	if ep.Provider() != destination.Provider() {
		slog.Debug("Source and destination providers differ, registering credentials...")
		serverManager, err := ep.Globus.ConnectServerManagerClient()
		if err != nil {
			return uuid.UUID{}, err
		}
		if credential, err = serverManager.AddOrUpdateUserCredential(user, destination.Provider()); err != nil {
			return uuid.UUID{}, err
		}
	}

	return ep.Globus.Transfer(credential, ep.Id(), destination.Id(), filesWithFullPath)
}

// mapping of Globus status code strings to DTS status codes
var statusCodesForStrings = map[string]endpoints.TransferStatusCode{
	"ACTIVE":    endpoints.TransferStatusActive,
	"INACTIVE":  endpoints.TransferStatusInactive,
	"SUCCEEDED": endpoints.TransferStatusSucceeded,
	"FAILED":    endpoints.TransferStatusFailed,
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	taskStatus, err := ep.Globus.TaskStatus(id)
	if err != nil {
		return endpoints.TransferStatus{}, err
	}

	// check for an error condition in NiceStatus
	if taskStatus.NiceStatus != "" && taskStatus.NiceStatus != "OK" && taskStatus.NiceStatus != "Queued" {
		// get the event list for this task
		events, err := ep.Globus.TaskEvents(id)
		if err != nil {
			return endpoints.TransferStatus{}, err
		}
		if taskStatus.NiceStatus == "AUTH" {
			// sometimes Globus throws an AUTH error here during a network burp, so we
			// ignore it and report a failed status check (after all, we can't get here
			// without AUTHing successfully!)
			for _, event := range events {
				if event.IsError {
					slog.Debug(fmt.Sprintf("Globus task %s: status check failed with AUTH error below (probably bogus, ignoring): ", id.String()))
					slog.Debug(fmt.Sprintf("Globus task %s: %s (%s):\n%s", id.String(), event.Description, event.Code, event.Details))
				}
			}
		} else {
			// it's probably real, so traverse the event list
			return endpoints.TransferStatus{
				Code:                endpoints.TransferStatusFailed,
				Message:             descriptionFromEventList(events, taskStatus.NiceStatusShortDescription),
				NumFiles:            taskStatus.Files,
				NumFilesSkipped:     taskStatus.FilesSkipped,
				NumFilesTransferred: taskStatus.FilesTransferred,
			}, nil
		}
	}
	return endpoints.TransferStatus{
		Code:                statusCodesForStrings[taskStatus.Status],
		NumFiles:            taskStatus.Files,
		NumFilesSkipped:     taskStatus.FilesSkipped,
		NumFilesTransferred: taskStatus.FilesTransferred,
	}, nil
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	return ep.Globus.Cancel(id)
}

// Performs an HTTPS PUT request on the endpoint, uploading the content of the given reader as
// the request body. Only supported if the Globus endpoint has an associated HTTPS server.
func (ep *Endpoint) PutFromReader(resource string, body io.Reader) error {
	httpsClient, err := ep.Globus.HttpsClient(ep.Id())
	if err != nil {
		return err
	}
	absPath := filepath.Join(ep.Paths.Base, resource)
	return httpsClient.PutFile(absPath, body)
}

//-----------
// Internals
//-----------

func (ep *Endpoint) determineProvider() (string, error) {
	manager, err := ep.Globus.ConnectServerManagerClient()
	if err != nil {
		if _, notAvailable := err.(*GlobusConnectServerManagerNotAvailableError); notAvailable {
			// No Globus Connect Manager Server -- we are Globus only
			return "globus", nil
		}
		return "", err // something went wrong accessing the API
	}

	// sift through the storage policies on the manager's underlying storage gateway
	// NOTE: we assume only a single Globus premium connector is present, and we match the
	// first policy we find.
	policies, err := manager.StoragePolicies()
	slog.Debug(fmt.Sprintf("Storage gateway policies: %v", policies))
	if err != nil {
		return "", err
	}
	for _, policy := range policies {
		if policy == "s3" {
			return "s3", nil
		}
	}
	return "globus", nil
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
func descriptionFromEventList(events []GlobusEvent, fallback string) string {
	missing_files := make(map[string]bool)
	inaccessible_files := make(map[string]bool)
	for _, event := range events {
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
