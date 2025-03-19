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

package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/endpoints"
)

type xferRecord struct {
	Status   endpoints.TransferStatus
	Files    []endpoints.FileTransfer
	Canceled bool
}

// This type implements an endpoint that moves files around on a local
// file system. It's used only for testing.
type Endpoint struct {
	// descriptive endpoint name (obtained from config)
	Name string
	// endpoint UUID (obtained from config)
	Id uuid.UUID
	// root directory for endpoint (default: current working directory)
	root string
	// transfers in progress
	Xfers map[uuid.UUID]xferRecord
}

// creates a new local endpoint using the information supplied in the
// DTS configuration file under the given endpoint name
func NewEndpoint(endpointName string) (endpoints.Endpoint, error) {
	epConfig, found := config.Endpoints[endpointName]
	if !found {
		return nil, fmt.Errorf("'%s' is not an endpoint", endpointName)
	}
	if epConfig.Provider != "local" {
		return nil, fmt.Errorf("'%s' is not a local endpoint", endpointName)
	}
	if epConfig.Root == "" {
		return nil, fmt.Errorf("'%s' requires a root directory to be specified", endpointName)
	}

	ep := &Endpoint{
		Name:  epConfig.Name,
		Id:    epConfig.Id,
		Xfers: make(map[uuid.UUID]xferRecord),
	}
	err := ep.setRoot(epConfig.Root)
	return ep, err
}

// sets the root directory for the local endpoint after checking that it exists
func (ep *Endpoint) setRoot(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		ep.root = dir
	}
	return err
}

func (ep *Endpoint) Root() string {
	return ep.root
}

func (ep *Endpoint) FilesStaged(files []interface{}) (bool, error) {
	for _, resource := range files {
		descriptor := resource.(map[string]interface{})
		absPath := filepath.Join(ep.root, descriptor["path"].(string))
		_, err := os.Stat(absPath)
		if err != nil {
			return false, nil
		}
	}
	return true, nil
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	xfers := make([]uuid.UUID, 0)
	for xferId, xfer := range ep.Xfers {
		switch xfer.Status.Code {
		case endpoints.TransferStatusSucceeded, endpoints.TransferStatusFailed:
		default:
			xfers = append(xfers, xferId)
		}
	}
	return xfers, nil
}

// implements asynchronous local file transfers and validation
func (ep *Endpoint) transferFiles(xferId uuid.UUID, dest endpoints.Endpoint) {
	var err error
	xfer := ep.Xfers[xferId]
	for _, file := range xfer.Files {
		// has the transfer been canceled?
		if xfer.Canceled {
			break
		}

		sourcePath := filepath.Join(ep.Root(), file.SourcePath)
		destPath := filepath.Join(dest.Root(), file.DestinationPath)

		// check for the source directory
		sourceDir := filepath.Dir(sourcePath)
		var sourceDirInfo os.FileInfo
		sourceDirInfo, err = os.Stat(sourceDir)
		if err != nil {
			break
		}

		// create the destination directory if needed
		destDir := filepath.Dir(destPath)
		_, err = os.Stat(destDir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) { // destination dir doesn't exist
				os.MkdirAll(destDir, sourceDirInfo.Mode())
			} else { // something else happened
				break
			}
		}

		// copy the file into place
		var data []byte
		var sourceFileInfo os.FileInfo
		sourceFileInfo, err = os.Stat(sourcePath)
		if err != nil {
			break
		}
		data, err = os.ReadFile(sourcePath)
		if err != nil {
			break
		}
		err = os.WriteFile(destPath, data, sourceFileInfo.Mode())
		if err != nil {
			break
		}
		xfer.Status.NumFilesTransferred++
		continue
	}
	if err != nil { // trouble!
		xfer.Status.Code = endpoints.TransferStatusFailed
	} else if xfer.Canceled {
		xfer.Status.Code = endpoints.TransferStatusFailed
	} else { // all's well
		xfer.Status.Code = endpoints.TransferStatusSucceeded
	}
	ep.Xfers[xferId] = xfer
}

func (ep *Endpoint) Transfer(dst endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {
	var xferId uuid.UUID
	_, ok := dst.(*Endpoint)
	if !ok {
		return xferId, fmt.Errorf("Cannot transfer files between a local endpoint and another type of endpoint!")
	}

	// first, we check that all requested files are staged on this endpoint
	requestedFiles := make([]interface{}, len(files))
	for i, file := range files {
		requestedFiles[i] = map[string]interface{}{
			"path": file.SourcePath, // only the Path field is required
		}
	}
	staged, err := ep.FilesStaged(requestedFiles)
	if err != nil {
		return xferId, err
	}
	if staged {
		// assign a UUID to the transfer and set it going
		xferId := uuid.New()
		ep.Xfers[xferId] = xferRecord{
			Status: endpoints.TransferStatus{
				Code:                endpoints.TransferStatusActive,
				NumFiles:            len(files),
				NumFilesTransferred: 0,
			},
			Files: files,
		}
		go ep.transferFiles(xferId, dst)
		return xferId, nil
	}
	return xferId, fmt.Errorf("The files requested for transfer are not yet staged.")
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	if xfer, found := ep.Xfers[id]; found {
		return xfer.Status, nil
	}
	return endpoints.TransferStatus{
		Code: endpoints.TransferStatusUnknown,
	}, fmt.Errorf("Transfer %s not found!", id.String())
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	if xfer, found := ep.Xfers[id]; found {
		xfer.Canceled = true
		return nil
	}
	return fmt.Errorf("Transfer %s not found!", id.String())
}

// this method is specific to local endpoints and gives access to the
// local filesystem
func (ep *Endpoint) FS() (fs.FS, error) {
	return os.DirFS(filepath.Join("/", ep.root)), nil
}
