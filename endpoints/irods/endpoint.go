// Copyright (c) 2025 The KBase Project and its Contributors
// Copyright (c) 2025 Cohere Consulting, LLC
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

package irods

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/irods/irods_client_s3_api"

	"github.com/kbase/dts/endpoints"
)

// This file implements an iRODS endpoint. Right now it's just a stub to identify iRODS endpoints,
// but in the near future, we can implement it with the iRODS Go client
// (https://github.com/cyverse/go-irodsclient)

// This type satisfies the endpoints.Endpoint interface for iRODS endpoints.
type Endpoint struct {
}

type Config struct {
	// iRODS host
	Hostname string `yaml:"hostname"`
	// port exposed by iRODS host for transfers
	Port int `yaml:"port"`
	// iRODS user who initiates transfers
	Username string `yaml:"username"`
	// password for iRODS user
	Password string `yaml:"password"`
	// zone for iRODS client
	Zone string `yaml:"zone"`
	// S3 proxy information for iRODS <-> S3 transfers
	S3Proxy struct {
		// S3 proxy host
		Hostname string `yaml:"hostname"`
		// S3 proxy port
		Port int `yaml:"port"`
	} `yaml:"s3_proxy"`
}

// creates a new local endpoint using the information supplied in the
// DTS configuration file under the given endpoint name
func NewEndpoint(config Config) (endpoints.Endpoint, error) {
	return &Endpoint{}, nil
}

// constructs a local endpoint from a configuration map
func EndpointConstructor(conf map[string]any) (endpoints.Endpoint, error) {
	return &Endpoint{}, nil
}

func (ep Endpoint) Provider() string {
	return "irods"
}

func (ep Endpoint) Root() string {
	return ""
}

func (ep *Endpoint) FilesStaged(descriptors []map[string]any) (bool, error) {
	return false, fmt.Errorf("endpoints.іrods.Endpoint.FilesStaged: not implemented")
}

func (ep *Endpoint) Transfers() ([]uuid.UUID, error) {
	return nil, fmt.Errorf("endpoints.іrods.Endpoint.Transfers: not implemented")
}

func (ep *Endpoint) Transfer(dst endpoints.Endpoint, files []endpoints.FileTransfer) (uuid.UUID, error) {

	return uuid.UUID{}, fmt.Errorf("endpoints.іrods.Endpoint.Transfer: not implemented")
}

func (ep *Endpoint) Status(id uuid.UUID) (endpoints.TransferStatus, error) {
	return endpoints.TransferStatus{}, fmt.Errorf("endpoints.іrods.Endpoint.Status: not implemented")
}

func (ep *Endpoint) Cancel(id uuid.UUID) error {
	return fmt.Errorf("endpoints.іrods.Endpoint.Cancel: not implemented")
}
