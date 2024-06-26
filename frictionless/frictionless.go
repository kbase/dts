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

package frictionless

import (
	"encoding/json"
	"strings"

	"github.com/kbase/dts/credit"
)

// a Frictionless data package describing a set of related resources
// (https://specs.frictionlessdata.io/data-package/)
type DataPackage struct {
	// the name of the data package
	Name string `json:"name"`
	// a list of resources that belong to the package
	Resources []DataResource `json:"resources"`
	// a list identifying the license or licenses under which this resource is
	// managed (optional)
	Licenses []DataLicense `json:"licenses,omitempty"`
	// a list identifying the sources for this resource (optional)
	Sources []DataSource `json:"sources,omitempty"`
}

// a Frictionless data resource describing a file in a search
// (https://specs.frictionlessdata.io/data-resource/)
type DataResource struct {
	// a unique identifier for the resource
	Id string `json:"id"`
	// the name of the resource's file, with any suffix stripped off
	Name string `json:"name"`
	// a relative path to the resource's file within a data package directory
	Path string `json:"path"`
	// a title or label for the resource (optional)
	Title string `json:"title,omitempty"`
	// a description of the resource (optional)
	Description string `json:"description,omitempty"`
	// indicates the format of the resource's file, often used as an extension
	Format string `json:"format"`
	// the mediatype/mimetype of the resource (optional, e.g. "test/csv")
	MediaType string `json:"media_type,omitempty"`
	// the character encoding for the resource's file (optional, default: UTF-8)
	Encoding string `json:"encoding,omitempty"`
	// the size of the resource's file in bytes
	Bytes int `json:"bytes"`
	// the hash for the resource's file (algorithms other than MD5 are indicated
	// with a prefix to the hash delimited by a colon)
	Hash string `json:"hash"`
	// a list identifying the sources for this resource (optional)
	Sources []DataSource `json:"sources,omitempty"`
	// a list identifying the license or licenses under which this resource is
	// managed (optional)
	Licenses []DataLicense `json:"licenses,omitempty"`
	// credit metadata associated with the resource (optional for now)
	Credit credit.CreditMetadata `json:"credit,omitempty"`
	// any other fields requested e.g. by a search query (optional, raw JSON object)
	Extra json.RawMessage `json:"extra,omitempty"`
}

// call this to get a string containing the name of the hashing algorithm used
// by the receiver
func (res DataResource) HashAlgorithm() string {
	colon := strings.Index(res.Hash, ":")
	if colon != -1 {
		return res.Hash[:colon]
	} else {
		return "md5"
	}
}

// information about the source of a DataResource
type DataSource struct {
	// a descriptive title for the source
	Title string `json:"title"`
	// a URI or relative path pointing to the source (optional)
	Path string `json:"path,omitempty"`
	// an email address identifying a contact associated with the source (optional)
	Email string `json:"email,omitempty"`
}

// information about a license associated with a DataResource
type DataLicense struct {
	// the abbreviated name of the license
	Name string `json:"name"`
	// a URI or relative path at which the license text may be retrieved
	Path string `json:"path"`
	// the descriptive title of the license (optional)
	Title string `json:"title,omitempty"`
}
