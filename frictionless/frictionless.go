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
	// list of contributors to the data package
	Contributors []Contributor `json:"contributors,omitempty"`
	// a timestamp indicated when the package was created
	Created string `json:"created,omitempty"`
	// a Markdown description of the data package
	Description string `json:"description,omitempty"`
	// a URL for a web address related to the data package
	Homepage string `json:"homepage,omitempty"`
	// an image to use for this data package (URL or POSIX path)
	Image string `json:"image,omitempty"`
	// a machine-readable set of instructions for processing
	Instructions json.RawMessage `json:"instructions,omitempty"`
	// an array of string keywords to assist users searching for the data package
	// in catalogs
	Keywords []string `json:"keywords,omitempty"`
	// a list identifying the license or licenses under which this resource is
	// managed (optional)
	Licenses []DataLicense `json:"licenses,omitempty"`
	// the name of the data package
	Name string `json:"name"`
	// the profile of this descriptor per the DataPackage profiles specification
	// (https://specs.frictionlessdata.io/profiles/#language)
	Profile string `json:"profile,omitempty"`
	// a list of resources that belong to the package
	Resources []DataResource `json:"resources"`
	// a list identifying the sources for this resource (optional)
	Sources []DataSource `json:"sources,omitempty"`
	// a title or one sentence description for the data package
	Title string `json:"title,omitempty"`
	// a version string identifying the version of the data package, conforming to
	// semantic versioning if relevant
	Version string `json:"version,omitempty"`
}

// a Frictionless data resource describing a file in a search
// (https://specs.frictionlessdata.io/data-resource/)
type DataResource struct {
	// the size of the resource's file in bytes
	Bytes int `json:"bytes"`
	// credit metadata associated with the resource (optional for now)
	Credit credit.CreditMetadata `json:"credit,omitempty"`
	// a description of the resource (optional)
	Description string `json:"description,omitempty"`
	// the character encoding for the resource's file (optional, default: UTF-8)
	Encoding string `json:"encoding,omitempty"`
	// any other fields requested e.g. by a search query (optional, raw JSON object)
	Extra json.RawMessage `json:"extra,omitempty"`
	// indicates the format of the resource's file, often used as an extension
	Format string `json:"format"`
	// the hash for the resource's file (algorithms other than MD5 are indicated
	// with a prefix to the hash delimited by a colon)
	Hash string `json:"hash"`
	// a unique identifier for the resource
	Id string `json:"id"`
	// a list identifying the license or licenses under which this resource is
	// managed (optional)
	Licenses []DataLicense `json:"licenses,omitempty"`
	// the mediatype/mimetype of the resource (optional, e.g. "test/csv")
	MediaType string `json:"media_type,omitempty"`
	// the name of the resource's file, with any suffix stripped off
	Name string `json:"name"`
	// a relative path to the resource's file within a data package directory
	Path string `json:"path"`
	// a list identifying the sources for this resource (optional)
	Sources []DataSource `json:"sources,omitempty"`
	// a title or label for the resource (optional)
	Title string `json:"title,omitempty"`
	// the name of the endpoint at which this resource is accessed (not exposed to JSON)
	Endpoint string
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
	// an email address identifying a contact associated with the source (optional)
	Email string `json:"email,omitempty"`
	// a URI or relative path pointing to the source (optional)
	Path string `json:"path,omitempty"`
	// a descriptive title for the source
	Title string `json:"title"`
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

// information about a contributor to a DataPackage
type Contributor struct {
	// the contributor's email address
	Email string `json:"email"`
	// a string describing the contributor's organization
	Organization string `json:"organization"`
	// a fully qualified http URL pointing to a relevant location online for the
	// contributor
	Path string `json:"path"`
	// the role of the contributor ("author", "publisher", "maintainer",
	// "wrangler", "contributor")
	Role string `json:"role"`
	// name/title of the contributor (name for person, name/title of organization)
	Title string `json:"title"`
}
