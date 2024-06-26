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

package jdp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/frictionless"
)

const (
	jdpBaseURL     = "https://files.jgi.doe.gov/"
	filePathPrefix = "/global/dna/dm_archive/" // directory containing JDP files
)

// a mapping from file suffixes to format labels
var suffixToFormat = map[string]string{
	"bam":      "bam",
	"bam.bai":  "bai",
	"blasttab": "blast",
	"bz":       "bzip",
	"bz2":      "bzip2",
	"csv":      "csv",
	"faa":      "fasta",
	"fasta":    "fasta",
	"fasta.gz": "fasta",
	"fastq":    "fastq",
	"fastq.gz": "fastq",
	"fna":      "fasta",
	"gff":      "gff",
	"gff3":     "gff3",
	"gz":       "gz",
	"html":     "html",
	"info":     "texinfo",
	"out":      "text",
	"pdf":      "pdf",
	"tar":      "tar",
	"tar.gz":   "tar",
	"tar.bz":   "tar",
	"tar.bz2":  "tar",
	"tsv":      "tsv",
	"txt":      "text",
}

// this gets populated automatically with the keys in suffixToFormat
var supportedSuffixes []string

// a mapping from file format labels to mime types
var formatToMimeType = map[string]string{
	"bam":   "application/octet-stream",
	"bai":   "application/octet-stream",
	"csv":   "text/csv",
	"fasta": "text/plain",
	"fastq": "text/plain",
	"gff":   "text/plain",
	"gff3":  "text/plain",
	"gz":    "application/gzip",
	"bz":    "application/x-bzip",
	"bz2":   "application/x-bzip2",
	"tar":   "application/x-tar",
	"text":  "text/plain",
}

// a mapping from the JDP's reported file types to mime types
// (backup method for determining mime types)
var fileTypeToMimeType = map[string]string{
	"text":     "text/plain",
	"fasta":    "text/plain",
	"fasta.gz": "application/gzip",
	"fastq":    "text/plain",
	"fastq.gz": "application/gzip",
	"tab":      "text/plain",
	"tar.gz":   "application/x-tar",
	"tar.bz":   "application/x-tar",
	"tar.bz2":  "application/x-tar",
}

// extracts the file format from the name and type of the file
func formatFromFileName(fileName string) string {
	// make a list of the supported suffixes if we haven't yet
	if supportedSuffixes == nil {
		supportedSuffixes = make([]string, 0)
		for suffix, _ := range suffixToFormat {
			supportedSuffixes = append(supportedSuffixes, suffix)
		}
	}

	// determine whether the file matches any of the supported suffixes,
	// selecting the longest matching suffix
	format := "unknown"
	longestSuffix := 0
	for _, suffix := range supportedSuffixes {
		if strings.HasSuffix(fileName, suffix) && len(suffix) > longestSuffix {
			format = suffixToFormat[suffix]
			longestSuffix = len(suffix)
		}
	}
	return format
}

// extracts the file format from the name and type of the file
func mimeTypeFromFormatAndTypes(format string, fileTypes []string) string {
	// try to match the file type to a mime type
	for _, fileType := range fileTypes {
		if mimeType, ok := fileTypeToMimeType[fileType]; ok {
			return mimeType
		}
	}
	// check the file format to see whether it matches a mime type
	if mimeType, ok := formatToMimeType[format]; ok {
		return mimeType
	}
	return ""
}

// extracts file type information from the given File
func fileTypesFromFile(file File) []string {
	// TODO: See https://pkg.go.dev/encoding/json?utm_source=godoc#example-RawMessage-Unmarshal
	// TODO: for an example of how to unmarshal a variant type.
	return []string{}
}

// extracts source information from the given metadata
func sourcesFromMetadata(md Metadata) []frictionless.DataSource {
	sources := make([]frictionless.DataSource, 0)
	piInfo := md.Proposal.PI
	if len(piInfo.LastName) > 0 {
		var title string
		if len(piInfo.FirstName) > 0 {
			title = fmt.Sprintf("%s, %s", piInfo.LastName, piInfo.FirstName)
			if len(piInfo.MiddleName) > 0 {
				title += fmt.Sprintf(" %s", piInfo.MiddleName)
			}
			if len(piInfo.Institution) > 0 {
				if len(piInfo.Country) > 0 {
					title += fmt.Sprintf(" (%s, %s)", piInfo.Institution, piInfo.Country)
				} else {
					title += fmt.Sprintf(" (%s)", piInfo.Institution)
				}
			} else if len(piInfo.Country) > 0 {
				title += fmt.Sprintf(" (%s)", piInfo.Country)
			}
		}
		var doiURL string
		if len(md.Proposal.AwardDOI) > 0 {
			doiURL = fmt.Sprintf("https://doi.org/%s", md.Proposal.AwardDOI)
		}
		source := frictionless.DataSource{
			Title: title,
			Path:  doiURL,
			Email: piInfo.EmailAddress,
		}
		sources = append(sources, source)
	}
	return sources
}

// creates a credit metadata item from JDP file metadata
func creditFromIdAndMetadata(id string, md Metadata) credit.CreditMetadata {
	crd := credit.CreditMetadata{
		Identifier:   id,
		ResourceType: "dataset",
	}

	crd.Dates = []credit.EventDate{
		credit.EventDate{
			Date:  md.Proposal.DateApproved,
			Event: "approval",
		},
	}
	pi := md.Proposal.PI
	crd.Contributors = []credit.Contributor{
		credit.Contributor{
			ContributorType: "Person",
			// ContributorId: nothing yet
			Name:       strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
			GivenName:  strings.TrimSpace(fmt.Sprintf("%s %s", pi.FirstName, pi.MiddleName)),
			FamilyName: strings.TrimSpace(pi.LastName),
			Affiliations: []credit.Organization{
				credit.Organization{
					OrganizationName: pi.Institution,
				},
			},
			ContributorRoles: "PI",
		},
	}
	return crd
}

// creates a Frictionless DataResource-savvy name for a file:
// * the filename is down-cased
// * all periods are replaced with dashes
// * if needed for uniqueness, a number suffix is added
var dataResourceNameExists_ map[string]bool = make(map[string]bool)

func dataResourceName(filename string) string {
	name := strings.ToLower(filename)
	lastDot := strings.LastIndex(name, ".")
	if lastDot != -1 {
		name = name[:lastDot]
	}
	for {
		if _, alreadyExists := dataResourceNameExists_[name]; alreadyExists {
			// look for a numeric suffix after '-'
			lastDash := strings.LastIndex(name, "-")
			if lastDash == -1 { // no suffix, can just add "-1"
				name += "-1"
			} else {
				suffix := name[lastDash+1:]
				num, err := strconv.Atoi(suffix)
				if err != nil { // not a number! Can add "-1"
					name += "-1"
				} else { // a number, so add 1 to it and replace the suffix
					name = name[:lastDash+1] + fmt.Sprintf("-%d", num+1)
				}
			}
		} else {
			break
		}
	}
	return name
}

// creates a DataResource from a File
func dataResourceFromFile(file File) frictionless.DataResource {
	id := "JDP:" + file.Id
	format := formatFromFileName(file.Name)
	fileTypes := fileTypesFromFile(file)
	sources := sourcesFromMetadata(file.Metadata)

	// we use relative file paths in accordance with the Frictionless
	// Data Resource specification
	filePath := filepath.Join(strings.TrimPrefix(file.Path, filePathPrefix), file.Name)

	return frictionless.DataResource{
		Id:        id,
		Name:      dataResourceName(file.Name),
		Path:      filePath,
		Format:    format,
		MediaType: mimeTypeFromFormatAndTypes(format, fileTypes),
		Bytes:     file.Size,
		Hash:      file.MD5Sum,
		Sources:   sources,
		Credit:    creditFromIdAndMetadata(id, file.Metadata),
	}
}

// file database appropriate for handling JDP searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// database identifier
	Id string
	// ORCID identifier for database proxy
	Orcid string
	// HTTP client that caches queries
	Client http.Client
	// shared secret used for authentication
	Secret string
	// SSO token used for interim JDP access
	SsoToken string
	// mapping from staging UUIDs to JDP restoration request ID
	StagingIds map[uuid.UUID]int
}

func NewDatabase(orcid string) (databases.Database, error) {
	if orcid == "" {
		return nil, fmt.Errorf("No ORCID ID was given")
	}

	// make sure we have a shared secret or an SSO token
	secret, haveSecret := os.LookupEnv("DTS_JDP_SECRET")
	if !haveSecret { // check for SSO token
		_, haveToken := os.LookupEnv("DTS_JDP_SSO_TOKEN")
		if !haveToken {
			return nil, fmt.Errorf("No shared secret or SSO token was found for JDP authentication")
		}
	}

	return &Database{
		Id:         "jdp",
		Orcid:      orcid,
		Secret:     secret,
		SsoToken:   os.Getenv("DTS_JDP_SSO_TOKEN"),
		StagingIds: make(map[uuid.UUID]int),
	}, nil
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(request *http.Request) {
	if len(db.Secret) > 0 { // use shared secret
		request.Header.Add("Authorization", fmt.Sprintf("Token %s_%s", db.Orcid, db.Secret))
	} else { // try SSO token
		request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", db.SsoToken))
	}
}

// performs a GET request on the given resource, returning the resulting
// response and error
func (db *Database) get(resource string, values url.Values) (*http.Response, error) {
	var u *url.URL
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = resource
	u.RawQuery = values.Encode()
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("GET: %s", res))
	req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
	return db.Client.Do(req)
}

// performs a POST request on the given resource, returning the resulting
// response and error
func (db *Database) post(resource string, body io.Reader) (*http.Response, error) {
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err != nil {
		return nil, err
	}
	u.Path = resource
	res := fmt.Sprintf("%v", u)
	slog.Debug(fmt.Sprintf("POST: %s", res))
	req, err := http.NewRequest(http.MethodPost, res, body)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")
	return db.Client.Do(req)
}

// this helper extracts files for the JDP /search GET query with given parameters
func (db *Database) filesFromSearch(params url.Values) (databases.SearchResults, error) {
	var results databases.SearchResults

	idEncountered := make(map[string]bool) // keep track of duplicates
	resp, err := db.get("search", params)
	if err != nil {
		return results, err
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return results, err
	}
	type JDPResults struct {
		Organisms []struct {
			Id    string `json:"id"`
			Files []File `json:"files"`
		} `json:"organisms"`
	}
	results.Resources = make([]frictionless.DataResource, 0)
	var jdpResults JDPResults
	err = json.Unmarshal(body, &jdpResults)
	if err != nil {
		return results, err
	}
	for _, org := range jdpResults.Organisms {
		resources := make([]frictionless.DataResource, 0)
		for _, file := range org.Files {
			res := dataResourceFromFile(file)
			if params.Has("extra") {
				extraFields := strings.Split(params.Get("extra"), ",")
				for _, field := range extraFields {
					if field == "project_id" {
						res.Extra = json.RawMessage(fmt.Sprintf(`{"project_id": "%s"}`, org.Id))
					} // FIXME: for now, we ignore all other requested extra fields
				}
			}
			if _, encountered := idEncountered[res.Id]; !encountered {
				resources = append(resources, res)
				idEncountered[res.Id] = true
			}
		}
		results.Resources = append(results.Resources, resources...)
	}
	return results, nil
}

// returns the page number and page size corresponding to the given Pagination
// parameters
func pageNumberAndSize(offset, maxNum int) (int, int) {
	pageNumber := 1
	pageSize := 100
	if offset > 0 {
		if maxNum == -1 {
			pageSize = offset
			pageNumber = 2
		} else {
			pageSize = maxNum
			pageNumber = offset/pageSize + 1
		}
	} else {
		if maxNum > 0 {
			pageSize = maxNum
		}
	}
	return pageNumber, pageSize
}

func (db Database) SpecificSearchParameters() map[string]interface{} {
	return map[string]interface{}{
		// see https://files.jgi.doe.gov/apidoc/#/GET/search_list
		"f":     []string{"ssr", "biosample", "project_id", "library"},   // search specific field
		"s":     []string{"name", "id", "title", "kingdom", "score.avg"}, // sort order
		"d":     []string{"asc", "desc"},                                 // sort direction (ascending/descending)
		"extra": []string{"project_id"},                                  // list of requested extra fields
	}
}

// checks JDP-specific search parameters and adds them to the given URL values
func (db Database) addSpecificSearchParameters(params map[string]json.RawMessage, p *url.Values) error {
	paramSpec := db.SpecificSearchParameters()
	for name, jsonValue := range params {
		switch name {
		case "f": // field-specific search
			var value string
			err := json.Unmarshal(jsonValue, &value)
			if err != nil {
				return fmt.Errorf("Invalid JDP search field given (must be string)")
			}
			acceptedValues := paramSpec["f"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return fmt.Errorf("Invalid JDP search field: %s", value)
			}
		case "s": // sort order
			var value string
			err := json.Unmarshal(jsonValue, &value)
			if err != nil {
				return fmt.Errorf("Invalid JDP sort order given (must be string)")
			}
			acceptedValues := paramSpec["s"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return fmt.Errorf("Invalid JDP sort order: %s", value)
			}
		case "d": // sort direction
			var value string
			err := json.Unmarshal(jsonValue, &value)
			if err != nil {
				return fmt.Errorf("Invalid JDP sort direction given (must be string)")
			}
			acceptedValues := paramSpec["d"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return fmt.Errorf("Invalid JDP sort direction: %s", value)
			}
		case "extra": // comma-separated additional fields requested
			var value string
			err := json.Unmarshal(jsonValue, &value)
			if err != nil {
				return fmt.Errorf("Invalid JDP requested extra field given (must be string)")
			}
			acceptedValues := paramSpec["extra"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return fmt.Errorf("Invalid requested extra field: %s", value)
			}
		}
	}
	return nil
}

func (db *Database) Search(params databases.SearchParameters) (databases.SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	if params.Status == databases.SearchFileStatusStaged {
		p.Add(`ff[file_status]`, "RESTORED")
	} else if params.Status == databases.SearchFileStatusUnstaged {
		p.Add(`ff[file_status]`, "PURGED")
	}
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	if params.Specific != nil {
		err := db.addSpecificSearchParameters(params.Specific, &p)
		if err != nil {
			return databases.SearchResults{}, err
		}
	}

	return db.filesFromSearch(p)
}

func (db *Database) Resources(fileIds []string) ([]frictionless.DataResource, error) {
	// For the moment, this implementation is a stopgap to save the JDP team
	// from implementing additional functionality for the DTS. Here we query JAMO
	// directly for information about files that correspond to the given set of
	// file IDs.
	strippedFileIds := make([]string, len(fileIds))
	for i, fileId := range fileIds {
		strippedFileIds[i] = strings.TrimPrefix(fileId, "JDP:")
	}
	jamoFiles, err := queryJamo(strippedFileIds)
	if err != nil {
		return nil, err
	}

	// translate from JAMOese to Frictionless-ese
	resources := make([]frictionless.DataResource, len(jamoFiles))
	for i, jamoFile := range jamoFiles {
		resources[i] = frictionless.DataResource{
			Id:     "JDP:" + jamoFile.Id,
			Name:   dataResourceName(jamoFile.FileName),
			Path:   filepath.Join(strings.TrimPrefix(jamoFile.FilePath, filePathPrefix), jamoFile.FileName),
			Format: jamoFile.Metadata.FileFormat,
			Bytes:  jamoFile.FileSize,
			Hash:   jamoFile.MD5Sum,
		}
		// fill in holes where we can and patch up discrepancies
		if resources[i].Format == "" {
			resources[i].Format = formatFromFileName(resources[i].Path)
		} else if resources[i].Format == "txt" {
			resources[i].Format = "text"
		}
		resources[i].MediaType = mimeTypeFromFormatAndTypes(resources[i].Format, []string{})
		// fill in credit metadata
		resources[i].Credit = credit.CreditMetadata{
			Identifier:   resources[i].Id,
			ResourceType: "dataset",
		}
	}
	return resources, err
}

func (db *Database) StageFiles(fileIds []string) (uuid.UUID, error) {
	var xferId uuid.UUID

	// construct a POST request to restore archived files with the given IDs
	type RestoreRequest struct {
		Ids        []string `json:"ids"`
		SendEmail  bool     `json:"send_email"`
		ApiVersion string   `json:"api_version"`
	}

	// strip "JDP:" off the file IDs (and remove those without this prefix)
	fileIdsWithoutPrefix := make([]string, 0)
	for _, fileId := range fileIds {
		if strings.HasPrefix(fileId, "JDP:") {
			fileIdsWithoutPrefix = append(fileIdsWithoutPrefix, fileId[4:])
		}
	}

	data, err := json.Marshal(RestoreRequest{
		Ids:        fileIdsWithoutPrefix,
		SendEmail:  false,
		ApiVersion: "2",
	})
	if err != nil {
		return xferId, err
	}

	// NOTE: The slash in the resource is all-important for POST requests to
	// NOTE: the JDP!!
	resp, err := db.post("request_archived_files/", bytes.NewReader(data))
	if err != nil {
		return xferId, err
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return xferId, err
	}
	type RestoreResponse struct {
		RequestId int `json:"request_id"`
	}

	var jdpResp RestoreResponse
	err = json.Unmarshal(body, &jdpResp)
	if err != nil {
		return xferId, err
	}
	slog.Debug(fmt.Sprintf("Requested archived files from JDP (request ID: %d)",
		jdpResp.RequestId))
	xferId = uuid.New()
	db.StagingIds[xferId] = jdpResp.RequestId
	return xferId, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	if restoreId, found := db.StagingIds[id]; found {
		resource := fmt.Sprintf("request_archived_files/requests/%d", restoreId)
		resp, err := db.get(resource, url.Values{})
		if err != nil {
			return databases.StagingStatusUnknown, err
		}
		defer resp.Body.Close()
		var body []byte
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return databases.StagingStatusUnknown, err
		}
		type JDPResult struct {
			Status string `json:"status"` // "new", "pending", or "ready"
		}
		var jdpResult JDPResult
		err = json.Unmarshal(body, &jdpResult)
		if err != nil {
			return databases.StagingStatusUnknown, err
		}
		statusForString := map[string]databases.StagingStatus{
			"new":     databases.StagingStatusActive,
			"pending": databases.StagingStatusActive,
			"ready":   databases.StagingStatusSucceeded,
		}
		if status, ok := statusForString[jdpResult.Status]; ok {
			return status, nil
		} else {
			return status, fmt.Errorf("Unrecognized staging status string: %s", jdpResult.Status)
		}
	} else {
		return databases.StagingStatusUnknown, nil
	}
}

func (db *Database) Endpoint() (endpoints.Endpoint, error) {
	return endpoints.NewEndpoint(config.Databases[db.Id].Endpoint)
}

func (db *Database) LocalUser(orcid string) (string, error) {
	// no current mechanism for this
	return "localuser", nil
}
