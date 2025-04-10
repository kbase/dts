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
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
)

// file database appropriate for handling JDP searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// HTTP client that caches queries
	Client http.Client
	// shared secret used for authentication
	Secret string
	// mapping from staging UUIDs to JDP restoration request ID
	StagingRequests map[uuid.UUID]StagingRequest
}

type StagingRequest struct {
	// JDP staging request ID
	Id int
	// time of staging request (for purging)
	Time time.Time
}

func NewDatabase() (databases.Database, error) {
	// make sure we have a shared secret or an SSO token
	secret, haveSecret := os.LookupEnv("DTS_JDP_SECRET")
	if !haveSecret { // check for SSO token
		return nil, fmt.Errorf("No shared secret was found for JDP authentication")
	}

	// make sure we are using only a single endpoint
	if config.Databases["jdp"].Endpoint == "" {
		return nil, &databases.InvalidEndpointsError{
			Database: "jdp",
			Message:  "The JGI data portal should only have a single endpoint configured.",
		}
	}

	// NOTE: we can't enable HSTS for JDP requests at this time, because the
	// NOTE: server doesn't seem to support it. Maybe raise this issue with the
	// NOTE: team?
	return &Database{
		//Client:          databases.SecureHttpClient(),
		Secret:          secret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
	}, nil
}

func (db Database) SpecificSearchParameters() map[string]any {
	return map[string]any{
		// see https://files.jgi.doe.gov/apidoc/#/GET/search_list
		"d": []string{"asc", "desc"}, // sort direction (ascending/descending)
		"f": []string{"ssr", "biosample", "project_id", "library", // search specific field
			"img_taxon_oid"},
		"include_private_data": []int{0, 1},                                             // flag to include private data
		"s":                    []string{"name", "id", "title", "kingdom", "score.avg"}, // sort order
		"extra":                []string{"img_taxon_oid", "project_id"},                 // list of requested extra fields
	}
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
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

	// extract any requested "extra" metadata fields (and scrub them from params)
	var extraFields []string
	if p.Has("extra") {
		extraFields = strings.Split(p.Get("extra"), ",")
		p.Del("extra")
	}

	body, err := db.get("search", p)
	if err != nil {
		return databases.SearchResults{}, err
	}
	descriptors, err := descriptorsFromResponseBody(body, extraFields)
	return databases.SearchResults{
		Descriptors: descriptors,
	}, err
}

func (db *Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	// strip the "JDP:" prefix from our files and create a mapping from IDs to
	// their original order so we can hand back metadata accordingly
	strippedFileIds := make([]string, len(fileIds))
	indexForId := make(map[string]int)
	for i, fileId := range fileIds {
		strippedFileIds[i] = strings.TrimPrefix(fileId, "JDP:")
		indexForId[strippedFileIds[i]] = i
	}

	type MetadataRequest struct {
		Ids                []string `json:"ids"`
		Aggregations       bool     `json:"aggregations"`
		IncludePrivateData bool     `json:"include_private_data"`
	}
	data, err := json.Marshal(MetadataRequest{
		Ids:                strippedFileIds,
		Aggregations:       true,
		IncludePrivateData: true,
	})
	if err != nil {
		return nil, err
	}

	body, err := db.post("search/by_file_ids/", orcid, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	descriptors, err := descriptorsFromResponseBody(body, nil)
	if err != nil {
		return nil, err
	}

	// reorder the descriptors to match that of the requested file IDs
	descriptorsByFileId := make(map[string]map[string]any)
	for _, descriptor := range descriptors {
		descriptorsByFileId[descriptor["id"].(string)] = descriptor
	}
	for i, fileId := range fileIds {
		descriptors[i] = descriptorsByFileId[fileId]
	}
	return descriptors, nil
}

func (db *Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	var xferId uuid.UUID

	// construct a POST request to restore archived files with the given IDs
	type RestoreRequest struct {
		Ids                []string `json:"ids"`
		SendEmail          bool     `json:"send_email"`
		ApiVersion         string   `json:"api_version"`
		IncludePrivateData int      `json:"include_private_data"`
	}

	// strip "JDP:" off the file IDs (and remove those without this prefix)
	fileIdsWithoutPrefix := make([]string, 0)
	for _, fileId := range fileIds {
		if strings.HasPrefix(fileId, "JDP:") {
			fileIdsWithoutPrefix = append(fileIdsWithoutPrefix, fileId[4:])
		}
	}

	data, err := json.Marshal(RestoreRequest{
		Ids:                fileIdsWithoutPrefix,
		SendEmail:          false,
		ApiVersion:         "2",
		IncludePrivateData: 1, // we need this just in case!
	})
	if err != nil {
		return xferId, err
	}

	// NOTE: The slash in the resource is all-important for POST requests to
	// NOTE: the JDP!!
	body, err := db.post("request_archived_files/", orcid, bytes.NewReader(data))
	if err != nil {
		switch e := err.(type) {
		case *databases.ResourceNotFoundError:
			e.ResourceId = strings.Join(fileIds, ",")
		}
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
	slog.Debug(fmt.Sprintf("Requested %d archived files from JDP (request ID: %d)",
		len(fileIds), jdpResp.RequestId))
	xferId = uuid.New()
	db.StagingRequests[xferId] = StagingRequest{
		Id:   jdpResp.RequestId,
		Time: time.Now(),
	}
	return xferId, err
}

func (db *Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	db.pruneStagingRequests()
	if request, found := db.StagingRequests[id]; found {
		resource := fmt.Sprintf("request_archived_files/requests/%d", request.Id)
		body, err := db.get(resource, url.Values{})
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
		}
		return databases.StagingStatusUnknown, fmt.Errorf("Unrecognized staging status string: %s", jdpResult.Status)
	} else {
		return databases.StagingStatusUnknown, nil
	}
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db *Database) LocalUser(orcid string) (string, error) {
	// no current mechanism for this
	return "localuser", nil
}

func (db Database) Save() (databases.DatabaseSaveState, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(db.StagingRequests)
	if err != nil {
		return databases.DatabaseSaveState{}, err
	}
	return databases.DatabaseSaveState{
		Name: "jdp",
		Data: buffer.Bytes(),
	}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	enc := gob.NewDecoder(bytes.NewReader(state.Data))
	return enc.Decode(&db.StagingRequests)
}

//--------------------
// Internal machinery
//--------------------

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
		for suffix := range suffixToFormat {
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

// extracts source information from the given metadata
func sourcesFromMetadata(md Metadata) []any {
	sources := make([]any, 0)
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
		source := map[string]any{
			"title": title,
			"path":  doiURL,
			"email": piInfo.EmailAddress,
		}
		sources = append(sources, source)
	}
	return sources
}

// creates a Frictionless DataResource-savvy name for a file:
// * the name consists of lower case characters plus '.', '-', and '_'
// * all forbidden characters encountered in the filename are removed
// * a number suffix is added if needed to make the name unique
func dataResourceName(filename string) string {
	name := strings.ToLower(filename)

	// remove any file suffix
	lastDot := strings.LastIndex(name, ".")
	if lastDot != -1 {
		name = name[:lastDot]
	}

	// replace sequences of invalid characters with '_'
	for {
		isInvalid := func(c rune) bool {
			return !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '-' && c != '.'
		}
		start := strings.IndexFunc(name, isInvalid)
		if start >= 0 {
			nameRunes := []rune(name)
			end := start + 1
			for end < len(name) && isInvalid(nameRunes[end]) {
				end++
			}
			if end < len(name) {
				name = name[:start] + string('_') + name[end+1:]
			} else {
				name = name[:start] + string('_')
			}
		} else {
			break
		}
	}

	return name
}

// creates a Frictionless descriptor from a File
func descriptorFromOrganismAndFile(organism Organism, file File) map[string]any {
	id := "JDP:" + file.Id
	format := formatFromFileName(file.Name)
	sources := sourcesFromMetadata(file.Metadata)

	// we use relative file paths in accordance with the Frictionless
	// Data Resource specification
	filePath := filepath.Join(strings.TrimPrefix(file.Path, filePathPrefix), file.Name)

	pi := file.Metadata.Proposal.PI
	descriptor := map[string]any{
		"id":        id,
		"name":      dataResourceName(file.Name),
		"path":      filePath,
		"format":    format,
		"mediatype": mimetypeForFile(file.Name),
		"bytes":     file.Size,
		"hash":      file.MD5Sum,
		"credit": credit.CreditMetadata{
			Identifier:   id,
			ResourceType: "dataset",
			Titles: []credit.Title{
				{
					Title: organism.Title,
				},
			},
			Dates: []credit.EventDate{
				{
					Date:  file.Date,
					Event: "Created",
				},
				{
					Date:  file.AddedDate,
					Event: "Accepted",
				},
				{
					Date:  file.ModifiedDate,
					Event: "Updated",
				},
			},
			Publisher: credit.Organization{
				OrganizationId:   "ROR:04xm1d337",
				OrganizationName: "Joint Genome Institute",
			},
			RelatedIdentifiers: []credit.PermanentID{
				{
					Id:               file.Metadata.Proposal.DOI,
					Description:      "Proposal DOI",
					RelationshipType: "IsCitedBy",
				},
				{
					Id:               file.Metadata.Proposal.AwardDOI,
					Description:      "Awarded proposal DOI",
					RelationshipType: "IsCitedBy",
				},
			},
			Contributors: []credit.Contributor{
				{
					ContributorType: "Person",
					// ContributorId: nothing yet
					Name:       strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
					GivenName:  strings.TrimSpace(fmt.Sprintf("%s %s", pi.FirstName, pi.MiddleName)),
					FamilyName: strings.TrimSpace(pi.LastName),
					Affiliations: []credit.Organization{
						{
							OrganizationName: pi.Institution,
						},
					},
					ContributorRoles: "PI",
				},
			},
			Version: file.Date,
		},
	}
	if len(sources) > 0 {
		descriptor["sources"] = sources
	}
	return descriptor
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(orcid string, request *http.Request) {
	request.Header.Add("Authorization", fmt.Sprintf("Token %s_%s", orcid, db.Secret))
}

// performs a GET request on the given resource, returning the resulting
// response body and/or error
func (db *Database) get(resource string, values url.Values) ([]byte, error) {
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
	resp, err := db.Client.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200:
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	case 503:
		return nil, &databases.UnavailableError{
			Database: "jdp",
		}
	default:
		return nil, fmt.Errorf("An error occurred with the JDP database (%d)",
			resp.StatusCode)
	}
}

// performs a POST request on the given resource on behalf of the user with the
// given ORCID, returning the body of the resulting response (or an error)
func (db *Database) post(resource, orcid string, body io.Reader) ([]byte, error) {
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
	db.addAuthHeader(orcid, req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := db.Client.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200, 201, 204:
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	case 404:
		return nil, &databases.ResourceNotFoundError{
			Database: "JDP",
		}
	case 503:
		return nil, &databases.UnavailableError{
			Database: "jdp",
		}
	default:
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("An error occurred: %s", string(data))
	}
}

// this helper extracts files for the JDP /search GET query with given parameters
func descriptorsFromResponseBody(body []byte, extraFields []string) ([]map[string]any, error) {
	type JDPResults struct {
		Organisms []Organism `json:"organisms"`
	}
	var jdpResults JDPResults
	err := json.Unmarshal(body, &jdpResults)
	if err != nil {
		return nil, err
	}

	descriptors := make([]map[string]any, 0)

	for _, org := range jdpResults.Organisms {
		for _, file := range org.Files {
			descriptor := descriptorFromOrganismAndFile(org, file)

			// add any requested additional metadata
			if extraFields != nil {
				extras := make(map[string]any)
				for _, field := range extraFields {
					switch field {
					case "project_id":
						extras["project_id"] = org.Id
					case "img_taxon_oid":
						extras["img_taxon_oid"] = file.Metadata.IMG.TaxonOID
					}
				}
				descriptor["extra"] = extras
			}

			descriptors = append(descriptors, descriptor)
		}
	}
	return descriptors, nil
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

// checks JDP-specific search parameters and adds them to the given URL values
func (db Database) addSpecificSearchParameters(params map[string]any, p *url.Values) error {
	paramSpec := db.SpecificSearchParameters()
	for name, jsonValue := range params {
		var ok bool
		switch name {
		case "f": // field-specific search
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  "Invalid search field given (must be string)",
				}
			}
			acceptedValues := paramSpec["f"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  fmt.Sprintf("Invalid search field given: %s", value),
				}
			}
		case "s": // sort order
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  "Invalid JDP sort order given (must be string)",
				}
			}
			acceptedValues := paramSpec["s"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  fmt.Sprintf("Invalid JDP sort order: %s", value),
				}
			}
		case "d": // sort direction
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  "Invalid JDP sort direction given (must be string)",
				}
			}
			acceptedValues := paramSpec["d"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  fmt.Sprintf("Invalid JDP sort direction: %s", value),
				}
			}
		case "include_private_data": // search for private data
			var value int
			if value, ok = jsonValue.(int); !ok {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  "Invalid flag given for include_private_data (must be 0 or 1)",
				}
			}
			p.Add(name, fmt.Sprintf("%d", value))
		case "extra": // comma-separated additional fields requested
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  "Invalid JDP requested extra field given (must be comma-delimited string)",
				}
			}
			acceptedValues := paramSpec["extra"].([]string)
			if slices.Contains(acceptedValues, value) {
				p.Add(name, value)
			} else {
				return &databases.InvalidSearchParameter{
					Database: "JDP",
					Message:  fmt.Sprintf("Invalid requested extra field: %s", value),
				}
			}
		default:
			return &databases.InvalidSearchParameter{
				Database: "JDP",
				Message:  fmt.Sprintf("Unrecognized JDP-specific search parameter: %s", name),
			}
		}
	}
	return nil
}

func (db *Database) pruneStagingRequests() {
	deleteAfter := time.Duration(config.Service.DeleteAfter) * time.Second
	for uuid, request := range db.StagingRequests {
		requestAge := time.Since(request.Time)
		if requestAge > deleteAfter {
			delete(db.StagingRequests, uuid)
		}
	}
}

func mimetypeForFile(filename string) string {
	mimetype := mime.TypeByExtension(filepath.Ext(filename))
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	return mimetype
}
