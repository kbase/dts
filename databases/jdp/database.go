package jdp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"dts/config"
	"dts/core"
	"dts/credit"
	"dts/endpoints"
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
	"fna":      "fna",
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
	"txt":   "text/plain",
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

	// determine whether the file matches any of the supported suffixes
	for _, suffix := range supportedSuffixes {
		if strings.HasSuffix(fileName, suffix) {
			return suffixToFormat[suffix]
		}
	}
	return "unknown"
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
func sourcesFromMetadata(md Metadata) []core.DataSource {
	sources := make([]core.DataSource, 0)
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
		source := core.DataSource{
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
			CreditName: strings.TrimSpace(fmt.Sprintf("%s, %s %s", pi.LastName, pi.FirstName, pi.MiddleName)),
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

// creates a DataResource from a File
func dataResourceFromFile(file File) core.DataResource {
	id := "JDP:" + file.Id
	format := formatFromFileName(file.Name)
	fileTypes := fileTypesFromFile(file)
	sources := sourcesFromMetadata(file.Metadata)

	// the resource name is the filename with any suffix stripped off
	name := file.Name
	for _, suffix := range supportedSuffixes {
		index := strings.LastIndex(name, suffix)
		if index > 0 {
			name = name[:index-1]
			break
		}
	}

	// we use relative file paths in accordance with the Frictionless
	// Data Resource specification
	filePath := filepath.Join(strings.ReplaceAll(file.Path, filePathPrefix, ""), file.Name)

	return core.DataResource{
		Id:        id,
		Name:      name,
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
// (implements the core.Database interface)
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

func NewDatabase(orcid string) (core.Database, error) {
	if orcid == "" {
		return nil, fmt.Errorf("No ORCID ID was given")
	}

	// make sure we have a shared secret
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

// performs a GET request on the given resource, returning the resulting
// response and error
func (db *Database) get(resource string, values url.Values) (*http.Response, error) {
	var u *url.URL
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err == nil {
		u.Path = resource
		u.RawQuery = values.Encode()
		res := fmt.Sprintf("%v", u)
		log.Printf("GET: %s", res)
		req, err := http.NewRequest(http.MethodGet, res, http.NoBody)
		if err == nil {
			if len(db.Secret) > 0 {
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s_%s", db.Orcid, db.Secret))
			} else { // try SSO token
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", db.SsoToken))
			}
			return db.Client.Do(req)
		}
	}
	return nil, err
}

// performs a POST request on the given resource, returning the resulting
// response and error
func (db *Database) post(resource string, body io.Reader) (*http.Response, error) {
	u, err := url.ParseRequestURI(jdpBaseURL)
	if err == nil {
		u.Path = resource
		res := fmt.Sprintf("%v", u)
		log.Printf("POST: %s", res)
		var req *http.Request
		req, err = http.NewRequest(http.MethodPost, res, body)
		if err == nil {
			if len(db.Secret) > 0 {
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s_%s", db.Orcid, db.Secret))
			} else { // try SSO token
				req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", db.SsoToken))
			}
			req.Header.Set("Content-Type", "application/json")
			return db.Client.Do(req)
		}
	}
	return nil, err
}

// this helper extracts files for the JDP /search GET query with given parameters
func (db *Database) filesFromSearch(params url.Values) (core.SearchResults, error) {
	var results core.SearchResults

	idEncountered := make(map[string]bool) // keep track of duplicates
	resp, err := db.get("search", params)
	if err == nil {
		defer resp.Body.Close()
		var body []byte
		body, err = io.ReadAll(resp.Body)
		if err == nil {
			type JDPResults struct {
				Organisms []struct {
					Files []File `json:"files"`
				} `json:"organisms"`
			}
			var jdpResults JDPResults
			results.Resources = make([]core.DataResource, 0)
			err = json.Unmarshal(body, &jdpResults)
			if err == nil {
				for _, org := range jdpResults.Organisms {
					resources := make([]core.DataResource, 0)
					for _, file := range org.Files {
						res := dataResourceFromFile(file)
						if _, encountered := idEncountered[res.Id]; !encountered {
							resources = append(resources, res)
							idEncountered[res.Id] = true
						}
					}
					results.Resources = append(results.Resources, resources...)
				}
			}
		}
	}
	return results, err
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

func (db *Database) Search(params core.SearchParameters) (core.SearchResults, error) {
	// we assume the JDP interface for ElasticSearch queries
	// (see https://files.jgi.doe.gov/apidoc/)
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)

	p := url.Values{}
	p.Add("q", params.Query)
	p.Add("p", strconv.Itoa(pageNumber))
	p.Add("x", strconv.Itoa(pageSize))

	return db.filesFromSearch(p)
}

// This method is broken and needs a feature implemented by the JDP team.
func (db *Database) Resources(fileIds []string) ([]core.DataResource, error) {
	// strip prefix from file IDs and index them so we can sort them and
	// restore their original order
	strippedIds := make([]string, len(fileIds))
	indexForId := make(map[string]int)
	{
		type IndexedId struct {
			Id    string
			Index int
		}
		indexedIds := make([]IndexedId, len(fileIds))
		for i, id := range fileIds {
			indexedIds[i] = IndexedId{
				Id:    id,
				Index: i,
			}
			strippedIds[i] = strings.ReplaceAll(id, "JDP:", "")
		}

		// sort the indexed IDs and create the mapping
		slices.SortFunc(indexedIds, func(a, b IndexedId) int {
			if a.Id < b.Id {
				return -1
			} else if a.Id == b.Id {
				return 0
			} else {
				return 1
			}
		})
		for _, iid := range indexedIds {
			indexForId[iid.Id] = iid.Index
		}
	}

	// perform the search
	type FileFilter struct {
		Ids []string `json:"_id"`
	}
	ff, err := json.Marshal(FileFilter{Ids: strippedIds})
	if err == nil {
		p := url.Values{}
		p.Add("ff", string(ff))
		p.Add("since", strconv.Itoa(0))
		results, err := db.filesFromSearch(p)

		// filter out results with mismatched IDs
		// (we should ask the JDP team to provide a feature to fetch metadata
		//  for JAMO IDs)
		resources := make([]core.DataResource, 0)
		for _, res := range results.Resources {
			if _, found := indexForId[res.Id]; found {
				resources = append(resources, res)
			}
		}

		if len(resources) != len(strippedIds) {
			panic("Couldn't fetch resources for given IDs")
		}

		// sort the results
		slices.SortFunc(resources, func(a, b core.DataResource) int {
			if a.Id < b.Id {
				return -1
			} else if a.Id == b.Id {
				return 0
			} else {
				return 1
			}
		})

		// remap the sorted results so that they correspond to the originally
		// requested IDs
		remappedResources := make([]core.DataResource, len(resources))
		for i, res := range resources {
			fmt.Printf("indexed ID index[%d of %d]: %d\n", i, len(resources), indexForId[res.Id])
			remappedResources[indexForId[res.Id]] = res
		}
		return remappedResources, err
	}
	return nil, err
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

	resp, err := db.post("request_archived_files", bytes.NewReader(data))
	if err == nil {
		defer resp.Body.Close()
		var body []byte
		body, err = io.ReadAll(resp.Body)
		if err == nil {
			type RestoreResponse struct {
				RequestId int `json:"request_id"`
			}

			var jdpResp RestoreResponse
			err = json.Unmarshal(body, &jdpResp)
			if err == nil {
				xferId = uuid.New()
				db.StagingIds[xferId] = jdpResp.RequestId
			}
		}
	}

	return xferId, err
}

func (db *Database) StagingStatus(id uuid.UUID) (core.StagingStatus, error) {
	if restoreId, found := db.StagingIds[id]; found {
		resource := fmt.Sprintf("request_archived_files/requests/%d", restoreId)
		resp, err := db.get(resource, url.Values{})
		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err == nil {
				type JDPResult struct {
					Status string `json:"status"` // "ready" or not
				}
				var jdpResult JDPResult
				err = json.Unmarshal(body, &jdpResult)
				if err == nil {
					statusForString := map[string]core.StagingStatus{
						"ready": core.StagingStatusSucceeded,
					}
					if status, ok := statusForString[jdpResult.Status]; ok {
						return status, nil
					} else {
						return status, fmt.Errorf("Unrecognized staging status string: %s", jdpResult.Status)
					}
				}
			}
		}
		return core.StagingStatusUnknown, err
	} else {
		return core.StagingStatusUnknown, nil
	}
}

func (db *Database) Endpoint() core.Endpoint {
	endpoint, _ := endpoints.NewEndpoint(config.Databases[db.Id].Endpoint)
	return endpoint
}
