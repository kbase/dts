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

package nmdc

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
)

// file database appropriate for handling searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// Base URL for NMDC API
	BaseURL string
	// HTTP client that caches queries
	Client http.Client
	// authorization info
	Auth authorization
	// mapping of host URLs to endpoints
	EndpointForHost map[string]string
}

type Config struct {
	// Credentials for database
	Credential auth.Credential `yaml:"credential" mapstructure:"credential"`
	// NMDC-compatible endpoints
	Endpoints struct {
		Nersc string `yaml:"nersc" mapstructure:"nersc"`
		Emsl  string `yaml:"emsl" mapstructure:"emsl"`
	} `yaml:"endpoints" mapstructure:"endpoints"`
	// Base URL for NMDC API
	BaseURL string `yaml:"base_url,omitempty" mapstructure:"base_url,omitempty"`
}

func NewDatabase(conf Config) (databases.Database, error) {

	if conf.Credential.Id == "" {
		return nil, &databases.UnauthorizedError{
			Database: "nmdc",
			Message:  "No NMDC user was provided for authentication",
		}
	}
	if conf.Credential.Secret == "" {
		return nil, &databases.UnauthorizedError{
			Database: "nmdc",
			Message:  "No NMDC password was provided for authentication",
		}
	}

	// make sure the endpoints are valid
	if !endpoints.EndpointExists(conf.Endpoints.Nersc) {
		return nil, &databases.InvalidEndpointsError{
			Database: "nmdc",
			Message:  fmt.Sprintf("Nersc endpoint '%s' is not configured", conf.Endpoints.Nersc),
		}
	}
	if !endpoints.EndpointExists(conf.Endpoints.Emsl) {
		return nil, &databases.InvalidEndpointsError{
			Database: "nmdc",
			Message:  fmt.Sprintf("Emsl endpoint '%s' is not configured", conf.Endpoints.Emsl),
		}
	}

	// fetch functional endpoint names and map URLs to them
	// (see https://nmdc-documentation.readthedocs.io/en/latest/howto_guides/globus.html)
	baseUrl := defaultBaseApiURL
	if conf.BaseURL != "" {
		baseUrl = conf.BaseURL
	}
	db := &Database{
		BaseURL: baseUrl,
		Client:  databases.SecureHttpClient(time.Second * 120), // prevent HTTPS -> HTTP redirect!
		EndpointForHost: map[string]string{
			"https://data.microbiomedata.org/data/": conf.Endpoints.Nersc,
			"https://nmdcdemo.emsl.pnnl.gov/":       conf.Endpoints.Emsl,
		},
	}

	// get an API access token
	auth, err := db.getAccessToken(conf.Credential)
	if err != nil {
		return nil, err
	}
	db.Auth = auth

	return db, nil
}

func DatabaseConstructor(conf map[string]any) func() (databases.Database, error) {
	return func() (databases.Database, error) {
		var nmdcConf Config
		if err := mapstructure.Decode(conf, &nmdcConf); err != nil {
			return nil, err
		}
		return NewDatabase(nmdcConf)
	}
}

func (db Database) SpecificSearchParameters() map[string]any {
	// for details about NMDC-specific search parameters, see
	// https://api.microbiomedata.org/docs#/find:~:text=Find%20NMDC-,metadata,-entities.
	return map[string]any{
		"activity_id":    "",
		"data_object_id": "",
		"fields":         []string{},
		"filter":         "",
		"sort":           "",
		"sample_id":      "",
		"extra":          []string{},
	}
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
	if err := db.renewAccessTokenIfExpired(); err != nil {
		return databases.SearchResults{}, err
	}

	p := url.Values{}

	// fetch pagination parameters
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)
	p.Add("page", strconv.Itoa(pageNumber))
	p.Add("per_page", strconv.Itoa(pageSize))

	// add any NMDC-specific search parameters
	if params.Specific != nil {
		err := db.addSpecificSearchParameters(params.Specific, &p)
		if err != nil {
			return databases.SearchResults{}, err
		}
	}

	// NOTE: NMDC doesn't do "search" at the moment, so we interpret a query as
	// NOTE: the study with which desired data objects are associated
	if params.Query == "" {
		return databases.SearchResults{}, &databases.InvalidSearchQuery{
			Database: "nmdc",
			Message:  "no query provided",
		}
	} else if !isValidStudyId(params.Query) {
		return databases.SearchResults{}, &databases.InvalidSearchQuery{
			Database: "nmdc",
			Message:  fmt.Sprintf("invalid study ID: %s", params.Query),
		}
	}
	studyId := params.Query
	descriptors, err := db.createDataObjectDescriptorsForStudy(studyId)
	if err != nil {
		return databases.SearchResults{}, err
	}
	return databases.SearchResults{
		Descriptors: descriptors,
	}, err
}

func (db Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	if err := db.renewAccessTokenIfExpired(); err != nil {
		return nil, err
	}

	studyIds, dataObjectIds, err := parseFileIds(fileIds)
	if err != nil {
		return nil, err
	}

	// fetch metadata by study

	dataObjectIdsByStudy := make(map[string][]string)
	for i, studyId := range studyIds {
		if _, found := dataObjectIdsByStudy[studyId]; !found {
			dataObjectIdsByStudy[studyId] = append(dataObjectIdsByStudy[studyId], dataObjectIds[i])
		}
	}

	var descriptors []map[string]any
	for studyId, dataObjectIds := range dataObjectIdsByStudy {
		// fetch data objects in batches using MongoDB $in filter via nmdcschema endpoint
		const batchSize = 200
		dataObjects := make([]DataObject, 0, len(dataObjectIds))
		for start := 0; start < len(dataObjectIds); start += batchSize {
			end := start + batchSize
			if end > len(dataObjectIds) {
				end = len(dataObjectIds)
			}
			batch := dataObjectIds[start:end]

			idsJSON, err := json.Marshal(batch)
			if err != nil {
				return nil, err
			}
			filter := fmt.Sprintf(`{"id":{"$in":%s}}`, string(idsJSON))

			// paginate through results for this batch
			pageToken := ""
			for {
				params := url.Values{}
				params.Set("filter", filter)
				params.Set("max_page_size", strconv.Itoa(batchSize))
				if pageToken != "" {
					params.Set("page_token", pageToken)
				}

				body, err := db.get("nmdcschema/data_object_set", params)
				if err != nil {
					return nil, err
				}

				var result struct {
					Resources     []DataObject `json:"resources"`
					NextPageToken *string      `json:"next_page_token"`
				}
				if err := json.Unmarshal(body, &result); err != nil {
					return nil, err
				}
				dataObjects = append(dataObjects, result.Resources...)

				if result.NextPageToken == nil || *result.NextPageToken == "" {
					break
				}
				pageToken = *result.NextPageToken
			}
		}

		// fetch credit metadata associated with the study
		credit, err := db.creditMetadataForStudy(studyId)
		if err != nil {
			return nil, err
		}

		for _, dataObject := range dataObjects {
			dataObject.StudyId = studyId
			descriptors = append(descriptors, db.createDataObjectDescriptor(dataObject, credit))
		}
	}

	return descriptors, nil
}

func (db Database) EndpointNames() []string {
	var endpoints []string
	for _, endpoint := range db.EndpointForHost {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

func (db Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	// NMDC keeps all of its NERSC data on disk, so all files are already staged.
	// We simply generate a new UUID that can be handed to db.StagingStatus,
	// which returns databases.StagingStatusSucceeded.
	//
	// "We may eventually use tape but don't need to yet." -Shreyas Cholia, 2024-09-04
	return uuid.New(), nil
}

func (db Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
	// all files are hot!
	return databases.StagingStatusSucceeded, nil
}

func (db *Database) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (db Database) LocalUser(orcid string) (string, error) {
	// no current mechanism for this
	return "localuser", nil
}

func (db Database) Save() (databases.DatabaseSaveState, error) {
	// so far, this database has no internal state
	return databases.DatabaseSaveState{
		Name: "nmdc",
	}, nil
}

func (db *Database) Load(state databases.DatabaseSaveState) error {
	// no internal state -> nothing to do
	return nil
}

//====================
// Internal machinery
//====================

const (
	// NOTE: for now, we use the dev environment (-dev), not prod (which has bugs!)
	// NOTE: note also that NMDC is backed by two databases: one MongoDB and one PostGres,
	// NOTE: which are synced daily-esque. They will sort this out in the coming year,
	// NOTE: and it looks like PostGres is probably going to prevail.
	// NOTE: (See https://github.com/microbiomedata/NMDC_documentation/blob/main/docs/howto_guides/portal_guide.md)
	defaultBaseApiURL  = "https://api.microbiomedata.org/"           // mongoDB
	defaultBaseDataURL = "https://data-dev.microbiomedata.org/data/" // postgres (use in future)
)

func isValidStudyId(s string) bool {
	return strings.HasPrefix(s, "nmdc:") && len(s) >= 6
}

func isValidDataObjectId(s string) bool {
	return strings.HasPrefix(s, "nmdc:") && len(s) >= 6
}

func isValidFileId(s string) bool {
	lastColon := strings.LastIndex(s, ":")
	return lastColon != -1 && isValidStudyId(s[:lastColon]) && isValidDataObjectId("nmdc"+s[lastColon:])
}

func parseFileIds(fileIds []string) (studyIds, dataObjectIds []string, err error) {
	studyIds = make([]string, len(fileIds))
	dataObjectIds = make([]string, len(fileIds))
	for i, fileId := range fileIds {
		if !isValidFileId(fileId) {
			err = &databases.InvalidResourceIdError{
				Database:   "nmdc",
				ResourceId: fileId,
			}
		}
		lastColon := strings.LastIndex(fileId, ":")
		studyIds[i] = fileId[:lastColon]
		dataObjectIds[i] = "nmdc" + fileId[lastColon:]
	}
	return
}

//------------------------------
// Access to NMDC API endpoints
//------------------------------

type authorization struct {
	// API user credential
	Credential auth.Credential
	// client token and type (indicating how it's used in an auth header)
	Token, Type string
	// indicates whether the token expires
	Expires bool
	// time at which the token expires, if any
	ExpirationTime time.Time
}

// fetches an access token / type from NMDC using a credential
func (db *Database) getAccessToken(credential auth.Credential) (authorization, error) {
	var auth authorization
	// NOTE: no slash at the end of the resource, or there's an
	// NOTE: HTTPS -> HTTP redirect (?!??!!)
	resource := db.BaseURL + "token"

	// the token request must be URL-encoded
	data := url.Values{}
	data.Set("grant_type", "password")
	data.Set("username", credential.Id)
	data.Set("password", credential.Secret)
	request, err := http.NewRequest(http.MethodPost, resource, strings.NewReader(data.Encode()))
	if err != nil {
		return auth, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")

	response, err := db.Client.Do(request)
	if err != nil {
		return auth, err
	}

	switch response.StatusCode {
	case 200, 201, 204:
		defer response.Body.Close()
		var data []byte
		data, err = io.ReadAll(response.Body)
		if err != nil {
			return auth, err
		}
		type accessTokenResponse struct {
			Token   string `json:"access_token"`
			Type    string `json:"token_type"`
			Expires struct {
				Days    int `json:"days"`
				Hours   int `json:"hours"`
				Minutes int `json:"minutes"`
			} `json:"expires"`
		}
		var tokenResponse accessTokenResponse
		err = json.Unmarshal(data, &tokenResponse)
		if err != nil {
			return auth, err
		}
		// calculating the time of expiry, subtracting 1 minute for "slop"
		duration := time.Duration(24*tokenResponse.Expires.Days+tokenResponse.Expires.Hours)*time.Hour +
			time.Duration(tokenResponse.Expires.Minutes-1)*time.Minute
		return authorization{
			Credential:     credential,
			Token:          tokenResponse.Token,
			Type:           tokenResponse.Type,
			Expires:        true,
			ExpirationTime: time.Now().Add(duration),
		}, err
	case 503:
		return auth, &databases.UnavailableError{
			Database: "nmdc",
		}
	default:
		defer response.Body.Close()
		var data []byte
		data, _ = io.ReadAll(response.Body)
		type errorResponse struct {
			Detail string `json:"detail"`
		}
		var errResponse errorResponse
		err = json.Unmarshal(data, &errResponse)
		if err != nil {
			return auth, err
		}
		return auth, &databases.UnauthorizedError{
			Database: "nmdc",
			User:     credential.Id,
			Message:  errResponse.Detail,
		}
	}
}

// checks our access token for expiration and renews if necessary
func (db *Database) renewAccessTokenIfExpired() error {
	var err error
	if time.Now().After(db.Auth.ExpirationTime) { // token has expired
		db.Auth, err = db.getAccessToken(db.Auth.Credential)
	}
	return err
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(request *http.Request) {
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", db.Auth.Token))
}

// performs a GET request on the given resource, returning the resulting
// response body and/or error
func (db Database) get(resource string, values url.Values) ([]byte, error) {
	res, err := url.Parse(db.BaseURL)
	if err != nil {
		return nil, err
	}
	res.Path += resource
	res.RawQuery = values.Encode()
	slog.Debug(fmt.Sprintf("GET: %s", res.String()))
	req, err := http.NewRequest(http.MethodGet, res.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
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
			Database: "nmdc",
		}
	default:
		return nil, fmt.Errorf("an error occurred with the NMDC database (%d)",
			resp.StatusCode)
	}
}

//----------------
// Metadata types
//----------------

// data object type for JSON marshalling
// (see https://microbiomedata.github.io/nmdc-schema/DataObject/)
type DataObject struct {
	FileSizeBytes          int      `json:"file_size_bytes"`
	MD5Checksum            string   `json:"md5_checksum"`
	DataObjectType         string   `json:"data_object_type"`
	CompressionType        string   `json:"compression_type"`
	URL                    string   `json:"url"`
	Type                   string   `json:"type"`
	Id                     string   `json:"id"`
	Name                   string   `json:"name"`
	Description            string   `json:"description"`
	StudyId                string   `json:"study_id,omitempty"`
	WasGeneratedBy         string   `json:"was_generated_by,omitempty"`
	AlternativeIdentifiers []string `json:"alternative_identifiers,omitempty"`
}

// https://microbiomedata.github.io/nmdc-schema/CreditAssociation/
type CreditAssociation struct {
	Roles  []string    `json:"applied_roles"`
	Person PersonValue `json:"applies_to_person"`
	Type   string      `json:"type,omitempty"`
}

// https://microbiomedata.github.io/nmdc-schema/Doi/
type Doi struct {
	Value    string `json:"doi_value"`
	Provider string `json:"doi_provider,omitempty"`
	Category string `json:"doi_category"`
}

// https://microbiomedata.github.io/nmdc-schema/PersonValue/
type PersonValue struct {
	Email    string   `json:"email,omitempty"`
	Name     string   `json:"name,omitempty"`
	Orcid    string   `json:"orcid,omitempty"`
	Websites []string `json:"websites,omitempty"`
	RawValue string   `json:"has_raw_value,omitempty"` // name in 'FIRST LAST' format (if present)
}

// https://microbiomedata.github.io/nmdc-schema/Study/
type Study struct { // partial representation, includes only relevant fields
	Id                 string              `json:"id"`
	AlternativeNames   []string            `json:"alternative_names,omitempty"`
	AlternativeTitles  []string            `json:"alternative_titles,omitempty"`
	AssociatedDois     []Doi               `json:"associated_dois,omitempty"`
	Description        string              `json:"description,omitempty"`
	FundingSources     []string            `json:"funding_sources,omitempty"`
	CreditAssociations []CreditAssociation `json:"has_credit_associations,omitempty"`
	Name               string              `json:"name,omitempty"`
	RelatedIdentifiers string              `json:"related_identifiers,omitempty"`
	Title              string              `json:"title,omitempty"`
}

// https://microbiomedata.github.io/nmdc-schema/WorkflowExecution/
type WorkflowExecution struct {
	Id         string  `json:"id"`
	Name       string  `json:"name"`
	Studies    []Study `json:"studies"`
	Biosamples []any   `json:"biosamples"`
}

// returns descriptors for all data objects for a given study
func (db Database) createDataObjectDescriptorsForStudy(studyId string) ([]map[string]any, error) {
	relatedCredit, err := db.creditMetadataForStudy(studyId)
	if err != nil {
		return nil, err
	}

	// fetch the data objects for the study
	resource := fmt.Sprintf("data_objects/study/%s", studyId)
	body, err := db.get(resource, url.Values{})
	if err != nil {
		return nil, err
	}
	type DataObjectsByStudyResults struct {
		BiosampleId string       `json:"biosample_id"`
		DataObjects []DataObject `json:"data_objects"`
	}
	var objectSets []DataObjectsByStudyResults
	err = json.Unmarshal(body, &objectSets)
	if err != nil {
		return nil, err
	}

	// render descriptors from the data objects and credit metadata
	descriptors := make([]map[string]any, 0)
	for _, objectSet := range objectSets {
		for _, dataObject := range objectSet.DataObjects {
			dataObject.StudyId = studyId
			descriptors = append(descriptors, db.createDataObjectDescriptor(dataObject, relatedCredit))
		}
	}
	return descriptors, nil
}

// returns a descriptor for the given data object, including the given credit
// metadata (mined from the study to which the data object belongs)
func (db Database) createDataObjectDescriptor(dataObject DataObject, studyCredit credit.CreditMetadata) map[string]any {
	// fill in some particulars
	objectCredit := studyCredit
	if objectCredit.ResourceType == "" {
		objectCredit.ResourceType = "dataset"
	}
	objectCredit.Descriptions = append(objectCredit.Descriptions,
		credit.Description{
			DescriptionText: dataObject.Description,
			Language:        "en",
		})
	objectCredit.Identifier = dataObject.Id
	objectCredit.Url = dataObject.URL
	descriptor := map[string]any{
		"bytes":       dataObject.FileSizeBytes,
		"credit":      objectCredit,
		"description": dataObject.Description,
		"format":      formatFromType(dataObject.Type),
		"hash":        dataObject.MD5Checksum,
		"id":          dataObject.StudyId + strings.ReplaceAll(dataObject.Id, "nmdc:", ""),
		"mediatype":   mimetypeForFile(dataObject.URL),
		"name":        dataResourceName(dataObject.Name),
		"path":        dataObject.URL,
	}

	// strip the host from the resource's path and assign it an endpoint
	for hostURL, endpoint := range db.EndpointForHost {
		if strings.Contains(descriptor["path"].(string), hostURL) {
			// scrub the host URL and escape the colon so it passes Frictionless validation
			descriptor["path"] = strings.Replace(descriptor["path"].(string), hostURL, "", 1)
			descriptor["path"] = strings.ReplaceAll(descriptor["path"].(string), ":", "\\:")
			descriptor["endpoint"] = endpoint
		}
	}

	return descriptor
}

var idCategoryLabels = map[string]string{
	"award_doi":                "Awarded proposal DOI",
	"dataset_doi":              "Dataset DOI",
	"publication_doi":          "Publication DOI",
	"data_management_plan_doi": "Data management plan DOI",
}

// extracts credit metadata from the given study
func (db Database) creditMetadataForStudy(studyId string) (credit.CreditMetadata, error) {
	// fetch the study and its metadata
	resource := fmt.Sprintf("studies/%s", studyId)
	body, err := db.get(resource, url.Values{})
	if err != nil {
		return credit.CreditMetadata{}, err
	}
	var study Study
	err = json.Unmarshal(body, &study)
	if err != nil {
		return credit.CreditMetadata{}, err
	}

	// NOTE: principal investigator role is included with credit associations
	contributors := make([]credit.Contributor, len(study.CreditAssociations))
	for i, association := range study.CreditAssociations {
		contributors[i] = credit.Contributor{
			ContributorType:  "Person",
			ContributorId:    association.Person.Orcid,
			Name:             association.Person.Name,
			ContributorRoles: strings.Join(association.Roles, ","),
		}
		names := strings.Split(association.Person.Name, " ")
		contributors[i].GivenName = names[0]
		if len(names) > 1 {
			contributors[i].FamilyName = names[len(names)-1]
		}
	}

	var titles []credit.Title
	if study.Title != "" {
		titles = make([]credit.Title, len(study.AlternativeTitles)+1)
		titles[0].Title = study.Title
		for i, alternativeTitle := range study.AlternativeTitles {
			titles[i+1].Title = alternativeTitle
		}
	}

	var relatedIdentifiers []credit.PermanentID
	if len(study.AssociatedDois) > 0 {
		relatedIdentifiers = make([]credit.PermanentID, len(study.AssociatedDois))
		for i, doi := range study.AssociatedDois {
			relatedIdentifiers[i] = credit.PermanentID{
				Id:               doi.Value,
				RelationshipType: "IsCitedBy",
			}
			relatedIdentifiers[i].Description = idCategoryLabels[doi.Category]
		}
	}

	var fundingSources []credit.FundingReference
	if len(study.FundingSources) > 0 {
		fundingSources = make([]credit.FundingReference, len(study.FundingSources))
		for i, fundingSource := range study.FundingSources {
			// FIXME: fundingSource is just a string, so we must make assumptions!
			if strings.Contains(fundingSource, "Department of Energy") {
				fundingSources[i].Funder = credit.Organization{
					OrganizationId:   "ROR:01bj3aw27",
					OrganizationName: "United States Department of Energy",
				}
			}
		}
	}

	return credit.CreditMetadata{
		Contributors: contributors,
		Funding:      fundingSources,
		Publisher: credit.Organization{
			OrganizationId:   "ROR:05cwx3318",
			OrganizationName: "National Microbiome Data Collaborative",
		},
		RelatedIdentifiers: relatedIdentifiers,
		ResourceType:       "dataset",
		Titles:             titles,
	}, nil
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

// a mapping from NMDC file types to format labels
// (see https://microbiomedata.github.io/nmdc-schema/FileTypeEnum/)
var fileTypeToFormat = map[string]string{
	"Annotation Amino Acid FASTA":  "fasta",
	"Annotation Enzyme Commission": "tsv",
	"Annotation KEGG Orthology":    "tsv",
	"Assembly AGP":                 "agp",
	"Assembly Contigs":             "fasta",
	"Assembly Coverage BAM":        "bam",
	"Assembly Info File":           "texinfo",
	"Assembly Scaffolds":           "fasta",
	"BAI File":                     "bai",
	"CATH FunFams (Functional Families) Annotation GFF":   "gff3",
	"Centrifuge Krona Plot":                               "html",
	"Clusters of Orthologous Groups (COG) Annotation GFF": "gff3",
	"CRT Annotation GFF":                                  "gff3",
	"Direct Infusion FT ICR-MS Raw Data":                  "raw",
	"Error Corrected Reads":                               "fastq",
	"Filtered Sequencing Reads":                           "fastq",
	"Functional Annotation GFF":                           "gff3",
	"Genemark Annotation GFF":                             "gff3",
	"Gene Phylogeny tsv":                                  "tsv",
	"GOTTCHA2 Krona Plot":                                 "html",
	"KO_EC Annotation GFF":                                "gff3",
	"Kraken2 Krona Plot":                                  "html",
	"LC-DDA-MS/MS Raw Data":                               "raw",
	"Metagenome Bins":                                     "fasta",
	"Metagenome Raw Reads":                                "raw",
	"Metagenome Raw Read 1":                               "raw",
	"Metagenome Raw Read 2":                               "raw",
	"Misc Annotation GFF":                                 "gff3",
	"Pfam Annotation GFF":                                 "gff3",
	"Prodigal Annotation GFF":                             "gff3",
	"QC non-rRNA R1":                                      "fastq",
	"QC non-rRNA R2":                                      "fastq",
	"Read Count and RPKM":                                 "json",
	"RFAM Annotation GFF":                                 "gff3",
	"Scaffold Lineage tsv":                                "tsv",
	"Structural Annotation GFF":                           "gff3",
	"Structural Annotation Stats Json":                    "json",
	"SUPERFam Annotation GFF":                             "gff3",
	"SMART Annotation GFF":                                "gff3",
	"TIGRFam Annotation GFF":                              "gff3",
	"TMRNA Annotation GFF":                                "gff3",
	"TRNA Annotation GFF":                                 "gff3",
}

// extracts the file format from the name and type of the file
func formatFromType(fileType string) string {
	if format, found := fileTypeToFormat[fileType]; found {
		return format
	}
	return "unknown"
}

// extracts the file format from the name and type of the file
func mimetypeForFile(filename string) string {
	mimetype := mime.TypeByExtension(filepath.Ext(filename))
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	return mimetype
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
				name = name[:start] + string('_') + name[end:]
			} else {
				name = name[:start] + string('_')
			}
		} else {
			break
		}
	}

	return name
}

// checks NMDC-specific search parameters
func (db Database) addSpecificSearchParameters(params map[string]any, p *url.Values) error {
	paramSpec := db.SpecificSearchParameters()
	for name, jsonValue := range params {
		var ok bool
		switch name {
		case "activity_id", "data_object_id", "filter", "sort", "sample_id":
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "nmdc",
					Message:  fmt.Sprintf("invalid value for parameter %s (must be string)", name),
				}
			}
			p.Add(name, value)
		case "fields": // accepts comma-delimited strings
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "nmdc",
					Message:  "invalid NMDC requested extra field given (must be comma-delimited string)",
				}
			}
			acceptedValues := paramSpec["extra"].([]string)
			fieldValues := strings.Split(value, ",")
			for _, fieldValue := range fieldValues {
				fieldValue = strings.TrimSpace(fieldValue)
				if slices.Contains(acceptedValues, fieldValue) {
					p.Add(name, fieldValue)
				} else {
					return &databases.InvalidSearchParameter{
						Database: "nmdc",
						Message:  fmt.Sprintf("invalid requested extra field: %s", fieldValue),
					}
				}
			}
		case "extra": // accepts comma-delimited strings
			var value string
			if value, ok = jsonValue.(string); !ok {
				return &databases.InvalidSearchParameter{
					Database: "nmdc",
					Message:  "invalid NMDC requested extra field given (must be comma-delimited string)",
				}
			}
			acceptedValues := paramSpec["extra"].([]string)
			extraValues := strings.Split(value, ",")
			for _, extraValue := range extraValues {
				extraValue = strings.TrimSpace(extraValue)
				if slices.Contains(acceptedValues, extraValue) {
					p.Add(name, extraValue)
				} else {
					return &databases.InvalidSearchParameter{
						Database: "nmdc",
						Message:  fmt.Sprintf("Invalid requested extra field: %s", extraValue),
					}
				}
			}
		default:
			return &databases.InvalidSearchParameter{
				Database: "nmdc",
				Message:  fmt.Sprintf("Unrecognized NMDC-specific search parameter: %s", name),
			}
		}
	}
	return nil
}
