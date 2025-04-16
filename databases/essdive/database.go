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

package essdive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/piprate/json-gold/ld"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
)

// file database appropriate for handling searches and transfers
// (implements the databases.Database interface)
type Database struct {
	// HTTP client that caches queries
	Client http.Client
	// authorization info
	Auth authorization
}

func NewDatabase() (databases.Database, error) {
	if config.Databases["ess-dive"].Endpoint != "" {
		return nil, databases.InvalidEndpointsError{
			Database: "ess-dive",
			Message:  "ESS-DIVE requires a Globus endpoint",
		}
	}

	// NOTE: we prevent redirects from HTTPS -> HTTP!
	db := &Database{
		Client: databases.SecureHttpClient(),
	}

	err := db.getAccessToken()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (db Database) SpecificSearchParameters() map[string]interface{} {
	// for details about NMDC-specific search parameters, see
	// https://api.microbiomedata.org/docs#/find:~:text=Find%20NMDC-,metadata,-entities.
	return map[string]interface{}{
		"activity_id":    "",
		"data_object_id": "",
		"fields":         "",
		"filter":         "",
		"sort":           "",
		"sample_id":      "",
		"study_id":       "",
		"extra":          "",
	}
}

func (db *Database) Search(orcid string, params databases.SearchParameters) (databases.SearchResults, error) {
	p := url.Values{}

	// translate pagination parameters
	pageNumber, pageSize := pageNumberAndSize(params.Pagination.Offset, params.Pagination.MaxNum)
	p.Add("rowStart", strconv.Itoa(pageNumber*pageSize))
	p.Add("pageSize", strconv.Itoa(pageSize))

	// NOTE: currently, the query string is used to specify a dataset
	if params.Query != "" {
		p.Add("text", params.Query)
	}

	// otherwise, simply call the data_objects/ endpoint (possibly with a filter applied)
	return db.dataObjectsForDataset(p)
}

func (db Database) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	if err := db.renewAccessTokenIfExpired(); err != nil {
		return nil, err
	}

	// we use ESS-DIVE's Fusion API to retrieve metadata for individual files

	// gather relevant study IDs and use them to build credit metadata
	studyIdForDataObjectId, err := db.studyIdsForDataObjectIds(fileIds)
	if err != nil {
		return nil, err
	}
	creditForStudyId := make(map[string]credit.CreditMetadata)
	for _, studyId := range studyIdForDataObjectId {
		credit, foundStudyCredit := creditForStudyId[studyId]
		if !foundStudyCredit {
			credit, err = db.creditMetadataForStudy(studyId)
			if err != nil {
				return nil, err
			}
			creditForStudyId[studyId] = credit // cache for other data objects
		}
	}

	// construct data resources from the IDs
	resources := make([]map[string]any, len(fileIds))
	for i, fileId := range fileIds {
		body, err := db.get(fmt.Sprintf("data_objects/%s", fileId), url.Values{})
		if err != nil {
			return nil, err
		}
		var dataObject DataObject
		err = json.Unmarshal(body, &dataObject)
		if err != nil {
			return nil, err
		}
		resources[i], err = db.dataResourceFromDataObject(dataObject)
		if err != nil {
			return nil, err
		}

		// add credit metadata
		studyId := studyIdForDataObjectId[resources[i].Id]
		resources[i].Credit = creditForStudyId[studyId]
		resources[i].Credit.ResourceType = "dataset"
		resources[i].Credit.Identifier = resources[i].Id
	}
	return resources, nil
}

func (db Database) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (db Database) StagingStatus(id uuid.UUID) (databases.StagingStatus, error) {
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

//--------------------
// Internal machinery
//--------------------

const (
	baseApiURL   = "https://api.ess-dive.lbl.gov/"
	fusionApiURL = "https://fusion.ess-dive.lbl.gov/"
)

// Authorization / authentication

type authorization struct {
	// API user credential
	Credential credential
	// client token and type (indicating how it's used in an auth header)
	Token, Type string
	// indicates whether the token expires
	Expires bool
	// time at which the token expires, if any
	ExpirationTime time.Time
}

type credential struct {
	User, Password string
}

// fetches an access token / type from the environment if available
func (db *Database) getAccessToken() error {
	accessToken, haveaccessToken := os.LookupEnv("DTS_ESSDIVE_TOKEN")
	if !haveaccessToken {
		return databases.UnauthorizedError{
			Database: "ess-dive",
			Message:  "No access token (DTS_ESSDIVE_TOKEN) was provided for authentication",
		}
	}
	db.Auth = authorization{
		Token: accessToken,
		Type:  "Bearer",
	}
	return nil
}

func (db *Database) renewAccessTokenIfExpired() error {
	// FIXME: programmatic token renewal is not yet supported
}

// adds an appropriate authorization header to given HTTP request
func (db Database) addAuthHeader(request *http.Request) {
	request.Header.Add("Authorization", fmt.Sprintf("%s %s", db.Auth.Type, db.Auth.Token))
}

// performs a GET request on the given resource, returning the resulting
// response body and/or error
func (db Database) get(resource string, values url.Values) ([]byte, error) {
	res, err := url.Parse(baseApiURL)
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
			Database: "ess-dive",
		}
	default:
		return nil, fmt.Errorf("An error occurred with the ESS-DIVE database (%d)",
			resp.StatusCode)
	}
}

// performs a POST request on the given resource, returning the resulting
// response body and/or error
func (db Database) post(resource string, body io.Reader) ([]byte, error) {
	res, err := url.Parse(baseApiURL)
	if err != nil {
		return nil, err
	}
	res.Path += resource
	slog.Debug(fmt.Sprintf("POST: %s", res.String()))
	req, err := http.NewRequest(http.MethodPost, res.String(), body)
	if err != nil {
		return nil, err
	}
	db.addAuthHeader(req)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := db.Client.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 200, 201, 204:
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	case 503:
		return nil, &databases.UnavailableError{
			Database: "ess-dive",
		}
	default:
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("An error occurred mapping NMDC data objects to studies: %s",
			string(data))
	}
}

type dataset struct {
	Context     string   `json:"@context"`
	Type        string   `json:"@type"`
	Id          string   `json:"@id"`
	Name        string   `json:"name"`
	Description []string `json:"description"`
}

type creator struct {
	Type string `json:"@type"`
	Id   string `json:"@id"`
}

func (db Database) dataResourceFromDataObject(dataObject DataObject) (map[string]any, error) {
	resource := map[string]anyDataResource{
		Bytes: dataObject.FileSizeBytes,
		Credit: credit.CreditMetadata{
			Descriptions: []credit.Description{
				{
					DescriptionText: dataObject.Description,
					Language:        "en",
				},
			},
			Identifier: dataObject.Id,
			Url:        dataObject.URL,
		},
		Description: dataObject.Description,
		Format:      formatFromType(dataObject.Type),
		Hash:        dataObject.MD5Checksum,
		Id:          dataObject.Id,
		MediaType:   mimeTypeFromFormat(formatFromType(dataObject.Type)),
		Name:        dataResourceName(dataObject.Name),
		Path:        dataObject.URL,
	}

	// strip the host from the resource's path and assign it an endpoint
	for hostURL, endpoint := range db.EndpointForHost {
		if strings.Contains(resource.Path, hostURL) {
			resource.Path = strings.Replace(resource.Path, hostURL, "", 1)
			resource.Endpoint = endpoint
		}
	}

	return resource, nil
}

func (db Database) studyIdsForDataObjectIds(dataObjectIds []string) (map[string]string, error) {
	// We create an aggregation query on the data_generation_set collection.
	// The data_generation_set collection associates studies with data objects:
	// * the associated_studies field points to a study_set collection
	// * the was_informed_by field points to a workflow_execution_set collection,
	//   whose has_output field points to a data_object_set collection
	//
	// NOTE: The API documentation for find/aggregate queries
	// NOTE: (https://api.microbiomedata.org/docs#/queries/run_query_queries_run_post)
	// NOTE: includes words of caution:
	// NOTE:
	// NOTE: > For `find` and `aggregate`, note that cursor batching/pagination does
	// NOTE: > not work via this API, so ensure that you construct a command that
	// NOTE: > will return what you need in the "first batch". Also, the maximum
	// NOTE: > size of the returned payload is 16MB.
	// NOTE:
	// NOTE: If we need to, we can break up our aggregate queries into smaller
	// NOTE: chunks, since these queries are independent.
	type MatchIdInSlice struct {
		In []string `json:"$in,omitempty"`
	}
	type MatchOperation struct {
		// matches an ID with one of those in the given list
		Id MatchIdInSlice `json:"id"`
	}
	type LookupOperation struct {
		From         string `json:"from"`
		LocalField   string `json:"localField"`
		ForeignField string `json:"foreignField"`
		As           string `json:"as"`
	}
	type PipelineOperation struct {
		// this is a bit cheesy but is simple and works
		// we use struct pointers here so omitempty works properly
		Match  *MatchOperation  `json:"$match,omitempty"`
		Lookup *LookupOperation `json:"$lookup,omitempty"`
	}
	type CursorProperty struct {
		BatchSize int `json:"batchsize,omitempty"`
	}
	type AggregateRequest struct {
		Aggregate string              `json:"aggregate"`
		Pipeline  []PipelineOperation `json:"pipeline"`
		Cursor    CursorProperty      `json:"cursor,omitempty"`
	}
	data, err := json.Marshal(AggregateRequest{
		Aggregate: "data_object_set",
		Pipeline: []PipelineOperation{
			// match against our set of data object IDs
			{
				Match: &MatchOperation{
					Id: MatchIdInSlice{
						In: dataObjectIds,
					},
				},
			},
			// look up the data object's workflow execution set
			// (the study IDs for the data generation set are in
			//  the associated_studies field)
			{
				Lookup: &LookupOperation{
					From:         "data_generation_set",
					LocalField:   "was_generated_by",
					ForeignField: "id",
					As:           "data_generation_sets",
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	// run the query and extract the results
	// NOTE: recall that trailing slashes in POSTs currently cause chaos!
	body, err := db.post("queries:run", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	type DataGenerationSet struct {
		Id                string   `json:"id"`
		AssociatedStudies []string `json:"associated_studies"`
	}
	type DataObjectAndDataGenerationSet struct {
		DataObjectId       string              `json:"id"`
		DataGenerationSets []DataGenerationSet `json:"data_generation_sets"`
	}
	type QueryResults struct {
		Ok     int `json:"ok"`
		Cursor struct {
			FirstBatch []DataObjectAndDataGenerationSet `json:"firstBatch"`
			Id         int                              `json:"id"`
			NS         string                           `json:"ns"`
		}
	}
	var results QueryResults
	err = json.Unmarshal(body, &results)
	if err != nil {
		return nil, err
	}

	// map each data object ID to the corresponding study ID
	studyIdForDataObjectId := make(map[string]string)
	for _, record := range results.Cursor.FirstBatch {
		// FIXME: for now, take the first study in the first data generation set
		if len(record.DataGenerationSets) > 0 {
			if len(record.DataGenerationSets[0].AssociatedStudies) > 0 {
				studyIdForDataObjectId[record.DataObjectId] = record.DataGenerationSets[0].AssociatedStudies[0]
			} else {
				slog.Debug(fmt.Sprintf("No study is associated with the data object %s", record.DataObjectId))
			}
		} else {
			slog.Debug(fmt.Sprintf("No data generation info was found for the data object %s", record.DataObjectId))
		}
	}
	return studyIdForDataObjectId, err
}

// fetches metadata for data objects (no credit metadata, alas) based on the
// given URL search parameters
func (db Database) dataObjects(params url.Values) (databases.SearchResults, error) {
	var results databases.SearchResults

	// extract any requested "extra" metadata fields (and scrub them from params)
	// FIXME: no extra fields yet, so we simply remove this parameter
	//var extraFields []string
	if params.Has("extra") {
		//extraFields = strings.Split(params.Get("extra"), ",")
		params.Del("extra")
	}

	body, err := db.get("data_objects/", params)
	type DataObjectResults struct {
		// NOTE: we only extract the results field for now
		Results []DataObject `json:"results"`
	}
	if err != nil {
		return results, err
	}
	var dataObjectResults DataObjectResults
	err = json.Unmarshal(body, &dataObjectResults)
	if err != nil {
		return results, err
	}

	// map data object IDs to study IDs so we can retrieve credit info

	// assemble all data object identifiers and map them to study IDs
	dataObjectIds := make([]string, len(dataObjectResults.Results))
	for i, dataObject := range dataObjectResults.Results {
		dataObjectIds[i] = dataObject.Id
	}
	studyIdForDataObjectId, err := db.studyIdsForDataObjectIds(dataObjectIds)
	if err != nil {
		return results, err
	}

	// create data resources from data objects, fetch study metadata, and fill in
	// data resource credit information
	results.Resources = make([]frictionless.DataResource, len(dataObjectResults.Results))
	creditForStudyId := make(map[string]credit.CreditMetadata)
	for i, dataObject := range dataObjectResults.Results {
		studyId := studyIdForDataObjectId[dataObject.Id]
		credit, foundStudyCredit := creditForStudyId[studyId]
		if !foundStudyCredit {
			credit, err = db.creditMetadataForStudy(studyId)
			if err != nil {
				return results, err
			}
			creditForStudyId[studyId] = credit // cache for other data objects
		}
		results.Resources[i], err = db.dataResourceFromDataObject(dataObject)
		if err != nil {
			return results, err
		}
		results.Resources[i].Credit = credit
	}

	return results, nil
}

// fetches credit metadata for the package with the given ID
func (db Database) creditMetadataForPackage(packageId string) (credit.CreditMetadata, error) {
	// vvv credit-related NMDC schema types vvv

	// https://microbiomedata.github.io/nmdc-schema/PersonValue/
	type PersonValue struct {
		Email    string   `json:"email,omitempty"`
		Name     string   `json:"name,omitempty"`
		Orcid    string   `json:"orcid,omitempty"`
		Websites []string `json:"websites,omitempty"`
		RawValue string   `json:"has_raw_value,omitempty"` // name in 'FIRST LAST' format (if present)
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

	// fetch the study with the given ID
	var creditMetadata credit.CreditMetadata
	body, err := db.get(fmt.Sprintf("studies/%s", studyId), url.Values{})
	if err != nil {
		return creditMetadata, err
	}
	var study Study
	err = json.Unmarshal(body, &study)
	if err != nil {
		return creditMetadata, err
	}

	// fish metadata out of the study

	// NOTE: principal investigator role is included with credit associations
	contributors := make([]credit.Contributor, len(study.CreditAssociations))
	for i, association := range study.CreditAssociations {
		contributors[i] = credit.Contributor{
			ContributorType:  "Person",
			ContributorId:    association.Person.Orcid,
			Name:             association.Person.Name,
			ContributorRoles: strings.Join(association.Roles, ","),
		}
		names := strings.Split(" ", association.Person.Name)
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
			switch doi.Category {
			case "award_doi":
				relatedIdentifiers[i].Description = "Awarded proposal DOI"
			case "dataset_doi":
				relatedIdentifiers[i].Description = "Dataset DOI"
			case "publication_doi":
				relatedIdentifiers[i].Description = "Publication DOI"
			case "data_management_plan_doi":
				relatedIdentifiers[i].Description = "Data management plan DOI"
			}
		}
	}

	var fundingSources []credit.FundingReference
	if len(study.FundingSources) > 0 {
		fundingSources = make([]credit.FundingReference, len(study.FundingSources))
		for i, fundingSource := range study.FundingSources {
			// FIXME: fundingSource is just a string, so we must make assumptions!
			if strings.Index(fundingSource, "Department of Energy") != -1 {
				fundingSources[i].Funder = credit.Organization{
					OrganizationId:   "ROR:01bj3aw27",
					OrganizationName: "United States Department of Energy",
				}
			}
		}
	}

	creditMetadata = credit.CreditMetadata{
		// Identifier, Dates, and Version fields are specific to DataResources, omitted here
		Contributors: contributors,
		Funding:      fundingSources,
		Publisher: credit.Organization{
			OrganizationId:   "ROR:05cwx3318",
			OrganizationName: "National Microbiome Data Collaborative",
		},
		RelatedIdentifiers: relatedIdentifiers,
		ResourceType:       "dataset",
		Titles:             titles,
	}
	// FIXME: we can probably chase down credit metadata dates using the
	// FIXME: generated_by (Activity) field, instantiated as one of the
	// FIXME: concrete types listed here: https://microbiomedata.github.io/nmdc-schema/WorkflowExecutionActivity/

	return creditMetadata, err
}

// fetches file metadata for data objects associated with the given study
func (db Database) dataObjectsForStudy(studyId string, params url.Values) (databases.SearchResults, error) {
	var results databases.SearchResults

	body, err := db.get(fmt.Sprintf("data_objects/study/%s", studyId), params)
	if err != nil {
		return results, err
	}

	type DataObjectsByStudyResults struct {
		BiosampleId string       `json:"biosample_id"`
		DataObjects []DataObject `json:"data_objects"`
	}
	var objectSets []DataObjectsByStudyResults
	err = json.Unmarshal(body, &objectSets)
	if err != nil {
		return results, err
	}

	// FIXME: I'm not able to get filters to work as (seems to be) intended, so
	// FIXME: this is a short-term hack.
	var dataObjectType string
	if params.Has("filter") {
		filter := params.Get("filter")
		colon := strings.Index(filter, ":")
		if strings.Contains(filter, "data_object_type:") {
			dataObjectType = filter[colon+1:]
		}
	}

	// create resources for the data objects
	results.Resources = make([]frictionless.DataResource, 0)
	for _, objectSet := range objectSets {
		for _, dataObject := range objectSet.DataObjects {
			// FIXME: apply hack!
			if dataObjectType != "" && dataObject.DataObjectType != dataObjectType {
				slog.Debug(fmt.Sprintf("Data object type mismatch (want %s, got %s)", dataObjectType, dataObject.DataObjectType))
				continue
			}
			resource, err := db.dataResourceFromDataObject(dataObject)
			if err != nil {
				return results, err
			}
			results.Resources = append(results.Resources, resource)
		}
	}

	// fill in study-level credit metadata for each resource
	studyCreditMetadata, err := db.creditMetadataForStudy(studyId)
	if err != nil {
		return results, err
	}
	for i := range results.Resources {
		results.Resources[i].Credit.Contributors = studyCreditMetadata.Contributors
		results.Resources[i].Credit.Funding = studyCreditMetadata.Funding
		results.Resources[i].Credit.Publisher = studyCreditMetadata.Publisher
		results.Resources[i].Credit.RelatedIdentifiers = studyCreditMetadata.RelatedIdentifiers
		results.Resources[i].Credit.ResourceType = studyCreditMetadata.ResourceType
		results.Resources[i].Credit.Titles = studyCreditMetadata.Titles
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

// a mapping from file format labels to mime types
var formatToMimeType = map[string]string{
	"agp":     "application/octet-stream",
	"bam":     "application/octet-stream",
	"bai":     "application/octet-stream",
	"csv":     "text/csv",
	"fasta":   "text/plain",
	"fastq":   "text/plain",
	"gff":     "text/plain",
	"gff3":    "text/plain",
	"gz":      "application/gzip",
	"bz":      "application/x-bzip",
	"bz2":     "application/x-bzip2",
	"json":    "application/json",
	"raw":     "application/octet-stream",
	"tar":     "application/x-tar",
	"text":    "text/plain",
	"texinfo": "text/plain",
	"tsv":     "text/plain",
}

// extracts the file format from the name and type of the file
func formatFromType(fileType string) string {
	if format, found := fileTypeToFormat[fileType]; found {
		return format
	}
	return "unknown"
}

// extracts the file format from the name and type of the file
func mimeTypeFromFormat(format string) string {
	if mimeType, ok := formatToMimeType[format]; ok {
		return mimeType
	}
	return "application/octet-stream"
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
