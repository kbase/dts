package nmdc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

const nmdcConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
  endpoint: globus-jdp
databases:
  nmdc:
    name: National Microbiome Data Collaborative
    organization: DOE
    user: ${DTS_NMDC_USER}
    password: ${DTS_NMDC_PASSWORD}
    endpoints:
      nersc: globus-nmdc-nersc
      emsl: globus-nmdc-emsl
endpoints:
  globus-nmdc-nersc:
    name: NMDC (NERSC)
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    root: /
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  globus-nmdc-emsl:
    name: NMDC Bulk Data Cache
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    root: /
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

const mockStudyResponse string = `{
  "id": "nmdc:sty-11-r2h77870",
  "name": "Tara Oceans Mediterranean Sea Expedition 2013",
  "description": "Metagenomes and environmental data from the Tara Oceans Mediterranean Sea Expedition 2013",
  "dois": [
	{
       "doi_value": "10.5281/zenodo.1242459",
	   "doi_category": "primary"
	}
  ],
  "title": "Tara Oceans Mediterranean Sea Expedition 2013"
}`


const mockDataObjectResponse string = `{
	"id": "nmdc:do-1234-abcde56789",
	"name": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
	"description": "Metagenomes and environmental data from the Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
	"title": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
	"was_generated_by": "nmdc:wf-1234-abcde56789"
}`

const mockDataObjectWithNmdcWorkflowResponse string = `{
	"id": "nmdc:do-5678-efghij12345",
	"name": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
	"description": "Metagenomes and environmental data from the Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
	"title": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
	"was_generated_by": "nmdc:wf-1234-abcde56789"
}`
			
const mockDataObjectsResponse string = `[
	{
		"biosample_id": "nmdc:bs-1234-abcde56789",
		"data_objects": [
			{
				"id": "nmdc:do-1234-abcde56789",
				"name": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
				"description": "Metagenomes and environmental data from the Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
				"title": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 1",
				"was_generated_by": "nmdc:wf-1234-abcde56789"
			},
			{
				"id": "nmdc:do-5678-efghij12345",
				"name": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
				"description": "Metagenomes and environmental data from the Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
				"title": "Tara Oceans Mediterranean Sea Expedition 2013 - Data Object 2",
				"was_generated_by": "nmdc:wf-1234-abcde56789"
			}
		]
	}
]`

const mockWorkflowResponse string = `{
	"id": "nmdc:wf-1234-abcde56789",
	"name": "Mock Workflow",
	"studies": [
		{
			"id": "nmdc:sty-11-r2h77870",
			"title": "Tara Oceans Mediterranean Sea Expedition 2013"
		}
	],
	"biosamples": [
		{
			"id": "nmdc:bs-1234-abcde56789",
			"name": "Mock Biosample 1"
		}
	]
}`

const mockWorkflowTooManyStudeiesResponse string = `{
	"id": "nmdc:wf-too-many-studies",
	"name": "Mock Workflow with Too Many Studies",
	"studies": [
		{
			"id": "nmdc:sty-11-r2h77870",
			"title": "Tara Oceans Mediterranean Sea Expedition 2013"
		},
		{
			"id": "nmdc:sty-22-x3y4z56789",
			"title": "Another Study"
		}
	],
	"biosamples": [
		{
			"id": "nmdc:bs-1234-abcde56789",
			"name": "Mock Biosample 1"
		}
	]
}`

const mockWorkflowTooManyBiosamplesResponse string = `{
	"id": "nmdc:wf-too-many-biosamples",
	"name": "Mock Workflow with Too Many Biosamples",
	"studies": [
		{
			"id": "nmdc:sty-11-r2h77870",
			"title": "Tara Oceans Mediterranean Sea Expedition 2013"
		}
	],
	"biosamples": [
		{
			"id": "nmdc:bs-1234-abcde56789",
			"name": "Mock Biosample 1"
		},
		{
			"id": "nmdc:bs-5678-fghij12345",
			"name": "Mock Biosample 2"
		}
	]
}`

// If the DTS_KBASE_TEST_ORCID environment variable is set, we will
// assume valid NMDC credentials are available for testing.
var areValidCredentials bool = false
var testOrcid string = "0000-0002-0785-587X"

var mockNmdcServer *httptest.Server
var mockNmdcUser string = "testuser"
var mockNmdcPassword string = "testpassword"
var mockNmdcSecret string = "testsecret"
var mockNerscEndpoint string = "globus-nmdc-nersc"
var mockEmslEndpoint string = "globus-nmdc-emsl"


// since NMDC doesn't support search queries at this time, we search for
// data objects related to a study
var nmdcSearchParams map[string]any

// Creates a mock NMDC server for testing
func createMockNmdcServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/token":
			err := r.ParseForm()
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "invalid request",
				})
				return
			}
			grantType := r.FormValue("grant_type")
			username := r.FormValue("username")
			password := r.FormValue("password")
			if grantType != "password" ||
				username != mockNmdcUser ||
				password != mockNmdcPassword {
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "invalid credentials",
				})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"access_token": mockNmdcSecret,
				"token_type":   "bearer",
				"expires": map[string]any{
					"days":    1,
					"hours":   0,
					"minutes": 0,
				},
			})
		default:
			if strings.HasPrefix(r.URL.Path, "/studies") {
				// return mock search results for study: /studies/nmdc:sty-11-r2h77870
				id := strings.TrimPrefix(r.URL.Path, "/studies/")
				if id == "nmdc:sty-11-r2h77870" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockStudyResponse))
					return
				}
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "study not found",
				})
				return
			} else if strings.HasPrefix(r.URL.Path, "/data_objects/study/") {
				// return mock data object search results for study data objects
				studyId := strings.TrimPrefix(r.URL.Path, "/data_objects/study/")
				if studyId == "nmdc:sty-11-r2h77870" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockDataObjectsResponse))
					return
				}
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "data objects for study not found",
				})
				return
			} else if strings.HasPrefix(r.URL.Path, "/data_objects/") {
				// return mock data object descriptors for: /data_objects/{id}
				dataObjectId := strings.TrimPrefix(r.URL.Path, "/data_objects/")
				if dataObjectId == "nmdc:do-1234-abcde56789" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockDataObjectResponse))
					return
				} else if dataObjectId == "nmdc:do-5678-efghij12345" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockDataObjectWithNmdcWorkflowResponse))
					return
				}
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "data object not found",
				})
				return
			} else if strings.HasPrefix(r.URL.Path, "/workflow_executions/") {
				// return mock workflow for: /workflow_executions/{id}
				workflowId := strings.TrimPrefix(r.URL.Path, "/workflow_executions/")
				if !strings.HasSuffix(workflowId, "/related_resources") {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{
						"error": "invalid workflow id",
					})
					return
				}
				workflowId = strings.TrimSuffix(workflowId, "/related_resources")
				if workflowId == "nmdc:wf-1234-abcde56789" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockWorkflowResponse))
					return
				} else if workflowId == "nmdc:wf-too-many-studies" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockWorkflowTooManyStudeiesResponse))
					return
				} else if workflowId == "nmdc:wf-too-many-biosamples" {
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(mockWorkflowTooManyBiosamplesResponse))
					return
				}
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "workflow not found",
				})
				return
			}
			// default: not found
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not found",
			})
		}
	}))
}

// create a mock NMDC database for testing
func NewMockDatabase(baseUrl string) func() (databases.Database, error) {
	return func() (databases.Database, error) {
		return &Database{
			BaseURL:        baseUrl,
			Auth:           authorization{
				Credential: credential{
					User:     mockNmdcUser,
					Password: mockNmdcPassword,
				},
				Token: mockNmdcSecret,
				Type:  "basic",
				Expires: false,
			},
			EndpointForHost: map[string]string{
			"https://data.microbiomedata.org/data/": mockNerscEndpoint,
			"https://nmdcdemo.emsl.pnnl.gov/":       mockEmslEndpoint,
		},
		}, nil
	}
}

// function for setting mock database options
func mockDatabaseOptions(cfg *DatabaseConfig) {
	cfg.BaseURL = mockNmdcServer.URL + "/" // add trailing slash to match default URL format
}

// helper function replaces embedded environment variables in yaml strings
// when they don't exist in the environment
func setTestEnvVars(yaml string) (string, bool) {
	testVars := map[string]string{
		"DTS_NMDC_USER":            mockNmdcUser,
		"DTS_NMDC_PASSWORD":        mockNmdcPassword,
		"DTS_GLOBUS_TEST_ENDPOINT": "5e5f7d4e-3f4b-11eb-9ac6-0a58a9feac02",
		"DTS_GLOBUS_CLIENT_ID":     "test_client_id",
		"DTS_GLOBUS_CLIENT_SECRET": "test_client_secret",
	}
	hasValidCredentials := true
	// check for existence of each variable. when not present, replace
	// instances of it in the yaml string with a test value
	for key, value := range testVars {
		if os.Getenv(key) == "" {
			yaml = os.Expand(yaml, func(yamlVar string) string {
				if yamlVar == key {
					hasValidCredentials = false
					return value
				}
				return "${" + yamlVar + "}"
			})
		}
	}
	return yaml, hasValidCredentials
}

// this function gets called at the beginning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	configString, isValid := setTestEnvVars(nmdcConfig)
	areValidCredentials = isValid
	config.Init([]byte(configString))
	conf, err := config.NewConfig([]byte(configString))
	if err != nil {
		panic("Couldn't read test configuration: " + err.Error())
	}
	if areValidCredentials {
		err := databases.RegisterDatabase("nmdc", DatabaseConstructor(conf))
		if err != nil {
			panic("Couldn't register NMDC database: " + err.Error())
		}
	} else {
		mockNmdcServer := createMockNmdcServer()
		err := databases.RegisterDatabase("nmdc", NewMockDatabase(mockNmdcServer.URL))
		if err != nil {
			panic("Couldn't register NMDC database: " + err.Error())
		}
	}
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpointFromConfig)

	// construct NMDC-specific search parameters for a study
	nmdcSearchParams = make(map[string]any)
	nmdcSearchParams["study_id"] = "nmdc:sty-11-r2h77870"

	// check for valid NMDC credentials
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	if orcid != "" {
		testOrcid = orcid
	}
	mockNmdcServer = createMockNmdcServer()
}

// this function gets called after all tests have been run
func breakdown() {
	if mockNmdcServer != nil {
		mockNmdcServer.Close()
	}
}

// Get an instance of the NMDC database for a specific test
func getNmdcDatabase(t *testing.T) databases.Database {
	assert := assert.New(t)
	configString, _ := setTestEnvVars(nmdcConfig)
	conf, err := config.NewConfig([]byte(configString))
	assert.Nil(err, "Couldn't read test configuration")
	var db databases.Database
	db, err = NewDatabase(conf)
	assert.NotNil(db, "NMDC database not created")
	assert.Nil(err, "NMDC database creation encountered an error")
	return db
}

// Get an instance of the NMDC mock database for a specific test
func getMockNmdcDatabase(t *testing.T) databases.Database {
	assert := assert.New(t)
	configString, _ := setTestEnvVars(nmdcConfig)
	conf, err := config.NewConfig([]byte(configString))
	assert.Nil(err, "Couldn't read test configuration")
	var db databases.Database
	db, err = NewDatabase(conf, mockDatabaseOptions)
	assert.NotNil(db, "NMDC mock database not created")
	assert.Nil(err, "NMDC mock database creation encountered an error")
	return db
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	configString, _ := setTestEnvVars(nmdcConfig)
	conf, err := config.NewConfig([]byte(configString))
	assert.Nil(err, "Couldn't read test configuration")
	if areValidCredentials {
		db, err := NewDatabase(conf)
		assert.NotNil(db, "NMDC database not created")
		assert.Nil(err, "NMDC database creation encountered an error")
	} else {
		db, err := NewDatabase(conf, mockDatabaseOptions)
		assert.NotNil(db, "NMDC mock database not created")
		assert.Nil(err, "NMDC mock database creation encountered an error")
	}
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	var db databases.Database
	if areValidCredentials {
		db = getNmdcDatabase(t)
	} else {
		db = getMockNmdcDatabase(t)
	}
	
	params := databases.SearchParameters{
		Query:    "",
		Specific: nmdcSearchParams,
	}
	results, err := db.Search(testOrcid, params)
	assert.True(len(results.Descriptors) > 0, "NMDC search query returned no results")
	assert.Nil(err, "NMDC search query encountered an error")
}

func TestDescriptors(t *testing.T) {
	assert := assert.New(t)
	var db databases.Database
	var expectedCount int
	if areValidCredentials {
		db = getNmdcDatabase(t)
		expectedCount = 10
	} else {
		db = getMockNmdcDatabase(t)
		expectedCount = 2
	}
	params := databases.SearchParameters{
		Query:    "",
		Specific: nmdcSearchParams,
	}
	results, _ := db.Search(testOrcid, params)
	fileIds := make([]string, len(results.Descriptors))
	for i, descriptor := range results.Descriptors {
		fileIds[i] = descriptor["id"].(string)
	}
	descriptors, err := db.Descriptors(testOrcid, fileIds[:expectedCount])
	assert.Nil(err, "NMDC resource query encountered an error")
	assert.True(len(descriptors) >= expectedCount, // can include biosample metadata!
		"NMDC resource query didn't return all results")
	for i, desc := range descriptors[:expectedCount] {
		nmdcSearchResult := results.Descriptors[i]
		assert.Equal(nmdcSearchResult["id"], desc["id"], "Resource ID mismatch")
		assert.Equal(nmdcSearchResult["name"], desc["name"], "Resource name mismatch")
		assert.Equal(nmdcSearchResult["path"], desc["path"], "Resource path mismatch")
		assert.Equal(nmdcSearchResult["format"], desc["format"], "Resource format mismatch")
		assert.Equal(nmdcSearchResult["bytes"], desc["bytes"], "Resource size mismatch")
		assert.Equal(nmdcSearchResult["mediatype"], desc["mediatype"], "Resource media type mismatch")
		assert.Equal(nmdcSearchResult["credit"].(credit.CreditMetadata).Identifier, desc["credit"].(credit.CreditMetadata).Identifier, "Resource credit ID mismatch")
		// skip comparisons of 
		assert.Equal(nmdcSearchResult["credit"].(credit.CreditMetadata).ResourceType, desc["credit"].(credit.CreditMetadata).ResourceType, "Resource credit resource type mismatch")
	}
}

func TestCreateDataObjectDescriptor(t *testing.T) {
	assert := assert.New(t)
	db := getMockNmdcDatabase(t)
	dataObject := DataObject{
		Id:          "nmdc:do-1234-abcde56789",
		Name:        "Test Data Object.txt",
		Description: "This is a test data object",
		FileSizeBytes: 123456,
		MD5Checksum: "d41d8cd98f00b204e9800998ecf8427e",
		URL:		 "https://data.microbiomedata.org/data/nmdc:do-1234-abcde56789",
		Type:	   "data_object",
	}
	studyCredit := credit.CreditMetadata{
		Identifier:   "original-study-id",
		ResourceType: "study",
		Url: 		"original-study-url",
	}
	nmdcDb := db.(*Database)
	descriptor := nmdcDb.createDataObjectDescriptor(dataObject, studyCredit)
	assert.Equal(dataObject.Id, descriptor["id"], "Data object descriptor ID mismatch")
	assert.Equal("test_data_object", descriptor["name"], "Data object descriptor name mismatch")
	assert.Equal("nmdc%3Ado-1234-abcde56789", descriptor["path"], "Data object descriptor path mismatch")
	assert.Equal("application/octet-stream", descriptor["mediatype"], "Data object descriptor media type mismatch")
	assert.Equal(dataObject.FileSizeBytes, descriptor["bytes"], "Data object descriptor size mismatch")
	creditMeta, ok := descriptor["credit"].(credit.CreditMetadata)
	assert.True(ok, "Data object descriptor credit type assertion failed")
	assert.Equal(dataObject.Id, creditMeta.Identifier, "Data object descriptor credit ID mismatch")
	assert.Equal(studyCredit.ResourceType, creditMeta.ResourceType, "Data object descriptor credit resource type mismatch")
	assert.Equal(dataObject.URL, creditMeta.Url, "Data object descriptor credit URL mismatch")
}

func TestCreditAndBiosampleForWorkflow(t *testing.T) {
	assert := assert.New(t)
	db := getMockNmdcDatabase(t)
	dbNmdc := db.(*Database)

	// check no workflow id
	relatedCredit, relatedBiosample, err := dbNmdc.creditAndBiosampleForWorkflow("")
	assert.NotNil(err, "creditAndBiosampleForWorkflow with no workflow ID should not error")
	assert.Equal(credit.CreditMetadata{}, relatedCredit, "creditAndBiosampleForWorkflow with no workflow ID should return no credit")
	assert.Nil(relatedBiosample, "creditAndBiosampleForWorkflow with no workflow ID should return no biosample")

	// check valid workflow id
	relatedCredit, relatedBiosample, err = dbNmdc.creditAndBiosampleForWorkflow("nmdc:wf-1234-abcde56789")
	assert.Nil(err, "creditAndBiosampleForWorkflow with valid workflow ID should not error")
	assert.Equal("", relatedCredit.Identifier,
		"creditAndBiosampleForWorkflow returned non-empty credit identifier")
	assert.Equal("Tara Oceans Mediterranean Sea Expedition 2013", relatedCredit.Titles[0].Title,
		"creditAndBiosampleForWorkflow returned incorrect credit name")
	assert.Equal("dataset", relatedCredit.ResourceType,
		"creditAndBiosampleForWorkflow returned incorrect credit resource type")
	assert.NotNil(relatedBiosample, "creditAndBiosampleForWorkflow with valid workflow ID should return biosample")
	assert.Equal("nmdc:bs-1234-abcde56789", relatedBiosample["id"],
		"creditAndBiosampleForWorkflow returned incorrect biosample ID")

	// check invalid workflow id indicating raw data
	relatedCredit, relatedBiosample, err = dbNmdc.creditAndBiosampleForWorkflow("nmdc:omg-invalid-workflow-id")
	assert.NotNil(err, "creditAndBiosampleForWorkflow with invalid workflow ID should error")
	assert.Equal(credit.CreditMetadata{}, relatedCredit, "creditAndBiosampleForWorkflow with invalid workflow ID should return no credit")
	assert.Nil(relatedBiosample, "creditAndBiosampleForWorkflow with invalid workflow ID should return no biosample")

	// check with invalid workflow id format
	relatedCredit, relatedBiosample, err = dbNmdc.creditAndBiosampleForWorkflow("invalid-workflow-id-format")
	assert.NotNil(err, "creditAndBiosampleForWorkflow with invalid workflow ID format should error")
	assert.Equal(credit.CreditMetadata{}, relatedCredit, "creditAndBiosampleForWorkflow with invalid workflow ID format should return no credit")
	assert.Nil(relatedBiosample, "creditAndBiosampleForWorkflow with invalid workflow ID format should return no biosample")

	// check workflow with too many studies
	relatedCredit, relatedBiosample, err = dbNmdc.creditAndBiosampleForWorkflow("nmdc:wf-too-many-studies")
	assert.NotNil(err, "creditAndBiosampleForWorkflow with workflow ID having too many studies should error")
	assert.Equal(credit.CreditMetadata{}, relatedCredit, "creditAndBiosampleForWorkflow with workflow ID having too many studies should return no credit")
	assert.Nil(relatedBiosample, "creditAndBiosampleForWorkflow with workflow ID having too many studies should return no biosample")
	
	// check workflow with too many biosamples
	relatedCredit, relatedBiosample, err = dbNmdc.creditAndBiosampleForWorkflow("nmdc:wf-too-many-biosamples")
	assert.NotNil(err, "creditAndBiosampleForWorkflow with workflow ID having too many biosamples should error")
	assert.Equal(credit.CreditMetadata{}, relatedCredit, "creditAndBiosampleForWorkflow with workflow ID having too many biosamples should return no credit")
	assert.Nil(relatedBiosample, "creditAndBiosampleForWorkflow with workflow ID having too many biosamples should return no biosample")
}

func TestCreditMetadataForStudy(t *testing.T) {
	assert := assert.New(t)
	db := Database{}
	study := Study{
		Id:		  "nmdc:sty-11-r2h77870",
		Title:    "Primary Title",
		AlternativeTitles: []string{
			"Secondary Title",
			"Tertiary Title",
		},
		CreditAssociations: []CreditAssociation{
			{
				Roles:       []string{"creator"},
				Person: PersonValue{
					Email: "jane.doe@example.com",
					Name:  "Jane Doe",
				},
			},
			{
				Roles:       []string{"contributor","tester"},
				Person: PersonValue{
					Name: "John Smith",
					Orcid: "0000-0002-1825-0097",
				},
				Type: "person",
			},
			{
				Roles:       []string{"singer"},
				Person: PersonValue{
					Name: "Cher",
				},
			},
		},
		AssociatedDois: []Doi{
			{
				Value:    "10.1234/example.doi.1",
				Category: "primary",
			},
			{
				Value:    "10.5678/example.doi.2",
				Category: "dataset_doi",
			},
		},
		FundingSources: []string{
			"Department of Energy",
			"NSF",
		},
	}
	credit := db.creditMetadataForStudy(study)
	assert.Equal("Jane Doe", credit.Contributors[0].Name,
		"Credit metadata first contributor name is incorrect")
	assert.Equal("Jane", credit.Contributors[0].GivenName,
		"Credit metadata first contributor given name is incorrect")
	assert.Equal("Doe", credit.Contributors[0].FamilyName,
		"Credit metadata first contributor family name is incorrect")
	assert.Equal("creator", credit.Contributors[0].ContributorRoles,
		"Credit metadata first contributor role is incorrect")
	assert.Equal("John Smith", credit.Contributors[1].Name,
		"Credit metadata second contributor name is incorrect")
	assert.Equal("John", credit.Contributors[1].GivenName,
		"Credit metadata second contributor given name is incorrect")
	assert.Equal("Smith", credit.Contributors[1].FamilyName,
		"Credit metadata second contributor family name is incorrect")
	assert.Equal("0000-0002-1825-0097", credit.Contributors[1].ContributorId,
		"Credit metadata second contributor ORCID is incorrect")
	assert.Equal("contributor,tester", credit.Contributors[1].ContributorRoles,
		"Credit metadata second contributor first role is incorrect")
	assert.Equal("Cher", credit.Contributors[2].Name,
		"Credit metadata third contributor name is incorrect")
	assert.Equal("Cher", credit.Contributors[2].GivenName,
		"Credit metadata third contributor given name is incorrect")
	assert.Equal("", credit.Contributors[2].FamilyName,
		"Credit metadata third contributor family name is incorrect")
	assert.Equal("singer", credit.Contributors[2].ContributorRoles,
		"Credit metadata third contributor role is incorrect")
	assert.Equal("Primary Title", credit.Titles[0].Title,
		"Credit metadata primary title is incorrect")
	assert.Equal("Secondary Title", credit.Titles[1].Title,
		"Credit metadata first alternative title is incorrect")
	assert.Equal("Tertiary Title", credit.Titles[2].Title,
		"Credit metadata second alternative title is incorrect")
	assert.Equal("10.1234/example.doi.1", credit.RelatedIdentifiers[0].Id,
		"Credit metadata primary DOI is incorrect")
	assert.Equal("IsCitedBy", credit.RelatedIdentifiers[0].RelationshipType,
		"Credit metadata primary DOI relationship type is incorrect")
	assert.Equal("", credit.RelatedIdentifiers[0].Description,
		"Credit metadata primary DOI description is incorrect")
	assert.Equal("10.5678/example.doi.2", credit.RelatedIdentifiers[1].Id,
		"Credit metadata dataset DOI is incorrect")
	assert.Equal("IsCitedBy", credit.RelatedIdentifiers[1].RelationshipType,
		"Credit metadata dataset DOI relationship type is incorrect")
	assert.Equal("Dataset DOI", credit.RelatedIdentifiers[1].Description,
		"Credit metadata dataset DOI description is incorrect")
	assert.Equal(2, len(credit.Funding),
		"Credit metadata funding source count is incorrect")
	assert.Equal("ROR:01bj3aw27", credit.Funding[0].Funder.OrganizationId,
		"Credit metadata first funding source organization ID is incorrect")
	assert.Equal("United States Department of Energy", credit.Funding[0].Funder.OrganizationName,
		"Credit metadata first funding source name is incorrect")
	assert.Equal("", credit.Funding[1].Funder.OrganizationId,
		"Unrecognized funding source should have empty Funder instance")
	assert.Equal("", credit.Funding[1].Funder.OrganizationName,
		"Unrecognized funding source should have empty Funder instance")
}

func TestPageNumberAndSize(t *testing.T) {
	assert := assert.New(t)
	num, size := pageNumberAndSize(0, 0)
	assert.Equal(1, num, "Page number for offset 0 and size 0 is incorrect")
	assert.Equal(100, size, "Page size for offset 0 and size 0 is incorrect")

	num, size = pageNumberAndSize(0, 10)
	assert.Equal(1, num, "Page number for offset 0 and size 10 is incorrect")
	assert.Equal(10, size, "Page size for offset 0 and size 10 is incorrect")

	num, size = pageNumberAndSize(25, 25)
	assert.Equal(2, num, "Page number for offset 25 and size 25 is incorrect")
	assert.Equal(25, size, "Page size for offset 25 and size 25 is incorrect")

	num, size = pageNumberAndSize(50, -1)
	assert.Equal(2, num, "Page number for offset 50 and size -1 is incorrect")
	assert.Equal(50, size, "Page size for offset 50 and size -1 is incorrect")
}

func TestFormatFromType(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		FileType 	 string
		ExpectedFormat string
	}{
		{"BAI File", "bai"},
		{"Metagenome Bins", "fasta"},
		{"unknown type", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.FileType, func(t *testing.T) {
			format := formatFromType(tt.FileType)
			assert.Equal(tt.ExpectedFormat, format,
				"Format for type %q is incorrect", tt.FileType)
		})
	}
}

func TestMimeTypeForFile(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		FileName     string
		ExpectedMIME string
	}{
		{"test.txt", "text/plain"},
		{"test.html", "text/html"},
		{"test.json", "application/json"},
		{"test.xml", "application/xml"},
		{"test.mp4", "video/mp4"},
		{"test.mp3", "audio/mpeg"},
		{"test.unknown", "application/octet-stream"},
	}
	for _, tt := range tests {
		t.Run(tt.FileName, func(t *testing.T) {
			mime := mimetypeForFile(tt.FileName)
			ok := strings.Contains(mime, tt.ExpectedMIME)
			assert.True(ok, "MIME type for %q is incorrect", tt.FileName)
		})
	}
}

func TestDataResourceName(t *testing.T) {
	assert := assert.New(t)
	inputOutputPairs := map[string]string{
		"name-with.valid_chars.txt": "name-with.valid_chars",
		"name with!invalid*%(chars).html": "name_with_invalid_chars_",
		"": "",
		"^.*$&%": "_",
	}
	for input, expectedOutput := range inputOutputPairs {
		t.Run(input, func(t *testing.T) {
			output := dataResourceName(input)
			assert.Equal(expectedOutput, output,
				"dataResourceName produced incorrect output")
		})
	}
}

func TestAddSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	db := Database{}
	validParams := map[string]any{
		"study_id": "nmdc:sty-11-r2h77870",
		"data_object_id": "nmdc:do-1234-abcde56789",
	}
	p := url.Values{}
	p.Set("existing_param", "existing_value")
	err := db.addSpecificSearchParameters(validParams, &p)
	assert.Nil(err, "Adding NMDC specific search parameters encountered an error")
	assert.Equal("nmdc:sty-11-r2h77870", p.Get("study_id"),
		"NMDC specific search parameter 'study_id' has incorrect value")
	assert.Equal("nmdc:do-1234-abcde56789", p.Get("data_object_id"),
		"NMDC specific search parameter 'data_object_id' has incorrect value")
	assert.Equal("existing_value", p.Get("existing_param"),
		"Existing search parameter value was modified incorrectly")

	invalidParams := []map[string]any{
		{"invalid_param": "some_value"},
		{"study_id": 12345}, // invalid type
		{"data_object_id": []string{"nmdc:do-1234-abcde56789"}}, // invalid type
		{"extra": "invalid_field,other_invalid_field"}, // invalid value
		{"extra": 23456}, // invalid type
		{"fields": "invalid_field"}, // invalid value
		{"fields": 34567}, // invalid type
	}
	for _, params := range invalidParams {
		p := url.Values{}
		p.Set("existing_param", "existing_value")
		err := db.addSpecificSearchParameters(params, &p)
		assert.NotNil(err, "Adding invalid NMDC specific search parameters did not return an error")
		assert.Equal(1, len(p),
			"Invalid NMDC specific search parameters modified the parameter list")
		assert.Equal("existing_value", p.Get("existing_param"),
			"Existing search parameter value was modified incorrectly")
	}
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
