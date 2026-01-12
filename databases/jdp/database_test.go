package jdp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

const jdpConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
  endpoint: globus-jdp
credentials:
  jdp:
    secret: ${DTS_JDP_SECRET}
  globus:
    id: ${DTS_GLOBUS_CLIENT_ID}
    secret: ${DTS_GLOBUS_CLIENT_SECRET}
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    endpoint: globus-jdp
    credential: jdp
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    credential: globus
`

const jdpDbConfig string = `
endpoint: globus-jdp
credential:
  secret: ${DTS_JDP_SECRET}
delete_after: 86400
`

// when valid JDP credentials are not available, use a mock database
var isMockDatabase bool = false

var mockJDPServer *httptest.Server
var mockJDPSecret string = "mock_shared_secret"
var mockOrcId string = "0000-0000-9876-0000"
var mockStagedFileId = 12345

const mockResponseBody string = `{
		"organisms": [
			{
				"id": "org456",
				"name": "Test Organism",
				"title": "His Royal Testness",
				"files": [
					{
						"_id": "file123",
						"file_name": "testfile.txt",
						"file_path": "/data",
						"type": "text/plain",
						"file_size": 2048,
						"metadata": {
							"analysis_project_id": 7890,
							"img": {
								"taxon_oid": "A321"
							}
						}
					}
				]
			}
		]
	}`

// Response structure for mock JDP Server
type SearchResults struct {
	Descriptors []map[string]any `json:"descriptors"`
}

// create a mock JDP server for testing
func createMockJDPServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/mock_get":
			message := r.URL.Query().Get("message")
			// Check for orcid parameter and validate against auth header if present
			orcid := r.URL.Query().Get("orcid")
			if orcid != "" {
				authHeader := r.Header.Get("Authorization")
				if authHeader == "" || !strings.Contains(authHeader, orcid) {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{"error": "ORCID mismatch with authorization"})
					return
				}
			}
			if message == "" {
				message = "This is a mock GET response from the JDP server."
			}
			response := struct {
				Message string `json:"message"`
			}{Message: message}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)

		case "/mock_post":
			var requestData struct {
				Message string `json:"message"`
			}
			err := json.NewDecoder(r.Body).Decode(&requestData)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
				return
			}
			response := struct {
				Message string `json:"message"`
			}{Message: requestData.Message}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)

		case "/search":
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.Contains(authHeader, mockOrcId) {
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]string{"error": "ORCID mismatch with authorization"})
				return
			}
			query := r.URL.Query().Get("q")
			response := `{
				"organisms": []
			}`
			if query == "file123" {
				specifics := r.URL.Query().Get("s")
				if specifics != "" && specifics == "title" {
					extras := r.URL.Query().Get("extra")
					if extras != "taxon_oid,project_id" {
						// Return a mock search result
						response = mockResponseBody
					}
				}
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(response))

		default:
			// Handle paths like "/request_archived_files/requests/12345"
			if strings.HasPrefix(r.URL.Path, "/request_archived_files/requests/") {
				// Extract the ID from the path
				id := strings.TrimPrefix(r.URL.Path, "/request_archived_files/requests/")
				response := struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				}{
					ID:     id,
					Status: "pending",
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(response)
				return
			} else if strings.HasPrefix(r.URL.Path, "/request_archived_files/") {
				// Handle POST requests to "/request_archived_files/"
				authHeader := r.Header.Get("Authorization")
				if authHeader != fmt.Sprintf("Token %s_%s", mockOrcId, mockJDPSecret) {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized"})
					return
				}
				if r.Method == http.MethodPost {
					var requestData struct {
						ID string `json:"id"`
					}
					err := json.NewDecoder(r.Body).Decode(&requestData)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
						return
					}
					response := struct {
						RequestId int `json:"request_id"`
					}{
						RequestId: mockStagedFileId,
					}
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(response)
					return
				}
			} else if strings.HasPrefix(r.URL.Path, "/search/by_file_ids/") {
				// Return file descriptors for given file IDs
				authHeader := r.Header.Get("Authorization")
				if authHeader == "" || !strings.Contains(authHeader, mockOrcId) {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]string{"error": "ORCID mismatch with authorization"})
					return
				}
				var requestData struct {
					Ids                []string `json:"ids"`
					Aggregations       bool     `json:"aggregations"`
					IncludePrivateData int      `json:"include_private_data"`
				}
				err := json.NewDecoder(r.Body).Decode(&requestData)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request payload"})
					return
				}
				var found bool = false
				for _, id := range requestData.Ids {
					if id == "file123" {
						found = true
					}
				}
				responseBody := `{
					"organisms": []
				}`
				if found {
					responseBody = mockResponseBody
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(responseBody))
				return
			} else {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{
					"error": "Not found",
				})
			}
		}
	}))
}

// create a mock JDP database for testing
func NewMockDatabase(baseUrl string) func() (databases.Database, error) {
	return func() (databases.Database, error) {
		return &Database{
			BaseURL:         baseUrl,
			Secret:          mockJDPSecret,
			StagingRequests: make(map[uuid.UUID]StagingRequest),
		}, nil
	}
}

// helper function replaces embedded environment variables in yaml strings
// when they don't exist in the environment
func setTestEnvVars(yaml string) string {
	testVars := map[string]string{
		"DTS_JDP_SECRET":           mockJDPSecret,
		"DTS_GLOBUS_TEST_ENDPOINT": "5e5f7d4e-3f4b-11eb-9ac6-0a58a9feac02",
		"DTS_GLOBUS_CLIENT_ID":     "test_client_id",
		"DTS_GLOBUS_CLIENT_SECRET": "test_client_secret",
	}

	// check for existence of each variable. when any are missing, set all
	// to the values defined above to use the mock JDP server
	for key := range testVars {
		if os.Getenv(key) == "" {
			isMockDatabase = true
			break
		}
	}
	if os.Getenv("DTS_TEST_WITH_MOCK_SERVICES") == "true" {
		for key, value := range testVars {
			yaml = strings.ReplaceAll(yaml, "${"+key+"}", value)
		}
		return yaml
	}
	if isMockDatabase {
		panic("Environment variables for JDP tests not set; use DTS_TEST_WITH_MOCK_SERVICES=true to run with mock services")
	}
	return yaml
}

// this function gets called at the beginning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(setTestEnvVars(jdpConfig)))
	var jdpConfig Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &jdpConfig)
	if err != nil {
		panic(err)
	}

	// create a mock JDP server (useful even when we have a real one)
	mockJDPServer = createMockJDPServer()
	if err != nil {
		panic(err)
	}

	if isMockDatabase {
		err = databases.RegisterDatabase("jdp", NewMockDatabase(mockJDPServer.URL))
	} else {
		var confMap map[string]any
		err = mapstructure.Decode(jdpConfig, &confMap)
		if err != nil {
			panic("Couldn't decode config to map: " + err.Error())
		}
		err = databases.RegisterDatabase("jdp", DatabaseConstructor(confMap))
	}
	if err != nil {
		panic(err)
	}
	endpoints.RegisterEndpointProvider("globus", globus.EndpointConstructor)
}

// this function gets called after all tests have been run
func breakdown() {
	if mockJDPServer != nil {
		mockJDPServer.Close()
	}
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	var conf Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &conf)
	assert.Nil(err, "Failed to unmarshal JDP database config")
	jdpDb, err := NewDatabase(conf)
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
	endpointName := jdpDb.EndpointNames()
	assert.Equal([]string{"globus-jdp"}, endpointName, "JDP database returned incorrect endpoint name")
}

func TestNewDatabaseWithoutJDPCredential(t *testing.T) {
	assert := assert.New(t)
	const jdpConfigNoCredential string = `
name: JGI Data Portal
organization: Joint Genome Institute
endpoint: globus-jdp
`
	var conf Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpConfigNoCredential)), &conf)
	assert.Nil(err, "Failed to unmarshal JDP database config without credential")
	assert.Nil(err, "Failed to get config data for JDP database without credential")
	jdpDb, err := NewDatabase(conf)
	assert.Nil(jdpDb, "JDP database somehow created without credential")
	assert.NotNil(err, "JDP database creation without shared secret encountered no error")
}

func TestNewDatabaseWithMissingEndpoint(t *testing.T) {
	assert := assert.New(t)
	const jdpConfigMissingEndpoint string = `
name: JGI Data Portal
organization: Joint Genome Institute
credential:
  secret: ${DTS_JDP_SECRET}
`
	// manually parse the config string to avoid validation errors because of the
	// missing endpoint
	bytes := []byte(setTestEnvVars(jdpConfigMissingEndpoint))
	bytes = []byte(os.ExpandEnv(string(bytes)))
	var configData Config
	err := yaml.Unmarshal(bytes, &configData)
	assert.Nil(err, "Failed to unmarshal config data for JDP database with missing endpoint")
	jdpDb, err := NewDatabase(configData)
	assert.Nil(jdpDb, "JDP database somehow created with missing endpoint")
	assert.NotNil(err, "JDP database creation with missing endpoint encountered no error")
}

func TestNewDatabaseFunc(t *testing.T) {
	assert := assert.New(t)
	var configData Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
	assert.Nil(err, "Failed to get config data for JDP database")
	var confMap map[string]any
	err = mapstructure.Decode(configData, &confMap)
	assert.Nil(err, "Failed to decode config data for JDP database")
	createFunc := DatabaseConstructor(confMap)
	jdpDb, err := createFunc()
	assert.NotNil(jdpDb, "JDP database not created by factory function")
	assert.Nil(err, "JDP database creation by factory function encountered an error")
}

func TestSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	var configData Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
	assert.Nil(err, "Failed to get config data for JDP database")
	db, err := NewDatabase(configData)
	assert.NotNil(db, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")

	params := db.SpecificSearchParameters()
	// check a few values
	extraString, ok := params["extra"].([]string)
	assert.True(ok, "Specific search parameters 'extra' is not a string slice")
	expectedExtra := []string{"img_taxon_oid", "project_id"}
	assert.True(slices.Equal(expectedExtra, extraString),
		"Specific search parameters 'extra' has incorrect values")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	if !isMockDatabase {
		orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
		var configData Config
		err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
		assert.Nil(err, "Failed to get config data for JDP database")
		db, _ := NewDatabase(configData)
		params := databases.SearchParameters{
			Query: "prochlorococcus",
			Pagination: struct {
				Offset, MaxNum int
			}{
				Offset: 1,
				MaxNum: 50,
			},
		}
		assert.NotNil(db)
		results, err := db.Search(orcid, params)
		assert.True(len(results.Descriptors) > 0, "JDP search query returned no results")
		assert.Nil(err, "JDP search query encountered an error")
	} else {
		orcid := mockOrcId
		db := Database{
			BaseURL:         mockJDPServer.URL,
			Secret:          mockJDPSecret,
			StagingRequests: make(map[uuid.UUID]StagingRequest),
			DeleteAfter:     time.Duration(1) * time.Hour,
		}
		params := databases.SearchParameters{
			Query: "file123",
			Specific: map[string]any{
				"s":     "title",
				"extra": "img_taxon_oid,project_id",
			},
		}
		results, err := db.Search(orcid, params)
		assert.Nil(err, "JDP mock search query encountered an error")
		assert.Equal(1, len(results.Descriptors), "JDP mock search query returned no results")
		assert.Equal("JDP:file123", results.Descriptors[0]["id"], "JDP mock search query returned incorrect file ID")

		// test with query that returns no results
		params = databases.SearchParameters{
			Query: "nonexistentfile",
		}
		results, err = db.Search(orcid, params)
		assert.Nil(err, "JDP mock search query encountered an error")
		assert.Equal(0, len(results.Descriptors), "JDP mock search query returned results for nonexistent file")
	}
}

func TestStageFiles(t *testing.T) {
	assert := assert.New(t)
	mockServer := createMockJDPServer()
	defer mockServer.Close()
	db := Database{
		BaseURL:         mockServer.URL,
		Secret:          mockJDPSecret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
		DeleteAfter:     time.Duration(1) * time.Hour,
	}
	fileIds := []string{"file1", "file2"}
	id, err := db.StageFiles(mockOrcId, fileIds)
	assert.Nil(err, "Database StageFiles encountered an error")
	assert.NotNil(id, "Database StageFiles returned nil ID")
	assert.Equal(mockStagedFileId, db.StagingRequests[id].Id, "Database StageFiles returned incorrect ID")
}

func TestStagingStatus(t *testing.T) {
	assert := assert.New(t)
	mockServer := createMockJDPServer()
	defer mockServer.Close()
	db := Database{
		BaseURL:         mockServer.URL,
		Secret:          mockJDPSecret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
		DeleteAfter:     time.Duration(1) * time.Hour,
	}
	req1 := StagingRequest{
		Id:   789,
		Time: time.Now(),
	}
	req2 := StagingRequest{
		Id:   4,
		Time: time.Now(),
	}
	uuid1 := uuid.New()
	uuid2 := uuid.New()
	db.StagingRequests[uuid1] = req1
	db.StagingRequests[uuid2] = req2
	status, err := db.StagingStatus(uuid2)
	assert.Nil(err, "Database StagingStatus encountered an error")
	assert.NotNil(status, "Database StagingStatus returned nil status")
	assert.Equal(databases.StagingStatusActive, status, "Database StagingStatus returned incorrect status")
}

func TestFinalize(t *testing.T) {
	assert := assert.New(t)
	db := Database{}
	err := db.Finalize("", uuid.UUID{})
	assert.Nil(err, "Database Finalize encountered an error")
}

func TestLocalUser(t *testing.T) {
	assert := assert.New(t)
	db := Database{}
	localUser, err := db.LocalUser("test-orcid")
	assert.Nil(err, "Database LocalUser encountered an error")
	assert.Equal("localuser", localUser, "Database LocalUser returned incorrect value")
}

func TestSaveLoad(t *testing.T) {
	assert := assert.New(t)
	var configData Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
	assert.Nil(err, "Failed to unmarshal JDP database config for save/load test")
	db, err := NewDatabase(configData)
	assert.NotNil(db, "JDP database not created for save/load test")
	assert.Nil(err, "JDP database creation for save/load test encountered an error")

	// save the database state
	state, err := db.Save()
	assert.Nil(err, "JDP database save encountered an error")
	assert.Equal("jdp", state.Name,
		"JDP database save returned incorrect database name")
	assert.True(len(state.Data) > 0, "JDP database save returned empty data")

	// load the saved state into a new database instance
	newDb, _ := NewDatabase(configData)
	err = newDb.Load(state)
	assert.Nil(err, "JDP database load encountered an error")
}

func TestSearchByIMGTaxonOID(t *testing.T) {
	assert := assert.New(t)
	if !isMockDatabase {
		orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
		var configData Config
		err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
		assert.Nil(err, "Failed to get config data for JDP database")
		db, err := NewDatabase(configData)
		assert.NotNil(db, "JDP database not created")
		assert.Nil(err, "JDP database creation encountered an error")
		params := databases.SearchParameters{
			Query: "2582580701",
			Pagination: struct {
				Offset, MaxNum int
			}{
				Offset: 1,
				MaxNum: 50,
			},
			Specific: map[string]any{
				"f":     "img_taxon_oid",
				"extra": "img_taxon_oid",
			},
		}
		results, err := db.Search(orcid, params)
		assert.True(len(results.Descriptors) > 0, "JDP search query returned no results")
		assert.Nil(err, "JDP search query encountered an error")
	}
}

func TestSourcesFromMetadata(t *testing.T) {
	assert := assert.New(t)
	metadataJson := `{
		"proposal": {
			"pi": {
				"last_name": "Doe",
				"first_name": "Jane",
				"middle_name": "Any",
				"email_address": "jane.doe@example.com",
				"institution": "University of Testing",
				"country": "USA"
			},
			"award_doi": "10.1234/test.award.doi"
		}
	}`
	var metadata Metadata
	err := json.Unmarshal([]byte(metadataJson), &metadata)
	assert.Nil(err, "Failed to unmarshal metadata JSON")
	sources := sourcesFromMetadata(metadata)
	assert.Equal(1, len(sources), "Incorrect number of sources extracted from metadata")
	sourceMap, ok := sources[0].(map[string]any)
	assert.True(ok, "Source extracted from metadata is not a map[string]any")
	assert.Equal(3, len(sourceMap), "Incorrect number of source fields extracted from metadata")
	title, ok := sourceMap["title"].(string)
	assert.True(ok, "PI name extracted from metadata is not a string")
	assert.Equal("Doe, Jane Any (University of Testing, USA)", title, "Incorrect PI name extracted from metadata")
	path, ok := sourceMap["path"].(string)
	assert.True(ok, "Institution extracted from metadata is not a string")
	assert.Equal("https://doi.org/10.1234/test.award.doi", path, "Incorrect award DOI extracted from metadata")
	email, ok := sourceMap["email"].(string)
	assert.True(ok, "Email extracted from metadata is not a string")
	assert.Equal("jane.doe@example.com", email, "Incorrect email extracted from metadata")
}

func TestDataResourceName(t *testing.T) {
	assert := assert.New(t)
	inputOutputPairs := map[string]string{
		"name-with.valid_chars.txt":        "name-with.valid_chars",
		"name with!invalid*%($chars).html": "name_with_invalid_chars_",
		"":                                 "",
		"^.*$&%":                           "_",
	}
	for input, expectedOutput := range inputOutputPairs {
		t.Run(input, func(t *testing.T) {
			assert.Equal(expectedOutput, dataResourceName(input),
				"Data resource name mapping is incorrect")
		})
	}
}

func TestGet(t *testing.T) {
	assert := assert.New(t)

	// set up a database instance pointing to the mock JDP server
	db := &Database{
		BaseURL:         mockJDPServer.URL,
		Secret:          mockJDPSecret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
	}

	// perform a GET request to the mock endpoint without arguments
	responseBody, err := db.get("/mock_get", url.Values{})
	assert.Nil(err, "JDP database GET request encountered an error")
	var responseData struct {
		Message string `json:"message"`
	}
	err = json.Unmarshal(responseBody, &responseData)
	assert.Nil(err, "JDP database GET response unmarshalling encountered an error")
	expectedMessage := "This is a mock GET response from the JDP server."
	assert.Equal(expectedMessage, responseData.Message,
		"JDP database GET response message is incorrect")

	// perform a GET request to the mock endpoint with a message argument
	values := url.Values{}
	values.Add("message", "Hello, JDP!")
	values.Add("orcid", mockOrcId)
	responseBody, err = db.get("/mock_get", values)
	assert.Nil(err, "JDP database GET request with argument encountered an error")
	err = json.Unmarshal(responseBody, &responseData)
	assert.Nil(err, "JDP database GET response with argument unmarshalling encountered an error")
	expectedMessage = "Hello, JDP!"
	assert.Equal(expectedMessage, responseData.Message,
		"JDP database GET response message with argument is incorrect")
}

func TestPost(t *testing.T) {
	assert := assert.New(t)

	// set up a database instance pointing to the mock JDP server
	db := &Database{
		BaseURL:         mockJDPServer.URL,
		Secret:          mockJDPSecret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
	}

	// perform a POST request to the mock endpoint
	responseBody, err := db.post("/mock_post", "", bytes.NewBuffer([]byte(`{"message": "Hello, JDP!"}`)))
	assert.Nil(err, "JDP database POST request encountered an error")
	var responseData struct {
		Message string `json:"message"`
	}
	err = json.Unmarshal(responseBody, &responseData)
	assert.Nil(err, "JDP database POST response unmarshalling encountered an error")
	expectedMessage := "Hello, JDP!"
	assert.Equal(expectedMessage, responseData.Message,
		"JDP database POST response message with argument is incorrect")
}

func TestDescriptors(t *testing.T) {
	assert := assert.New(t)
	if !isMockDatabase {
		orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
		var configData Config
		err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
		assert.Nil(err, "Failed to unmarshal JDP database config for save/load test")
		db, _ := NewDatabase(configData)
		params := databases.SearchParameters{
			Query: "prochlorococcus",
		}
		assert.NotNil(db)
		results, _ := db.Search(orcid, params)
		fileIds := make([]string, len(results.Descriptors))
		for i, descriptor := range results.Descriptors {
			fileIds[i] = descriptor["id"].(string)
		}
		descriptors, err := db.Descriptors(orcid, fileIds[:10])
		assert.Nil(err, "JDP resource query encountered an error")
		assert.Equal(10, len(descriptors),
			"JDP resource query didn't return requested number of results")
		for i, desc := range descriptors {
			jdpSearchResult := results.Descriptors[i]
			assert.Equal(jdpSearchResult["id"], desc["id"], "Resource ID mismatch")
			assert.Equal(jdpSearchResult["name"], desc["name"], "Resource name mismatch")
			assert.Equal(jdpSearchResult["path"], desc["path"], "Resource path mismatch")
			assert.Equal(jdpSearchResult["format"], desc["format"], "Resource format mismatch")
			assert.Equal(jdpSearchResult["bytes"], desc["bytes"], "Resource size mismatch")
			assert.Equal(jdpSearchResult["mediatype"], desc["mediatype"], "Resource media type mismatch")
			assert.Equal(jdpSearchResult["credit"].(credit.CreditMetadata).Identifier, desc["credit"].(credit.CreditMetadata).Identifier, "Resource credit ID mismatch")
			assert.Equal(jdpSearchResult["credit"].(credit.CreditMetadata).ResourceType, desc["credit"].(credit.CreditMetadata).ResourceType, "Resource credit resource type mismatch")
		}
	} else {
		orcid := mockOrcId
		db := Database{
			BaseURL:         mockJDPServer.URL,
			Secret:          mockJDPSecret,
			StagingRequests: make(map[uuid.UUID]StagingRequest),
			DeleteAfter:     time.Duration(1) * time.Hour,
		}
		descriptors, err := db.Descriptors(orcid, []string{"JDP:file123"})
		assert.Nil(err, "JDP database Descriptors request encountered an error")
		assert.Equal(1, len(descriptors), "JDP database Descriptors request returned incorrect number of results")
		assert.Equal("JDP:file123", descriptors[0]["id"], "JDP database Descriptors request returned incorrect file ID")

		// test with non-existent file ID
		descriptors, err = db.Descriptors(orcid, []string{"JDP:nonexistent"})
		assert.NotNil(err, "JDP database Descriptors request for nonexistent file ID encountered an error")
		assert.Equal(0, len(descriptors), "JDP database Descriptors request for nonexistent file ID returned results")
	}
}

func TestAddSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	var configData Config
	err := yaml.Unmarshal([]byte(setTestEnvVars(jdpDbConfig)), &configData)
	assert.Nil(err, "Failed to get config data for JDP database")
	db, err := NewDatabase(configData)
	assert.NotNil(db, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")

	validParams := map[string]any{
		"extra":                "project_id,img_taxon_oid",
		"d":                    "asc",
		"f":                    "library",
		"include_private_data": 1,
		"s":                    "title",
	}
	urlValues := url.Values{}
	urlValues.Add("foo", "bar")
	urlValues.Add("baz", "qux")
	jdpDB, ok := db.(*Database)
	assert.NotNil(jdpDB, "Failed to cast db to *Database")
	assert.True(ok, "Database cast encountered an error")

	err = jdpDB.addSpecificSearchParameters(validParams, &urlValues)
	assert.Nil(err, "Adding specific search parameters encountered an error")
	assert.Equal("bar", urlValues.Get("foo"), "Existing URL parameter 'foo' was modified")
	assert.Equal("qux", urlValues.Get("baz"), "Existing URL parameter 'baz' was modified")
	extraParams := urlValues["extra"]
	expectedExtra := []string{"project_id", "img_taxon_oid"}
	assert.True(slices.Equal(expectedExtra, extraParams),
		"Specific search parameters 'extra' has incorrect values")

	invalidValues := []map[string]any{
		{"extra": "invalid_extra"},      // not an allowed value
		{"extra": 123},                  // should be a string
		{"d": "invalid_direction"},      // should be 'asc' or 'desc'
		{"d": 789},                      // should be a string
		{"f": "invalid_field"},          // not an allowed value
		{"f": []int{1, 2, 3}},           // should be a string
		{"include_private_data": 5},     // should be 0 or 1
		{"include_private_data": "yes"}, // should be an integer
		{"s": "invalid_sort"},           // not an allowed value
		{"s": 456},                      // should be a string
		{"unknown_param": "some_value"}, // unknown parameter
	}

	for _, invalidParams := range invalidValues {
		t.Run(fmt.Sprintf("%v", invalidParams), func(t *testing.T) {
			urlValues = url.Values{}
			err = jdpDB.addSpecificSearchParameters(invalidParams, &urlValues)
			assert.NotNil(err, "Adding invalid specific search parameters did not return an error")
			var invalidParamErr *databases.InvalidSearchParameter
			assert.True(errors.As(err, &invalidParamErr), "Expected InvalidSearchParameter error type")
		})
	}
}

func TestDescriptorFromOrganismAndFile(t *testing.T) {
	assert := assert.New(t)
	file := File{
		Id:           "file123",
		Name:         "testfile.txt",
		Path:         "/data/",
		Size:         2048.0,
		Owner:        "jdoe",
		AddedDate:    "01022020",
		ModifiedDate: "02022020",
		PurgeDate:    "03022020",
		Date:         "04022020",
		Status:       "active",
		Type:         "txt",
		MD5Sum:       "abc123def456ghi789jkl012mno345pq",
		User:         "jdoe",
		Group:        "users",
		Permissions:  "rw-r--r--",
		DataGroup:    "data",
	}
	organism := Organism{
		Id:    "org456",
		Name:  "Test Organism",
		Title: "His Royal Testness",
		Files: []File{file},
	}
	descriptor := descriptorFromOrganismAndFile(organism, file)
	assert.NotNil(descriptor, "Descriptor creation returned nil")
	assert.Equal("JDP:file123", descriptor["id"], "Descriptor ID is incorrect")
	assert.Equal("testfile", descriptor["name"], "Descriptor name is incorrect")
	assert.Equal("/data/testfile.txt", descriptor["path"], "Descriptor path is incorrect")
	assert.Equal("text", descriptor["format"], "Descriptor format is incorrect")
	ok := strings.Contains(descriptor["mediatype"].(string), "text/plain")
	assert.True(ok, "Descriptor media type is incorrect")
	assert.Equal(int(2048), descriptor["bytes"], "Descriptor size is incorrect")
	assert.Equal("JDP:file123", descriptor["credit"].(credit.CreditMetadata).Identifier, "Descriptor credit ID is incorrect")
	assert.Equal("dataset", descriptor["credit"].(credit.CreditMetadata).ResourceType, "Descriptor credit resource type is incorrect")

}

func TestDescriptorsFromResponseBody(t *testing.T) {
	assert := assert.New(t)
	descriptors, err := descriptorsFromResponseBody([]byte(mockResponseBody), nil)
	assert.Nil(err, "Parsing descriptors from response body encountered an error")
	assert.Equal(1, len(descriptors), "Incorrect number of descriptors parsed from response body")
	descriptor := descriptors[0]
	assert.Equal("JDP:file123", descriptor["id"], "Parsed descriptor ID is incorrect")
	assert.Equal("testfile", descriptor["name"], "Parsed descriptor name is incorrect")
	assert.Equal("/data/testfile.txt", descriptor["path"], "Parsed descriptor path is incorrect")
	assert.Equal("text", descriptor["format"], "Parsed descriptor format is incorrect")
	ok := strings.Contains(descriptor["mediatype"].(string), "text/plain")
	assert.True(ok, "Parsed descriptor media type is incorrect")
	assert.Equal(int(2048), descriptor["bytes"], "Parsed descriptor size is incorrect")
	assert.Equal("JDP:file123", descriptor["credit"].(credit.CreditMetadata).Identifier, "Parsed descriptor credit ID is incorrect")
	assert.Equal("dataset", descriptor["credit"].(credit.CreditMetadata).ResourceType, "Parsed descriptor credit resource type is incorrect")

	// response with extra fields
	extraFields := []string{"img_taxon_oid", "project_id"}
	descriptors, err = descriptorsFromResponseBody([]byte(mockResponseBody), extraFields)
	assert.Nil(err, "Parsing descriptors with extra fields from response body encountered an error")
	assert.Equal(1, len(descriptors), "Incorrect number of descriptors parsed from response body with extra fields")
	descriptor = descriptors[0]
	extraValue, exists := descriptor["extra"].(map[string]any)["img_taxon_oid"]
	assert.True(exists, "Extra field 'img_taxon_oid' not found in parsed descriptor")
	assert.Equal("A321", extraValue, "Extra field 'img_taxon_oid' has incorrect value in parsed descriptor")
	extraValue, exists = descriptor["extra"].(map[string]any)["project_id"]
	assert.True(exists, "Extra field 'project_id' not found in parsed descriptor")
	assert.Equal("org456", extraValue, "Extra field 'project_id' has incorrect value in parsed descriptor")

}

func TestPageNumberAndSize(t *testing.T) {
	assert := assert.New(t)
	num, size := pageNumberAndSize(0, 0)
	assert.Equal(1, num, "Page number for offset 0 and size 0 is incorrect")
	assert.Equal(50, size, "Page size for offset 0 and size 0 is incorrect")

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

func TestPruneStagingRequests(t *testing.T) {
	assert := assert.New(t)
	db := &Database{
		StagingRequests: make(map[uuid.UUID]StagingRequest),
		DeleteAfter:     time.Minute * 30,
	}
	newUuid := uuid.New()
	db.StagingRequests[newUuid] = StagingRequest{
		Id:   1,
		Time: time.Now(),
	}
	oldUuid := uuid.New()
	db.StagingRequests[oldUuid] = StagingRequest{
		Id:   2,
		Time: time.Now().Add(-time.Hour),
	}
	db.pruneStagingRequests()
	_, existsNew := db.StagingRequests[newUuid]
	_, existsOld := db.StagingRequests[oldUuid]
	assert.True(existsNew, "New staging request was incorrectly pruned")
	assert.False(existsOld, "Old staging request was not pruned")
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
		{"test.xml", "xml"},
		{"test.mp4", "video/mp4"},
		{"test.mp3", "audio/mpeg"},
		{"test.unknown", "application/octet-stream"},
	}
	for _, tt := range tests {
		t.Run(tt.FileName, func(t *testing.T) {
			mime := mimetypeForFile(tt.FileName)
			ok := strings.Contains(mime, tt.ExpectedMIME)
			assert.True(ok, "MIME type for %q is incorrect; expected %q but got %q", tt.FileName, tt.ExpectedMIME, mime)
		})
	}
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
