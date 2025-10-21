package jdp

import (
	"context"
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

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humamux"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
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
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    endpoint: globus-jdp
    secret: ${DTS_JDP_SECRET}
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

// when valid JDP credentials are not available, use a mock database
var isMockDatabase bool = false

var mockJDPServer *httptest.Server
var mockJDPSecret string = "mock_shared_secret"

// Response structure for mock JDP Server
type SearchResults struct {
	Descriptors []map[string]any `json:"descriptors"`
}

// create a mock JDP server for testing
func createMockJDPServer() *httptest.Server {
	router := mux.NewRouter()
	api := humamux.New(router, huma.DefaultConfig("Mock JDP Server", "1.0.0"))

	huma.Register(api, huma.Operation{
		OperationID: "searchJDP",
		Method:      http.MethodGet,
		Path:        "/search",
		Security:    []map[string][]string{{"bearerAuth": {}}},
	}, func(ctx context.Context, input *struct {
		Authorization string `header:"Authorization"`
	}) (*SearchResults, error) {
		return &SearchResults{}, nil
	})

	return httptest.NewServer(router)
}

// create a mock JDP database for testing
func NewMockDatabase() (databases.Database, error) {
	return &Database{
		Secret:          mockJDPSecret,
		StagingRequests: make(map[uuid.UUID]StagingRequest),
	}, nil
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

	// check for existence of each variable. when not present, replace
	// instances of it in the yaml string with a test value
	for key, value := range testVars {
		if os.Getenv(key) == "" {
			yaml = os.Expand(yaml, func(yamlVar string) string {
				if yamlVar == key {
					isMockDatabase = true
					return value
				}
				return "${" + yamlVar + "}"
			})
		}
	}
	return yaml
}

// this function gets called at the beginning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(setTestEnvVars(jdpConfig)))
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
	if err != nil {
		panic(err)
	}
	if isMockDatabase {
		mockJDPServer = createMockJDPServer()
		err := databases.RegisterDatabase("jdp", NewMockDatabase)
		if err != nil {
			panic(err)
		}
	} else {
		err := databases.RegisterDatabase("jdp", NewDatabaseFunc(configData))
		if err != nil {
			panic(err)
		}
	}
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpointFromConfig)
}

// this function gets called after all tests have been run
func breakdown() {
	if mockJDPServer != nil {
		mockJDPServer.Close()
	}
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
	assert.Nil(err, "Failed to get config data for JDP database")
	jdpDb, err := NewDatabase(configData)
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
}

func TestNewDatabaseWithoutJDPSharedSecret(t *testing.T) {
	assert := assert.New(t)
	const jdpConfigNoSecret string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
  endpoint: globus-jdp
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    endpoint: globus-jdp
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfigNoSecret)))
	assert.Nil(err, "Failed to get config data for JDP database without shared secret")
	jdpDb, err := NewDatabase(configData)
	assert.Nil(jdpDb, "JDP database somehow created without shared secret available")
	assert.NotNil(err, "JDP database creation without shared secret encountered no error")
}

func TestNewDatabaseWithMissingEndpoint(t *testing.T) {
	assert := assert.New(t)
	const jdpConfigMissingEndpoint string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 60
  endpoint: globus-jdp
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    secret: ${DTS_JDP_SECRET}
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`
	// manually parse the config string to avoid validation errors because of the
	// missing endpoint
	bytes := []byte(setTestEnvVars(jdpConfigMissingEndpoint))
	bytes = []byte(os.ExpandEnv(string(bytes)))
	var configData config.ConfigData
	err := yaml.Unmarshal(bytes, &configData)
	assert.Nil(err, "Failed to unmarshal config data for JDP database with missing endpoint")
	jdpDb, err := NewDatabase(configData)
	assert.Nil(jdpDb, "JDP database somehow created with missing endpoint")
	assert.NotNil(err, "JDP database creation with missing endpoint encountered no error")
}

func TestNewDatabaseFunc(t *testing.T) {
	assert := assert.New(t)
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
	assert.Nil(err, "Failed to get config data for JDP database")
	createFunc := NewDatabaseFunc(configData)
	jdpDb, err := createFunc()
	assert.NotNil(jdpDb, "JDP database not created by factory function")
	assert.Nil(err, "JDP database creation by factory function encountered an error")
}

func TestSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
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
		configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
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
		results, err := db.Search(orcid, params)
		assert.True(len(results.Descriptors) > 0, "JDP search query returned no results")
		assert.Nil(err, "JDP search query encountered an error")
	}
}

func TestSaveLoad(t *testing.T) {
	assert := assert.New(t)
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
	assert.Nil(err, "Failed to get config data for JDP database")
	db, _ := NewDatabase(configData)

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
		configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
		assert.Nil(err, "Failed to get config data for JDP database")
		db, _ := NewDatabase(configData)
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

func TestDescriptors(t *testing.T) {
	assert := assert.New(t)
	if !isMockDatabase {
		orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
		configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
		assert.Nil(err, "Failed to get config data for JDP database")
		db, _ := NewDatabase(configData)
		params := databases.SearchParameters{
			Query: "prochlorococcus",
		}
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
	}
}

func TestAddSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	configData, err := config.GetConfigData([]byte(setTestEnvVars(jdpConfig)))
	assert.Nil(err, "Failed to get config data for JDP database")
	db, err := NewDatabase(configData)
	assert.NotNil(db, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")

	validParams := map[string]any{
		"extra": "project_id",
		"d":     "asc",
		"f":     "library",
		"include_private_data": 1,
		"s":     "title",
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
	expectedExtra := []string{"project_id"}
	assert.True(slices.Equal(expectedExtra, extraParams),
		"Specific search parameters 'extra' has incorrect values")

    invalidValues := []map[string]any{
		{"extra": "invlalid_extra"}, // not an allowed value
		{"extra": 123}, // should be a string
		{"d": "invalid_direction"}, // should be 'asc' or 'desc'
		{"d": 789}, // should be a string
		{"f": "invalid_field"}, // not an allowed value
		{"f": []int{1, 2, 3}}, // should be a string
		{"include_private_data": 5}, // should be 0 or 1
		{"include_private_data": "yes"}, // should be an integer
		{"s": "invalid_sort"}, // not an allowed value
		{"s": 456}, // should be a string
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

func TestPruneStagingRequests(t *testing.T) {
	assert := assert.New(t)
	db := &Database{
		StagingRequests: make(map[uuid.UUID]StagingRequest),
		DeleteAfter:     time.Minute * 30,
	}
	newUuid := uuid.New()
	db.StagingRequests[newUuid] = StagingRequest{
		Id:     1,
		Time:   time.Now(),
	}
	oldUuid := uuid.New()
	db.StagingRequests[oldUuid] = StagingRequest{
		Id:     2,
		Time:   time.Now().Add(-time.Hour),
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
		FileName    string
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

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
