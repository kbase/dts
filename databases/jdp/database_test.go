package jdp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humamux"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/credit"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

const jdpConfig string = `
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institue
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
	if isMockDatabase {
		mockJDPServer = createMockJDPServer()
		err := databases.RegisterDatabase("jdp", NewMockDatabase)
		if err != nil {
			panic(err)
		}
	} else {
		err := databases.RegisterDatabase("jdp", NewDatabase)
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
	jdpDb, err := NewDatabase()
	if isMockDatabase {
		assert.Nil(jdpDb, "JDP database created without valid credentials")
		assert.NotNil(err, "JDP database creation without valid credentials encountered no error")
	} else {
		assert.NotNil(jdpDb, "JDP database not created with valid credentials")
		assert.Nil(err, "JDP database creation with valid credentials encountered an error")
	}
}

func TestNewDatabaseWithoutJDPSharedSecret(t *testing.T) {
	assert := assert.New(t)
	jdpSecret := os.Getenv("DTS_JDP_SECRET")
	os.Unsetenv("DTS_JDP_SECRET")
	jdpDb, err := NewDatabase()
	os.Setenv("DTS_JDP_SECRET", jdpSecret)
	assert.Nil(jdpDb, "JDP database somehow created without shared secret available")
	assert.NotNil(err, "JDP database creation without shared secret encountered no error")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()
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

func TestSearchByIMGTaxonOID(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()
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

func TestDescriptors(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()
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

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
