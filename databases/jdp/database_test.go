package jdp

import (
	"os"
	"testing"

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

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(jdpConfig))
	databases.RegisterDatabase("jdp", NewDatabase)
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint)
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	jdpDb, err := NewDatabase()
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
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
