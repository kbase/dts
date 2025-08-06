package nmdc

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

const nmdcConfig string = `
databases:
  nmdc:
    name: National Microbiome Data Collaborative
    organization: DOE
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

// since NMDC doesn't support search queries at this time, we search for
// data objects related to a study
var nmdcSearchParams map[string]any

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(nmdcConfig))
	databases.RegisterDatabase("nmdc", NewDatabase)
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpointFromConfig)

	// construct NMDC-specific search parameters for a study
	nmdcSearchParams = make(map[string]any)
	nmdcSearchParams["study_id"] = "nmdc:sty-11-r2h77870"
}

// this function gets called after all tests have been run
func breakdown() {
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	db, err := NewDatabase()
	assert.NotNil(db, "NMDC database not created")
	assert.Nil(err, "NMDC database creation encountered an error")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()

	params := databases.SearchParameters{
		Query:    "",
		Specific: nmdcSearchParams,
	}
	results, err := db.Search(orcid, params)
	assert.True(len(results.Descriptors) > 0, "NMDC search query returned no results")
	assert.Nil(err, "NMDC search query encountered an error")
}

func TestDescriptors(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()
	params := databases.SearchParameters{
		Query:    "",
		Specific: nmdcSearchParams,
	}
	results, _ := db.Search(orcid, params)
	fileIds := make([]string, len(results.Descriptors))
	for i, descriptor := range results.Descriptors {
		fileIds[i] = descriptor["id"].(string)
	}
	descriptors, err := db.Descriptors(orcid, fileIds[:10])
	assert.Nil(err, "NMDC resource query encountered an error")
	assert.True(len(descriptors) >= 10, // can include biosample metadata!
		"NMDC resource query didn't return all results")
	for i, desc := range descriptors[:10] {
		nmdcSearchResult := results.Descriptors[i]
		assert.Equal(nmdcSearchResult["id"], desc["id"], "Resource ID mismatch")
		assert.Equal(nmdcSearchResult["name"], desc["name"], "Resource name mismatch")
		assert.Equal(nmdcSearchResult["path"], desc["path"], "Resource path mismatch")
		assert.Equal(nmdcSearchResult["format"], desc["format"], "Resource format mismatch")
		assert.Equal(nmdcSearchResult["bytes"], desc["bytes"], "Resource size mismatch")
		assert.Equal(nmdcSearchResult["mediatype"], desc["mediatype"], "Resource media type mismatch")
		assert.Equal(nmdcSearchResult["credit"].(credit.CreditMetadata).Identifier, desc["credit"].(credit.CreditMetadata).Identifier, "Resource credit ID mismatch")
		assert.Equal(nmdcSearchResult["credit"].(credit.CreditMetadata).ResourceType, desc["credit"].(credit.CreditMetadata).ResourceType, "Resource credit resource type mismatch")
	}
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
