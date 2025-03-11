package nmdc

import (
	"encoding/json"
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
var nmdcSearchParams map[string]json.RawMessage

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(nmdcConfig))
	databases.RegisterDatabase("nmdc", NewDatabase)
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint)

	// construct NMDC-specific search parameters for a study
	nmdcSearchParams = make(map[string]json.RawMessage)
	studyId, _ := json.Marshal("nmdc:sty-11-5tgfr349")
	nmdcSearchParams["study_id"] = studyId
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
	assert.True(len(results.Resources) > 0, "NMDC search query returned no results")
	assert.Nil(err, "NMDC search query encountered an error")
}

func TestResources(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase()
	params := databases.SearchParameters{
		Query:    "",
		Specific: nmdcSearchParams,
	}
	results, _ := db.Search(orcid, params)
	fileIds := make([]string, len(results.Resources))
	for i, res := range results.Resources {
		fileIds[i] = res.Descriptor()["id"].(string)
	}
	resources, err := db.Resources(orcid, fileIds[:10])
	assert.Nil(err, "NMDC resource query encountered an error")
	assert.Equal(10, len(resources),
		"NMDC resource query didn't return requested number of results")
	for i, resource := range resources {
		resDesc := resource.Descriptor()
		jdpSearchResult := results.Resources[i].Descriptor()
		assert.Equal(jdpSearchResult["id"], resDesc["id"], "Resource ID mismatch")
		assert.Equal(jdpSearchResult["name"], resDesc["name"], "Resource name mismatch")
		assert.Equal(jdpSearchResult["path"], resDesc["path"], "Resource path mismatch")
		assert.Equal(jdpSearchResult["format"], resDesc["format"], "Resource format mismatch")
		assert.Equal(jdpSearchResult["bytes"], resDesc["bytes"], "Resource size mismatch")
		assert.Equal(jdpSearchResult["mediatype"], resDesc["mediaType"], "Resource media type mismatch")
		assert.Equal(jdpSearchResult["credit"].(credit.CreditMetadata).Identifier, resDesc["credit"].(credit.CreditMetadata).Identifier, "Resource credit ID mismatch")
		assert.Equal(jdpSearchResult["credit"].(credit.CreditMetadata).ResourceType, resDesc["credit"].(credit.CreditMetadata).ResourceType, "Resource credit resource type mismatch")
	}
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
