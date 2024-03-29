package jdp

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/dnaeon/go-vcr.v3/recorder"

	"github.com/kbase/dts/config"
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
    url: https://files.jgi.doe.gov
    endpoint: globus-jdp
    auth:
      client_id: ${JGI_CLIENT_ID}
      client_secret: ${JGI_CLIENT_SECRET}
endpoints:
  globus-jdp:
    name: Globus NERSC DTN
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

var vcr *recorder.Recorder

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()
	config.Init([]byte(jdpConfig))
	databases.RegisterDatabase("jdp", NewDatabase)
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpoint)

	// check for a "record-jamo" flag
	var recordJamo bool
	flag.BoolVar(&recordJamo, "record-jamo", false, "records JAMO test queries for use in CI system")
	flag.Parse()

	// poke JAMO to see whether it's available in the current domain
	jamoIsAvailable := false
	const jamoBaseUrl = "https://jamo-dev.jgi.doe.gov/"
	resp, err := http.Get(jamoBaseUrl)
	if err != nil {
		panic(fmt.Errorf("setup: %s", err.Error()))
	}
	if resp.StatusCode == http.StatusOK { // success!
		jamoIsAvailable = true
	}

	// set up a "VCR" to manage the recording and playback of JAMO queries
	vcrMode := recorder.ModePassthrough // no recording or playback by default
	cassetteName := "fixtures/dts-jamo-test-cassette"
	if jamoIsAvailable {
		slog.Debug("Querying JAMO for file resource info")
		if recordJamo {
			slog.Debug("Recording JAMO query")
			vcrMode = recorder.ModeRecordOnly
		}
	} else { // JAMO not available -- play back
		slog.Debug("JAMO unavailable -- using pre-recorded results for query")
		vcrMode = recorder.ModeReplayOnly
	}
	vcr, err = recorder.NewWithOptions(&recorder.Options{
		CassetteName: cassetteName,
		Mode:         vcrMode,
	})
	if err != nil {
		panic(fmt.Errorf("setup: %s", err.Error()))
	}
	jamoClient = vcr.GetDefaultClient()
}

// this function gets called after all tests have been run
func breakdown() {
	if vcr != nil {
		vcr.Stop()
	}
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpDb, err := NewDatabase(orcid)
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
}

func TestNewDatabaseWithoutOrcid(t *testing.T) {
	assert := assert.New(t)
	jdpDb, err := NewDatabase("")
	assert.Nil(jdpDb, "Invalid JDP database somehow created")
	assert.NotNil(err, "JDP database creation without ORCID encountered no error")
}

func TestNewDatabaseWithoutJDPSharedSecret(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpSecret := os.Getenv("DTS_JDP_SECRET")
	os.Unsetenv("DTS_JDP_SECRET")
	jdpDb, err := NewDatabase(orcid)
	os.Setenv("DTS_JDP_SECRET", jdpSecret)
	assert.Nil(jdpDb, "JDP database somehow created without shared secret available")
	assert.NotNil(err, "JDP database creation without shared secret encountered no error")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	params := databases.SearchParameters{
		Query: "prochlorococcus",
		Pagination: struct {
			Offset, MaxNum int
		}{
			Offset: 1,
			MaxNum: 50,
		},
	}
	results, err := db.Search(params)
	assert.True(len(results.Resources) > 0, "JDP search query returned no results")
	assert.Nil(err, "JDP search query encountered an error")
}

func TestResources(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	params := databases.SearchParameters{
		Query: "prochlorococcus",
	}
	results, _ := db.Search(params)
	fileIds := make([]string, len(results.Resources))
	for i, res := range results.Resources {
		fileIds[i] = res.Id
	}
	resources, err := db.Resources(fileIds[:10])
	assert.Nil(err, "JDP resource query encountered an error")
	assert.Equal(10, len(resources),
		"JDP resource query didn't return requested number of results")
	// JAMO doesn't return source/credit metadata, and sometimes doesn't
	// have hashes either, so we have to check field by field
	for i, _ := range resources {
		jdpSearchResult := results.Resources[i]
		resource := resources[i]
		assert.Equal(jdpSearchResult.Id, resource.Id, "Resource ID mismatch")
		assert.Equal(jdpSearchResult.Name, resource.Name, "Resource name mismatch")
		assert.Equal(jdpSearchResult.Path, resource.Path, "Resource path mismatch")
		assert.Equal(jdpSearchResult.Format, resource.Format, "Resource format mismatch")
		// FIXME: looks like JDP and JAMO disagree about a resource size!
		//assert.Equal(jdpSearchResult.Bytes, resource.Bytes, "Resource size mismatch")
		assert.Equal(jdpSearchResult.MediaType, resource.MediaType, "Resource media type mismatch")
		assert.Equal(jdpSearchResult.Credit.Identifier, resource.Credit.Identifier, "Resource credit ID mismatch")
		assert.Equal(jdpSearchResult.Credit.ResourceType, resource.Credit.ResourceType, "Resource credit resource type mismatch")
	}
}

func TestEndpoint(t *testing.T) {
	assert := assert.New(t)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(orcid)
	endpoint, err := db.Endpoint()
	assert.Nil(err)
	assert.NotNil(endpoint, "JDP database has no endpoint")
}

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}
