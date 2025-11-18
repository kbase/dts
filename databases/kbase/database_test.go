package kbase

import (
	"log"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

const testOrcid = "0000-0002-1825-0097"

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}

func TestNewDatabase(t *testing.T) {
	assert := assert.New(t)
	db, err := NewDatabase(conf)
	assert.NotNil(db, "KBase database not created")
	assert.Nil(err, "KBase database creation encountered an error")
	endpointName := db.EndpointNames()
	assert.Equal([]string{"globus-kbase"}, endpointName, "KBase database returned incorrect endpoint name")
}

func TestSpecificSearchParameters(t *testing.T) {
	assert := assert.New(t)
	db, _ := NewDatabase(conf)
	params := db.SpecificSearchParameters()
	assert.Nil(params, "SpecificSearchParameters should return nil for kbase database")
}

func TestSearch(t *testing.T) {
	assert := assert.New(t)
	orcid := testOrcid
	db, _ := NewDatabase(conf)
	params := databases.SearchParameters{
		Query: "prochlorococcus",
		Pagination: struct {
			Offset, MaxNum int
		}{
			Offset: 1,
			MaxNum: 50,
		},
	}
	_, err := db.Search(orcid, params)
	assert.NotNil(err, "Search not implemented for kbase database!")
}

func TestResources(t *testing.T) {
	assert := assert.New(t)
	orcid := testOrcid
	db, _ := NewDatabase(conf)
	_, err := db.Descriptors(orcid, nil)
	assert.NotNil(err, "Descriptors not implemented for kbase database!")
}

func TestStageFiles(t *testing.T) {
	assert := assert.New(t)
	orcid := testOrcid
	db, _ := NewDatabase(conf)
	fileIds := []string{"file1", "file2"}
	_, err := db.StageFiles(orcid, fileIds)
	assert.NotNil(err, "StageFiles not implemented for kbase database!")
}

func TestStagingStatus(t *testing.T) {
	assert := assert.New(t)
	db, _ := NewDatabase(conf)
	id := uuid.New()
	_, err := db.StagingStatus(id)
	assert.NotNil(err, "StagingStatus not implemented for kbase database!")
}

func TestFinalize(t *testing.T) {
	assert := assert.New(t)
	orcid := testOrcid
	db, _ := NewDatabase(conf)
	id := uuid.New()
	err := db.Finalize(orcid, id)
	assert.Nil(err, "Finalize should return nil error for kbase database")
}

func TestLocalUser(t *testing.T) {
	assert := assert.New(t)
	db, _ := NewDatabase(conf)
	username, err := db.LocalUser("1234-5678-9101-112X")
	assert.Nil(err)
	assert.Equal("Alice", username)
	username, err = db.LocalUser("1235-5678-9101-112X")
	assert.NotNil(err)
	assert.Equal("", username)
	kbaseDb, ok := db.(*Database)
	assert.True(ok)
	err = kbaseDb.FinalizeDatabase()
	assert.Nil(err)
}

func TestSaveLoad(t *testing.T) {
	assert := assert.New(t)
	db, _ := NewDatabase(conf)
	state, err := db.Save()
	assert.Nil(err, "Save should not return an error for kbase database")
	assert.Equal("kbase", state.Name, "Save should return correct database name")
	err = db.Load(state)
	assert.Nil(err, "Load should not return an error for kbase database")
}

var CWD string
var TESTING_DIR string
var conf Config

const kbaseConfig string = `
service:
  data_dir: TESTING_DIR/data
  endpoint: globus-kbase
databases:
  kbase:
    name: KBase Workspace Service (KSS)
    organization: KBase
    endpoint: globus-kbase
endpoints:
  globus-kbase:
    name: KBase
    id: ${DTS_GLOBUS_TEST_ENDPOINT}
    provider: globus
    auth:
      client_id: ${DTS_GLOBUS_CLIENT_ID}
      client_secret: ${DTS_GLOBUS_CLIENT_SECRET}
`

const kbaseDbConfig string = `
name: KBase Workspace Service (KSS)
organization: KBase
data_directory: TESTING_DIR/data
endpoint: globus-kbase
`

// helper function replaces embedded environment variables in yaml string
// when they don't exist in the environment
func setTestEnvVars(yaml string) string {
	testVars := map[string]string{
		"DTS_GLOBUS_TEST_ENDPOINT": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"DTS_GLOBUS_CLIENT_ID":     "fake_client_id",
		"DTS_GLOBUS_CLIENT_SECRET": "fake_client_secret",
	}

	// check for existence of each variable. when not present, replace
	// instances of it in the yaml string with a test value
	for key, value := range testVars {
		if os.Getenv(key) == "" {
			yaml = os.Expand(yaml, func(yamlVar string) string {
				if yamlVar == key {
					return value
				}
				return "${" + yamlVar + "}"
			})
		}
	}

	return yaml
}

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()

	// jot down our CWD, create a temporary directory, and change to it
	var err error
	CWD, err = os.Getwd()
	if err != nil {
		log.Panicf("Couldn't get current working directory: %s", err)
	}
	log.Print("Creating testing directory...\n")
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "kbase-database-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// read the config file with TESTING_DIR replaced
	myConfig := strings.ReplaceAll(kbaseConfig, "TESTING_DIR", TESTING_DIR)
	myConfig = setTestEnvVars(myConfig)
	err = config.Init([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't initialize config: %s", err)
	}
	kbaseConfig := strings.ReplaceAll(kbaseDbConfig, "TESTING_DIR", TESTING_DIR)
	err = yaml.Unmarshal([]byte(setTestEnvVars(kbaseConfig)), &conf)
	if err != nil {
		log.Panicf("Couldn't parse config: %s", err)
	}

	setupUserFederationTests(config.Service.DataDirectory)

	var confMap map[string]any
	err = mapstructure.Decode(conf, &confMap)
	if err != nil {
		log.Panicf("Couldn't decode config to map: %s", err)
	}
	databases.RegisterDatabase("kbase", DatabaseConstructor(confMap))
	endpoints.RegisterEndpointProvider("globus", globus.EndpointConstructor)
}

// this function gets called after all tests have been run
func breakdown() {
	if TESTING_DIR != "" {
		// Remove the testing directory and its contents.
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}
