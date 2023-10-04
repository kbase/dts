package services

// unit test setup for the DTS prototype service
import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"dts/config"
	"dts/core"
)

// working directory from which the tests were invoked
var CWD string

// temporary testing directory
var TESTING_DIR string

// API prefix for requests.
var API_PREFIX = "http://localhost:8080/api/v1/"

// service instance
var service TransferService

const dtsConfig string = `
service:
  port: 8080
  max_connections: 100
  poll_interval: 100
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
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

// performs testing setup
func setup() {
	// jot down our CWD, create a temporary directory, and change to it
	var err error
	CWD, err = os.Getwd()
	if err != nil {
		log.Panicf("Couldn't get current working directory: %s", err)
	}
	log.Print("Creating testing directory...\n")
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// initialize the config environment
	err = config.Init([]byte(dtsConfig))
	if err != nil {
		log.Panic(fmt.Sprintf("Couldn't initialize test configuration: %s", err.Error()))
	}

	// Start the service.
	log.Print("Starting test mapping service...\n")
	go func() {
		service, err = NewDTSPrototype()
		if err != nil {
			log.Panicf("Couldn't construct the service: %s", err.Error())
		}
		err = service.Start(config.Service.Port)
		if err != nil {
			log.Panicf("Couldn't start search service: %s", err.Error())
		}
	}()

	// Give the service time to start up.
	time.Sleep(100 * time.Millisecond)

	// Change back to our original CWD.
	os.Chdir(CWD)
}

// Performs testing breakdown.
func breakdown() {

	if service != nil {
		// Gracefully shut the service down when it finishes its work.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		service.Shutdown(ctx)
	}

	if TESTING_DIR != "" {
		// Remove the testing directory and its contents.
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// sends a GET query with well-formed headers
func get(resource string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, resource, http.NoBody)
	if err == nil {
		accessToken := os.Getenv("DTS_KBASE_DEV_TOKEN")
		b64Token := base64.StdEncoding.EncodeToString([]byte(accessToken))
		req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", b64Token))
		return http.DefaultClient.Do(req)
	} else {
		return nil, err
	}
}

// queries the service's root endpoint
func TestQueryRoot(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	resp, err := get("http://localhost:8080/")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var root RootResponse
	err = json.Unmarshal(respBody, &root)
	assert.Nil(err)
	assert.Equal("DTS prototype", root.Name)
	assert.Equal(core.Version, root.Version)
}

// queries the service's databases endpoint
func TestQueryDatabases(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	resp, err := get(API_PREFIX + "databases")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var dbs []dbMetadata
	err = json.Unmarshal(respBody, &dbs)
	assert.Nil(err)
	assert.Equal(1, len(dbs))
	assert.Equal("jdp", dbs[0].Id)
	assert.Equal("JGI Data Portal", dbs[0].Name)
	assert.Equal("Joint Genome Institute", dbs[0].Organization)
}

// queries a specific (valid) database
func TestQueryValidDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	resp, err := get(API_PREFIX + "databases/jdp")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var db dbMetadata
	err = json.Unmarshal(respBody, &db)
	assert.Nil(err)
	assert.Equal("jdp", db.Id)
	assert.Equal("JGI Data Portal", db.Name)
	assert.Equal("Joint Genome Institute", db.Organization)
}

// queries a database that doesn't exist
func TestQueryInvalidDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	resp, err := get(API_PREFIX + "databases/nonexistentdb")
	assert.Nil(err)
	assert.Equal(404, resp.StatusCode)
}

// searches a specific database for files matching a simple query
func TestSearchDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	resp, err := get(API_PREFIX + "files?database=jdp&query=prochlorococcus")
	assert.Nil(err)

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	var results ElasticSearchResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err)
	assert.Equal("jdp", results.Database)
	assert.Equal("prochlorococcus", results.Query)
	assert.NotNil(results.Resources)
}

// attempts to fetch the status of a nonexistent transfer
func TestFetchInvalidTransferStatus(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	// try an ill-formed transfer
	resp, err := get(API_PREFIX + "transfers/xyzzy")
	assert.Nil(err)
	assert.Equal(400, resp.StatusCode)

	// I bet this one doesn't exist!!
	resp, err = get(API_PREFIX + "transfers/3f0f9563-e1f8-4b9c-9308-36988e25df0b")
	assert.Nil(err)
	assert.Equal(404, resp.StatusCode)
}

// runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	var status int
	setup()
	if TESTING_DIR != "" {
		status = m.Run()
	}
	breakdown()
	os.Exit(status)
}
