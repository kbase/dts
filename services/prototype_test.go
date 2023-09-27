package services

// unit test setup for the DTS prototype service
import (
	"context"
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
var API_PREFIX = "http://localhost:8080/api/v1"

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

// queries the service's root endpoint
func TestQueryRoot(t *testing.T) {
	assert := assert.New(t) // binds assert to t

	// Query the root endpoint.
	resp, err := http.Get("http://localhost:8080/")
	assert.Nil(err)

	// Read the response.
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err)
	defer resp.Body.Close()

	// The response should be a RootResponse with the service's metadata.
	var root RootResponse
	err = json.Unmarshal(respBody, &root)
	assert.Nil(err)
	assert.Equal("DTS prototype", root.Name)
	assert.Equal(core.Version, root.Version)
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
