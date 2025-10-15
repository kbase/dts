// Copyright (c) 2023 The KBase Project and its Contributors
// Copyright (c) 2023 Cohere Consulting, LLC
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// These tests must be run serially, since tasks are coordinated by a
// single instance.

package journal

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/dtstest"
)

// runs all tests serially
func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	tester.TestInitAndFinalize()
	tester.TestRecordSuccessfulTransfer()
	tester.TestRecordFailedTransfer()
}

// This runs setup, runs all tests, and does breakdown.
func TestMain(m *testing.M) {
	var status int
	setup()
	status = m.Run()
	breakdown()
	os.Exit(status)
}

// this function gets called at the begіnning of a test session
func setup() {
	dtstest.EnableDebugLogging()

	log.Print("Creating testing directory...\n")
	var err error
	TESTING_DIR, err = os.MkdirTemp(os.TempDir(), "data-transfer-service-tests-")
	if err != nil {
		log.Panicf("Couldn't create testing directory: %s", err)
	}
	os.Chdir(TESTING_DIR)

	// read in the config file with SOURCE_ROOT and DESTINATION_ROOT replaced
	myConfig := strings.ReplaceAll(journalConfig, "TESTING_DIR", TESTING_DIR)
	err = config.InitSelected([]byte(myConfig), true, false, false, false)
	if err != nil {
		log.Panicf("Couldn't initialize configuration: %s", err)
	}

	// Create the data directory where the transfer journal lives
	err = os.Mkdir(config.Service.DataDirectory, 0755)
	if err != nil {
		log.Panicf("Couldn't create data directory: %s", err)
	}
}

// this function gets called after all tests have been run
func breakdown() {
	if IsOpen() {
		Finalize()
	}
	if TESTING_DIR != "" {
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// To run the tests serially, we attach them to a SerialTests type and
// have them run by a a single test runner.
type SerialTests struct{ Test *testing.T }

func (t *SerialTests) TestInitAndFinalize() {
	assert := assert.New(t.Test)

	assert.False(IsOpen())
	err := Init()
	assert.Nil(err)
	assert.True(IsOpen())
	err = Finalize()
	assert.Nil(err)
	assert.False(IsOpen())
}

func (t *SerialTests) TestRecordSuccessfulTransfer() {
	assert := assert.New(t.Test)

	err := Init()
	assert.Nil(err)

	// generate a valid Frictionless data package for the manifest
	manifestString := `{"contributors":[{"email":"tad@example.com","organization":"","path":"","role":"author","title":"Tad the Rad"}],"created":"2024-11-19T16:37:21Z","keywords":["dts","manifest"],"name":"manifest","profile":"data-package","resources":[{"bytes":10751355980,"credit":{"comment":"","content_url":"","contributors":[{"contributor_type":"Person","contributor_id":"","name":"Right On Dee Williams","given_name":"Right On","family_name":"Williams","affiliations":[{"organization_id":"","organization_name":"The Company Company"}],"contributor_roles":"PI"}],"credit_metadata_source":"","dates":[{"date":"2021-07-04T01:37:34","event":"Created"},{"date":"2021-07-04T08:33:40.249000","event":"Accepted"},{"date":"2024-11-15T00:45:20.175000","event":"Updated"}],"descriptions":null,"funding":null,"identifier":"JDP:60e1d4d4c399d4ad32fe3bb6","license":{"id":"","url":""},"meta":{"credit_metadata_schema_version":"","saved_by":"","timestamp":0},"publisher":{"organization_id":"ROR:04xm1d337","organization_name":"Joint Genome Institute"},"related_identifiers":[{"id":"10.25585/1488219","description":"Proposal DOI","relationship_type":"IsCitedBy"},{"id":"10.46936/fics.proj.2016.49495/60006008","description":"Awarded proposal DOI","relationship_type":"IsCitedBy"}],"resource_type":"dataset","titles":[{"language":"","title":"img/submissions/253630/Ga0456371_contigs.fna","title_type":""}],"url":"","version":"2021-07-04T01:37:34"},"format":"fasta","hash":"55c3afc0a2d3b256332425eeebc581ac","id":"JDP:60e1d4d4c399d4ad32fe3bb6","media_type":"text/plain","name":"ga0456371_contigs","path":"img/submissions/253630/Ga0456371_contigs.fna","sources":[{"email":"kwrighton@gmail.com","path":"https://doi.org/10.46936/fics.proj.2016.49495/60006008","title":"Wrighton, Kelly (Colorado State University, United States)"}],"Endpoint":"globus-jdp"},{"bytes":1323656783,"credit":{"comment":"","content_url":"","contributors":[{"contributor_type":"Person","contributor_id":"","name":"Wrighton, Kelly","given_name":"Kelly","family_name":"Wrighton","affiliations":[{"organization_id":"","organization_name":"Colorado State University"}],"contributor_roles":"PI"}],"credit_metadata_source":"","dates":[{"date":"2021-01-17T00:22:08","event":"Created"},{"date":"2021-01-17T02:22:33.965000","event":"Accepted"},{"date":"2024-11-15T03:38:03.281000","event":"Updated"}],"descriptions":null,"funding":null,"identifier":"JDP:60040fe9536e7b328301be52","license":{"id":"","url":""},"meta":{"credit_metadata_schema_version":"","saved_by":"","timestamp":0},"publisher":{"organization_id":"ROR:04xm1d337","organization_name":"Joint Genome Institute"},"related_identifiers":[{"id":"10.25585/1488219","description":"Proposal DOI","relationship_type":"IsCitedBy"},{"id":"10.46936/fics.proj.2016.49495/60006008","description":"Awarded proposal DOI","relationship_type":"IsCitedBy"}],"resource_type":"dataset","titles":[{"language":"","title":"img/submissions/246789/Ga0456363_contigs.fna","title_type":""}],"url":"","version":"2021-01-17T00:22:08"},"format":"fasta","hash":"609848a41e79d0f9ec8867c9c866b18c","id":"JDP:60040fe9536e7b328301be52","media_type":"text/plain","name":"ga0456363_contigs","path":"img/submissions/246789/Ga0456363_contigs.fna","sources":[{"email":"kwrighton@gmail.com","path":"https://doi.org/10.46936/fics.proj.2016.49495/60006008","title":"Wrighton, Kelly (Colorado State University, United States)"}],"Endpoint":"globus-jdp"}]}`
	manifest, err := datapackage.FromString(manifestString, "manifest.json", validator.InMemoryLoader())
	assert.Nil(err)

	record := Record{
		Id:          uuid.New(),
		Source:      "source",
		Destination: "destination",
		Orcid:       "1234-5678-9012-3456",
		Status:      "succeeded",
		StartTime:   time.Now().Add(-time.Hour),
		StopTime:    time.Now(),
		PayloadSize: int64(12853294),
		NumFiles:    12,
		Manifest:    manifest,
	}
	err = RecordTransfer(record)
	assert.Nil(err)

	records, err := Records(record.StartTime, record.StopTime)
	assert.Nil(err)
	assert.NotNil(records)
	assert.Equal(1, len(records))
	assert.Equal(record.Id, records[0].Id)
	assert.Equal(record.Source, records[0].Source)
	assert.Equal(record.Destination, records[0].Destination)
	assert.Equal(record.Orcid, records[0].Orcid)
	assert.Equal(record.Status, records[0].Status)
	assert.Equal(record.PayloadSize, records[0].PayloadSize)
	assert.Equal(record.NumFiles, records[0].NumFiles)
	assert.True(record.StartTime.Equal(records[0].StartTime))
	assert.True(record.StopTime.Equal(records[0].StopTime))

	assert.Equal(manifest.ResourceNames(), record.Manifest.ResourceNames())

	err = Finalize()
	assert.Nil(err)
}

func (t *SerialTests) TestRecordFailedTransfer() {
	assert := assert.New(t.Test)

	err := Init()
	assert.Nil(err)

	record := Record{
		Id:          uuid.New(),
		Source:      "source",
		Destination: "destination",
		Orcid:       "1234-5678-9012-3456",
		Status:      "failed",
		StartTime:   time.Now(),
		StopTime:    time.Now(),
		PayloadSize: int64(12853294),
		NumFiles:    12,
	}
	err = RecordTransfer(record)
	assert.Nil(err)

	records, err := Records(record.StartTime, record.StopTime)
	assert.Nil(err)
	assert.NotNil(records)
	assert.Equal(1, len(records))
	assert.Equal(record.Id, records[0].Id)
	assert.Equal(record.Source, records[0].Source)
	assert.Equal(record.Destination, records[0].Destination)
	assert.Equal(record.Orcid, records[0].Orcid)
	assert.Equal(record.Status, records[0].Status)
	assert.Equal(record.PayloadSize, records[0].PayloadSize)
	assert.Equal(record.NumFiles, records[0].NumFiles)
	assert.True(record.StartTime.Equal(records[0].StartTime))
	assert.True(record.StopTime.Equal(records[0].StopTime))

	err = Finalize()
	assert.Nil(err)
}

// temporary testing directory
var TESTING_DIR string

// configuration
const journalConfig string = `
service:
  name: test
  port: 8080
  max_connections: 100
  poll_interval: 50  # milliseconds
  data_dir: TESTING_DIR/data
  manifest_dir: TESTING_DIR/manifests
  delete_after: 2    # seconds
`
