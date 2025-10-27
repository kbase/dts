package kbase

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/dtstest"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/endpoints/globus"
)

// this runs setup, runs all tests, and does breakdown
func TestMain(m *testing.M) {
	setup()
	status := m.Run()
	breakdown()
	os.Exit(status)
}

// runs all tests serially (so we can swap out KBase user tables)
func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	tester.TestNewDatabase()
	tester.TestUserFederation()
	tester.TestSearch()
	tester.TestResources()
	tester.TestLocalUser()
}

func (t *SerialTests) TestNewDatabase() {
	assert := assert.New(t.Test)
	db, err := NewDatabase(conf)
	assert.NotNil(db, "KBase database not created")
	assert.Nil(err, "KBase database creation encountered an error")
}

func (t *SerialTests) TestUserFederation() {
	assert := assert.New(t.Test)

	// make sure we can create a db with good user tables
	for i := range goodUserTables {
		err := copyDataFile(fmt.Sprintf("good_user_table_%d.csv", i), kbaseUserTableFile)
		assert.Nil(err, "Couldn't copy good_user_table_%d.csv into place.")
		db, err := NewDatabase(conf)
		assert.NotNil(db, fmt.Sprintf("KBase database not created with good_user_table_%d", i))
		assert.Nil(err, "KBase database creation encountered an error")
		kbaseDb, ok := db.(*Database)
		assert.True(ok, "KBase database is not of type *Database")
		err = kbaseDb.FinalizeDatabase()
		assert.Nil(err, "Error finalizing KBase database.")
	}

	// make sure we CAN'T create a db with bad user tables
	for i := range badUserTables {
		err := copyDataFile(fmt.Sprintf("bad_user_table_%d.csv", i), kbaseUserTableFile)
		assert.Nil(err, "Couldn't copy bad_user_table_%d.csv into place.")
		db, err := NewDatabase(conf)
		assert.Nil(db, fmt.Sprintf("KBase database created with bad_user_table_%d.csv", i))
		assert.NotNil(err, "KBase database creation with bad user table didn't encounter an error")
	}

	// copy a good user table back into place
	copyDataFile("good_user_table_0.csv", kbaseUserTableFile)
}

func (t *SerialTests) TestSearch() {
	assert := assert.New(t.Test)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
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

func (t *SerialTests) TestResources() {
	assert := assert.New(t.Test)
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	db, _ := NewDatabase(conf)
	_, err := db.Descriptors(orcid, nil)
	assert.NotNil(err, "Descriptors not implemented for kbase database!")
}

func (t *SerialTests) TestLocalUser() {
	assert := assert.New(t.Test)
	db, _ := NewDatabase(conf)
	username, err := db.LocalUser("1234-5678-9101-112X")
	assert.Nil(err)
	assert.Equal("Alice", username)
	username, err = db.LocalUser("1235-5678-9101-112X")
	assert.NotNil(err)
	assert.Equal("", username)
}

var CWD string
var TESTING_DIR string
var conf config.Config

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

// test user/ORCID spreadsheets
var goodUserTables = []string{
	`username,orcid
Alice,1234-5678-9101-112X
Bob,1234-5678-9101-1121
`,
	`orcid,username
1234-5678-9101-112X,Alice
1234-5678-9101-1121,Bob
`,
}

var badUserTables = []string{
	`nocommas`,
	`orcid,orcid
1234-5678-9101-1121,1234-5678-9101-1121
`,
	`username,orcid
1234-5678-9101-1121,Bob
Bob,1234-5678-9101-1121
`,
	`username,orcid
Bob,1234-5678-9101-1121
Bob,1234-5678-9101-1122
`,
	`username,orcid
Bob,1234-5678-9101-1121
Boberto,1234-5678-9101-1121
`,
}

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
	conf, err = config.NewConfig([]byte(myConfig))
	if err != nil {
		log.Panicf("Couldn't parse config: %s", err)
	}

	// create the data directory and populate it with our test spreadsheets
	os.Mkdir(config.Service.DataDirectory, 0755)
	for i, userTable := range goodUserTables {
		filename := filepath.Join(config.Service.DataDirectory, fmt.Sprintf("good_user_table_%d.csv", i))
		file, _ := os.Create(filename)
		io.WriteString(file, userTable)
		file.Close()
	}
	for i, userTable := range badUserTables {
		filename := filepath.Join(config.Service.DataDirectory, fmt.Sprintf("bad_user_table_%d.csv", i))
		file, _ := os.Create(filename)
		io.WriteString(file, userTable)
		file.Close()
	}

	// copy a good user table into place
	copyDataFile("good_user_table_0.csv", kbaseUserTableFile)

	databases.RegisterDatabase("kbase", DatabaseConstructor(conf))
	endpoints.RegisterEndpointProvider("globus", globus.NewEndpointFromConfig)
}

// copies a file from a source to a destination file within the DTS data directory
func copyDataFile(src, dst string) error {
	srcFile, err := os.Open(filepath.Join(config.Service.DataDirectory, src))
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.Create(filepath.Join(config.Service.DataDirectory, dst))
	if err != nil {
		return err
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	return err
}

// this function gets called after all tests have been run
func breakdown() {
	if TESTING_DIR != "" {
		// Remove the testing directory and its contents.
		log.Printf("Deleting testing directory %s...\n", TESTING_DIR)
		os.RemoveAll(TESTING_DIR)
	}
}

// To run the tests serially, we attach them to a SerialTests type and
// have them run by a a single test runner.
type SerialTests struct{ Test *testing.T }
