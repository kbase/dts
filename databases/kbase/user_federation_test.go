package kbase

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// valid user table csv contents
var goodUserTables = []string{
	`username,orcid,globusid
Alice,1234-5678-9101-112X,184014ac-97c0-4270-94af-97f0cc055673
Bob,1234-5678-9101-1121,4c5ad8e6-0f5d-4c06-a05d-c6198635675c
Dave,9402-1876-5432-1098,
`,
	`orcid,globusid,username
1234-5678-9101-112X,184014ac-97c0-4270-94af-97f0cc055673,Alice
1234-5678-9101-1121,4c5ad8e6-0f5d-4c06-a05d-c6198635675c,Bob
4321-1876-5432-1098,95f69174-be49-479d-9c71-812580b1371e,Charlie
`,
}

// invalid user table csv contents
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

// bad csv format
var badCSVFormat = []string{
	`username;orcid
Alice;1234-5678-9101-112X
Bob;1234-5678-9101-1121
`,
	`username|orcid
Alice|1234-5678-9101-112X
Bob|1234-5678-9101-1121
`,
}

// test data directory with csv files
var testDataDir string

func setupUserFederationTests(testDir string) {
	testDataDir = testDir

	// create the data directory and populate it with our test spreadsheets
	os.Mkdir(testDir, 0755)
	for i, userTable := range goodUserTables {
		filename := filepath.Join(testDir, fmt.Sprintf("good_user_table_%d.csv", i))
		file, _ := os.Create(filename)
		io.WriteString(file, userTable)
		file.Close()
	}
	for i, userTable := range badUserTables {
		filename := filepath.Join(testDir, fmt.Sprintf("bad_user_table_%d.csv", i))
		file, _ := os.Create(filename)
		io.WriteString(file, userTable)
		file.Close()
	}
	for i, userTable := range badCSVFormat {
		filename := filepath.Join(testDir, fmt.Sprintf("bad_csv_format_%d.csv", i))
		file, _ := os.Create(filename)
		io.WriteString(file, userTable)
		file.Close()
	}

	// copy a good user table into the default file location
	copyDataFile(testDir, "good_user_table_0.csv", kbaseUserTableFile)
}

// copies a file from a source to a destination file within the DTS data directory
func copyDataFile(testDir, src, dst string) error {
	srcFile, err := os.Open(filepath.Join(testDir, src))
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.Create(filepath.Join(testDir, dst))
	if err != nil {
		return err
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	return err
}

func TestKBaseStartReloadStop(t *testing.T) {
	assert := assert.New(t)

	kbaseFed := NewKBaseUserFederationFromFile(filepath.Join(testDataDir, "good_user_table_0.csv"))
	err := kbaseFed.Start()
	assert.Nil(err, "Error starting KBase user federation")

	// look up a user and that user's Globus ID
	username, err := kbaseFed.UsernameForOrcid("1234-5678-9101-112X")
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Alice", username, "Incorrect username for existing ORCID")
	globusId, err := kbaseFed.GlobusIdForOrcid("1234-5678-9101-112X")
	assert.Nil(err, "Error looking up existing Globus ID")
	assert.Equal("184014ac-97c0-4270-94af-97f0cc055673", globusId.String())

	// look up another user
	username, err = kbaseFed.UsernameForOrcid("9402-1876-5432-1098")
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Dave", username, "Incorrect username for existing ORCID")

	// look up a non-existing user
	username, err = kbaseFed.UsernameForOrcid("9999-8888-7777-6666")
	assert.NotNil(err, "No error looking up non-existing ORCID")
	assert.Equal("", username, "Username returned for non-existing ORCID")

	// try to restart the federation
	err = kbaseFed.Start()
	assert.Nil(err, "Error restarting KBase user federation")

	// reload the user table with updated data
	kbaseFed.FilePath = filepath.Join(testDataDir, "good_user_table_1.csv")
	err = kbaseFed.reloadUserTable()
	assert.Nil(err, "Error reloading user table")

	// look up a user from the updated table
	username, err = kbaseFed.UsernameForOrcid("1234-5678-9101-1121")
	assert.Nil(err, "Error looking up existing ORCID after reload")
	assert.Equal("Bob", username, "Incorrect username for existing ORCID after reload")

	// look up another user from the updated table
	username, err = kbaseFed.UsernameForOrcid("4321-1876-5432-1098")
	assert.Nil(err, "Error looking up existing ORCID after reload")
	assert.Equal("Charlie", username, "Incorrect username for existing ORCID after reload")

	// look up an ORCID that existed in the old table but not in the new table
	username, err = kbaseFed.UsernameForOrcid("9402-1876-5432-1098")
	assert.NotNil(err, "No error looking up old ORCID after reload")
	assert.Equal("", username, "Username returned for old ORCID after reload")

	// stop the user federation
	err = kbaseFed.Stop()
	assert.Nil(err, "Error stopping KBase user federation")

	// try to stop again
	err = kbaseFed.Stop()
	assert.NotNil(err, "No error stopping KBase user federation again")

	// try to look up a user after stopping
	username, err = kbaseFed.UsernameForOrcid("1234-5678-9101-112X")
	assert.NotNil(err, "No error looking up ORCID after stopping federation")
	assert.Equal("", username, "Username returned after stopping federation")
}

func TestIsUsername(t *testing.T) {
	assert := assert.New(t)

	validUsernames := []string{
		"john_doe",
		"Jane_Doe123",
		"user_name",
		"UserName",
		"a",
		"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_",
	}

	invalidUsernames := []string{
		"",          // Empty string
		"user name", // Space
		"user@name", // Special character
		"user$name", // Special character
		"user#name", // Special character
	}

	for _, username := range validUsernames {
		assert.True(isUsername(username), "Expected valid username: %s", username)
	}

	for _, username := range invalidUsernames {
		assert.False(isUsername(username), "Expected invalid username: %s", username)
	}
}

func TestIsOrcid(t *testing.T) {
	assert := assert.New(t)

	validOrcids := []string{
		"0000-0002-1825-0097",
		"1234-5678-9012-345X",
		"0000-0000-0000-0000",
	}

	invalidOrcids := []string{
		"0000-0002-1825-009",   // Too short
		"0000-0002-1825-00978", // Too long
		"0000-0002-1825-0090X", // Invalid character
		"0000_0002_1825_0097",  // Invalid separator
		"abcd-efgh-ijkl-mnop",  // Non-numeric
	}

	for _, orcid := range validOrcids {
		assert.True(isOrcid(orcid), "Expected valid ORCID: %s", orcid)
	}

	for _, orcid := range invalidOrcids {
		assert.False(isOrcid(orcid), "Expected invalid ORCID: %s", orcid)
	}
}
