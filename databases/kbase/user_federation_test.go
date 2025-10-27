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
	`username,orcid
Alice,1234-5678-9101-112X
Bob,1234-5678-9101-1121
Dave,9402-1876-5432-1098
`,
	`orcid,username
1234-5678-9101-112X,Alice
1234-5678-9101-1121,Bob
4321-1876-5432-1098,Charlie
`,
}

var goodUserMap = [2]map[string]string{
	{
		"1234-5678-9101-112X": "Alice",
		"1234-5678-9101-1121": "Bob",
		"9402-1876-5432-1098": "Dave",
	},
	{
		"1234-5678-9101-112X": "Alice",
		"1234-5678-9101-1121": "Bob",
		"4321-1876-5432-1098": "Charlie",
	},
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

func newTestKbaseUserFederation(t *testing.T, filePath string) KBaseUserFederation {
	kbaseFed := KBaseUserFederation{
		Started:    false,
		FilePath:   filePath,
		UpdateChan: make(chan struct{}),
		StopChan:   make(chan struct{}),
		OrcidChan:  make(chan string),
		UserChan:   make(chan string),
		ErrorChan:  make(chan error),
	}
	return kbaseFed
}

func TestKBaseStartReloadStop(t *testing.T) {
	assert := assert.New(t)

	kbaseFed := newTestKbaseUserFederation(t, filepath.Join(testDataDir, "good_user_table_0.csv"))
	err := kbaseFed.startUserFederation()
	assert.Nil(err, "Error starting KBase user federation")

	// look up a user
	username, err := kbaseFed.usernameForOrcid("1234-5678-9101-112X")
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Alice", username, "Incorrect username for existing ORCID")

	// look up another user
	username, err = kbaseFed.usernameForOrcid("9402-1876-5432-1098")
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Dave", username, "Incorrect username for existing ORCID")

	// look up a non-existing user
	username, err = kbaseFed.usernameForOrcid("9999-8888-7777-6666")
	assert.NotNil(err, "No error looking up non-existing ORCID")
	assert.Equal("", username, "Username returned for non-existing ORCID")

	// try to restart the federation
	err = kbaseFed.startUserFederation()
	assert.Nil(err, "Error restarting KBase user federation")

	// reload the user table with updated data
	kbaseFed.FilePath = filepath.Join(testDataDir, "good_user_table_1.csv")
	err = kbaseFed.reloadUserTable()
	assert.Nil(err, "Error reloading user table")

	// look up a user from the updated table
	username, err = kbaseFed.usernameForOrcid("1234-5678-9101-1121")
	assert.Nil(err, "Error looking up existing ORCID after reload")
	assert.Equal("Bob", username, "Incorrect username for existing ORCID after reload")

	// look up another user from the updated table
	username, err = kbaseFed.usernameForOrcid("4321-1876-5432-1098")
	assert.Nil(err, "Error looking up existing ORCID after reload")
	assert.Equal("Charlie", username, "Incorrect username for existing ORCID after reload")

	// look up an ORCID that existed in the old table but not in the new table
	username, err = kbaseFed.usernameForOrcid("9402-1876-5432-1098")
	assert.NotNil(err, "No error looking up old ORCID after reload")
	assert.Equal("", username, "Username returned for old ORCID after reload")

	// stop the user federation
	err = kbaseFed.stopUserFederation()
	assert.Nil(err, "Error stopping KBase user federation")

	// try to stop again
	err = kbaseFed.stopUserFederation()
	assert.NotNil(err, "No error stopping KBase user federation again")

	// try to look up a user after stopping
	username, err = kbaseFed.usernameForOrcid("1234-5678-9101-112X")
	assert.NotNil(err, "No error looking up ORCID after stopping federation")
	assert.Equal("", username, "Username returned after stopping federation")
}

func TestKbaseUserFederation(t *testing.T) {
	assert := assert.New(t)

	kbaseFed := newTestKbaseUserFederation(t, filepath.Join(testDataDir, "good_user_table_0.csv"))
	started := make(chan struct{})
	go kbaseFed.kbaseUserFederation(started)
	<-started

	// load the user table
	kbaseFed.UpdateChan <- struct{}{}
	err := <-kbaseFed.ErrorChan
	assert.Nil(err, "Error loading user table")

	// test existing ORCID
	kbaseFed.OrcidChan <- "1234-5678-9101-112X"
	username := <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Alice", username, "Incorrect username for existing ORCID")

	// test another existing ORCID
	kbaseFed.OrcidChan <- "9402-1876-5432-1098"
	username = <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.Nil(err, "Error looking up existing ORCID")
	assert.Equal("Dave", username, "Incorrect username for existing ORCID")

	// test non-existing ORCID
	kbaseFed.OrcidChan <- "9999-8888-7777-6666"
	username = <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.NotNil(err, "No error looking up non-existing ORCID")
	assert.Equal("", username, "Username returned for non-existing ORCID")

	// reload user table with updated data
	kbaseFed.FilePath = filepath.Join(testDataDir, "good_user_table_1.csv")
	kbaseFed.UpdateChan <- struct{}{}
	err = <-kbaseFed.ErrorChan
	assert.Nil(err, "Error updating user table")

	// test existing ORCID from updated table
	kbaseFed.OrcidChan <- "1234-5678-9101-1121"
	username = <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.Nil(err, "Error looking up existing ORCID after update")
	assert.Equal("Bob", username, "Incorrect username for existing ORCID after update")

	// test another existing ORCID from updated table
	kbaseFed.OrcidChan <- "4321-1876-5432-1098"
	username = <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.Nil(err, "Error looking up existing ORCID after update")
	assert.Equal("Charlie", username, "Incorrect username for existing ORCID after update")

	// test ORCID that existed in old table but not in new table
	kbaseFed.OrcidChan <- "9402-1876-5432-1098"
	username = <-kbaseFed.UserChan
	err = <-kbaseFed.ErrorChan
	assert.NotNil(err, "No error looking up old ORCID after update")
	assert.Equal("", username, "Username returned for old ORCID after update")

	// stop the user federation goroutine
	kbaseFed.StopChan <- struct{}{}
}

func TestReadUserTable(t *testing.T) {
	assert := assert.New(t)

	for i := range goodUserTables {
		filePath := filepath.Join(testDataDir, fmt.Sprintf("good_user_table_%d.csv", i))
		kbaseFed := newTestKbaseUserFederation(t, filePath)
		users, err := kbaseFed.readUserTable()
		assert.Nil(err, "Error reading good_user_table_%d.csv", i)
		assert.Equal(len(users), len(goodUserMap[i]), "Incorrect number of users read from good_user_table_%d.csv", i)
		for orcid, username := range goodUserMap[i] {
			readUsername, found := users[orcid]
			assert.True(found, "ORCID %s not found in users from good_user_table_%d.csv", orcid, i)
			assert.Equal(username, readUsername, "Incorrect username for ORCID %s in good_user_table_%d.csv", orcid, i)
		}
	}
	for i := range badUserTables {
		filePath := filepath.Join(testDataDir, fmt.Sprintf("bad_user_table_%d.csv", i))
		kbaseFed := newTestKbaseUserFederation(t, filePath)
		users, err := kbaseFed.readUserTable()
		assert.NotNil(err, "No error reading bad_user_table_%d.csv", i)
		assert.Nil(users, "Users read from bad_user_table_%d.csv", i)
	}
	for i := range badCSVFormat {
		filePath := filepath.Join(testDataDir, fmt.Sprintf("bad_csv_format_%d.csv", i))
		kbaseFed := newTestKbaseUserFederation(t, filePath)
		users, err := kbaseFed.readUserTable()
		assert.NotNil(err, "No error reading bad_csv_format_%d.csv", i)
		assert.Nil(users, "Users read from bad_csv_format_%d.csv", i)
	}
	kbaseFed := newTestKbaseUserFederation(t, "non_existent_file.csv")
	users, err := kbaseFed.readUserTable()
	assert.NotNil(err, "No error reading non_existent_file.csv")
	assert.Nil(users, "Users read from non_existent_file.csv")
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
		"",                     // Empty string
		"user name",           // Space
		"user@name",           // Special character
		"user$name",           // Special character
		"user#name",           // Special character
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
		"0000-0002-1825-009",    // Too short
		"0000-0002-1825-00978",  // Too long
		"0000-0002-1825-0090X",  // Invalid character
		"0000_0002_1825_0097",   // Invalid separator
		"abcd-efgh-ijkl-mnop",   // Non-numeric
	}

	for _, orcid := range validOrcids {
		assert.True(isOrcid(orcid), "Expected valid ORCID: %s", orcid)
	}

	for _, orcid := range invalidOrcids {
		assert.False(isOrcid(orcid), "Expected invalid ORCID: %s", orcid)
	}
}