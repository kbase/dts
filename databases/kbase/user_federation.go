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

package kbase

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
)

//=======================
// KBase user federation
//=======================

// In order to map an ORCID to a KBase username, we maintain a mapping that
// stores entries for all KBase users with ORCIDs. This mapping currently lives
// a 2-column spreadsheet (CSV) in the DTS data directory. The data in this
// spreadsheet is reloaded every hour on the top of the hour so a new file can
// be dropped into the data directory with predictable results.

// KBase User Federation
type KBaseUserFederation struct {
	Started    bool
	FilePath   string        // full path to the KBase user table file
	UpdateChan chan struct{} // triggers updates to the ORCID/user table
	StopChan   chan struct{} // stops the user federation subsystem
	OrcidChan  chan string   // passes ORCIDs in for lookup
	UserChan   chan string   // passes usernames out
	ErrorChan  chan error    // passes errors out
}

// configuration information for the KBase user federation subsystem
type KBaseUserFederationConfig struct {
	DataDirectory string `yaml:"data_directory" mapstructure:"data_directory"`
}

func newKBaseUserFederation(conf KBaseUserFederationConfig) (KBaseUserFederation, error) {
	kbaseFed := KBaseUserFederation{}
	kbaseFed.Started = false
	kbaseFed.FilePath = filepath.Join(conf.DataDirectory, kbaseUserTableFile)
	return kbaseFed, nil
}

// starts up the user federation machinery if it hasn't yet been started
func (kbaseFed *KBaseUserFederation) Start() error {
	if kbaseFed.Started {
		return nil
	}
	// fire up the user federation goroutine if needed
	started := make(chan struct{})
	go kbaseFed.kbaseUserFederation(started)
	<-started // wait for it to start

	// load the user table
	kbaseFed.UpdateChan <- struct{}{}
	err := <-kbaseFed.ErrorChan
	if err != nil {
		return err
	}

	// start a pulse that reloads the user table from a file at the top of every hour
	go func() {
		for {
			t := time.Now()
			topOfHour := time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			time.Sleep(time.Until(topOfHour))
			err := kbaseFed.reloadUserTable()

			// reloading errors are logged, not propagated
			if err != nil {
				slog.Warn(err.Error())
			}
		}
	}()

	return nil
}

// returns the KBase username associated with the given ORCID
func (kbaseFed *KBaseUserFederation) usernameForOrcid(orcid string) (string, error) {
	if !kbaseFed.Started {
		return "", fmt.Errorf("KBase federated user table not available")
	}
	kbaseFed.OrcidChan <- orcid
	username := <-kbaseFed.UserChan
	err := <-kbaseFed.ErrorChan
	return username, err
}

func (kbaseFed *KBaseUserFederation) reloadUserTable() error {
	kbaseFed.UpdateChan <- struct{}{}
	return <-kbaseFed.ErrorChan
}

// stops the user federation machinery
func (kbaseFed *KBaseUserFederation) Stop() error {
	if !kbaseFed.Started {
		return fmt.Errorf("KBase user federation not started")
	}
	kbaseFed.StopChan <- struct{}{}
	err := <-kbaseFed.ErrorChan
	return err
}

//-----------
// Internals
//-----------

const kbaseUserTableFile = "kbase_user_orcids.csv"

// This goroutine maintains a table that associates ORCIDs with KBase users.
// It fields requests for usernames given ORCIDs, and can also update the table
// by reading a file.
func (kbaseFed *KBaseUserFederation) kbaseUserFederation(started chan struct{}) {
	// channels
	kbaseFed.OrcidChan = make(chan string)
	kbaseFed.UserChan = make(chan string)
	kbaseFed.ErrorChan = make(chan error)
	kbaseFed.UpdateChan = make(chan struct{})
	kbaseFed.StopChan = make(chan struct{})

	// mapping of ORCIDs to KBase users
	kbaseUserTable := make(map[string]string)

	// we're ready
	kbaseFed.Started = true
	started <- struct{}{}

	for {
		select {
		case orcid := <-kbaseFed.OrcidChan: // fetching username for orcid
			if username, found := kbaseUserTable[orcid]; found {
				kbaseFed.UserChan <- username
				kbaseFed.ErrorChan <- nil
			} else {
				kbaseFed.UserChan <- ""
				kbaseFed.ErrorChan <- fmt.Errorf("KBase user not found for ORCID %s", orcid)
			}
		case <-kbaseFed.UpdateChan: // update ORCID/user table
			var err error
			newUserTable, err := kbaseFed.readUserTable()
			if err == nil {
				kbaseUserTable = newUserTable
			}
			kbaseFed.ErrorChan <- err
		case <-kbaseFed.StopChan: // stop the subsystem
			kbaseFed.Started = false
			kbaseFed.ErrorChan <- nil
			return
		}
	}
}

type UserOrcidRecord struct {
	User, Orcid string
}

// reads the user table file within the DTS data directory, returning a map
// with ORCID keys associated with username values
func (kbaseFed *KBaseUserFederation) readUserTable() (map[string]string, error) {
	// open the CVS file containing the user mapping
	filename := kbaseFed.FilePath
	slog.Info(fmt.Sprintf("Reading KBase user table from %s", filename))
	file, err := os.Open(filename)
	if err != nil {
		return nil, &InvalidKBaseUserSpreadsheetError{
			File:    kbaseUserTableFile,
			Message: "nonexistent file",
		}
	}
	defer file.Close()

	// Scan the file line by line. Each line should contain 2 cells separated
	// by a comma. The first line is almost certainly a header with column names,
	// but we can't be sure, so we simply read every line, checking that
	//
	// * there are 2 entries separated by exactly one comma
	// * exactly one of the entries is a well-formed ORCID (xxxx-xxxx-xxxx-xxxx)
	// * the other entry is a non-empty string with no special characters
	//
	// Lines beginning with '#' are ignored. This allows us to handle "irregularities" manually.
	//
	// The structure of all the lines in the file must agree. Every line that doesn't conform to these
	// requirements is ignored. If there's at least one valid line, we clear the existing KBase user
	// table and add each (ORCID, user) pair to the user table.

	// Finally, there must be a 1:1 correspondence between KBase users and ORCIDs. Otherwise we can't
	// map between these items. We track (user, orcid) pairs that violate this constraint and report
	// them after we read the entire table.
	multipleUsersForOrcid := make(map[string][]string)
	multipleOrcidsForUser := make(map[string][]string)

	orcidColumn := -1
	userColumn := -1
	orcidsForUsers := make(map[string]string)
	usersForOrcids := make(map[string]string)
	reader := csv.NewReader(file)
	reader.Comment = '#'
	records, err := reader.ReadAll()
	if err != nil {
		return nil, &InvalidKBaseUserSpreadsheetError{
			File:    kbaseUserTableFile,
			Message: "Couldn't parse CVS file",
		}
	}
	for _, record := range records {
		if len(record) != 2 {
			return nil, &InvalidKBaseUserSpreadsheetError{
				File:    kbaseUserTableFile,
				Message: fmt.Sprintf("%d comma-separated columns found (2 expected)", len(record)),
			}
		}

		if orcidColumn == -1 { // find the column with an ORCID
			for i := range 2 {
				if isOrcid(record[i]) {
					orcidColumn = i
					userColumn = (i + 1) % 2 // user column's the other one
				}
			}
		} else if !isOrcid(record[orcidColumn]) {
			// we've already established the ORCID column, but this line disagrees,
			// so the whole file is suspect
			return nil, &InvalidKBaseUserSpreadsheetError{
				File:    kbaseUserTableFile,
				Message: "Different lines list username, ORCID data in different columns",
			}
		}

		if orcidColumn != -1 {
			orcid := record[orcidColumn]
			// ORCID column's okay, but what about the user column?
			if !isUsername(record[userColumn]) {
				continue
			}
			username := record[userColumn]

			// have we seen this ORCID or username before? It's okay, as long as everything
			// is consistent
			if existingUser, found := usersForOrcids[orcid]; found {
				if existingUser != username {
					_, found := multipleUsersForOrcid[orcid]
					if !found {
						multipleUsersForOrcid[orcid] = []string{existingUser, username}
					}
				}
			} else {
				usersForOrcids[orcid] = username
			}
			if existingOrcid, found := orcidsForUsers[username]; found {
				if existingOrcid != orcid {
					_, found := multipleOrcidsForUser[username]
					if !found {
						multipleOrcidsForUser[username] = []string{existingOrcid, orcid}
					}
				}
			} else {
				orcidsForUsers[username] = orcid
			}
		}
	}

	// report any violations of the 1:1 user <-> orcid correspondence
	if len(multipleUsersForOrcid) > 0 || len(multipleOrcidsForUser) > 0 {
		var b strings.Builder
		for orcid, users := range multipleUsersForOrcid {
			fmt.Fprintf(&b, "ORCID %s is associated with multiple KBase users: %s\n", orcid, strings.Join(users, ", "))
		}
		for user, orcids := range multipleOrcidsForUser {
			fmt.Fprintf(&b, "KBase user %s is associated with multiple ORCIDS: %s\n", user, strings.Join(orcids, ", "))
		}
		return nil, &InvalidKBaseUserSpreadsheetError{
			File:    kbaseUserTableFile,
			Message: fmt.Sprintf("No 1:1 correspondence exists between users and ORCIDS:\n %s", b.String()),
		}
	}

	if len(usersForOrcids) == 0 {
		return nil, &InvalidKBaseUserSpreadsheetError{
			File:    kbaseUserTableFile,
			Message: "No valid username/ORCID pairs found",
		}
	}

	return usersForOrcids, nil
}

// returns true iff s contains a valid username
func isUsername(s string) bool {
	return len(s) > 0 && !strings.ContainsFunc(s, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_'
	})
}

// returns true iff s contains a valid ORCID (nnnn-nnnn-nnnn-nnn[nX])
func isOrcid(s string) bool {
	matched, err := regexp.MatchString(`^(\d{4}-){3}\d{3}[\dX]$`, s)
	return err == nil && matched
}
