package databases

import (
	"fmt"

	"dts/core"
	"dts/databases/jdp"
)

// we maintain a table of database instances, identified by their names
var allDatabases map[string]core.Database = make(map[string]core.Database)

// creates a database proxy associated with the given ORCID ID, based on the
// configured type, or returns an existing instance
func NewDatabase(orcid, dbName string) (core.Database, error) {
	var err error

	// do we have one of these already?
	key := fmt.Sprintf("orcid: %s db: %s", orcid, dbName)
	db, found := allDatabases[key]
	if !found {
		// go get one
		if dbName == "jdp" {
			db, err = jdp.NewDatabase(orcid)
		} else {
			err = fmt.Errorf("Unknown database type for '%s'", dbName)
		}
		if err == nil {
			allDatabases[dbName] = db // stash it
		}
	}
	return db, err
}
