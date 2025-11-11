package databases

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestDatabase struct {
	Name string
}

func (td *TestDatabase) SpecificSearchParameters() map[string]any {
	return map[string]any{}
}

func (td *TestDatabase) Search(query string, params SearchParameters) (SearchResults, error) {
	return SearchResults{}, nil
}

func (td *TestDatabase) Descriptors(orcid string, fileIds []string) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func (td *TestDatabase) EndpointNames() ([]string, error) {
	return []string{"test-endpoint"}, nil
}

func (td *TestDatabase) StageFiles(orcid string, fileIds []string) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (td *TestDatabase) StagingStatus(id uuid.UUID) (StagingStatus, error) {
	var s StagingStatus
	return s, nil
}

func (td *TestDatabase) Finalize(orcid string, id uuid.UUID) error {
	return nil
}

func (td *TestDatabase) LocalUser(orcid string) (string, error) {
	return "localuser", nil
}

func (td *TestDatabase) Save() (DatabaseSaveState, error) {
	if td.Name == "badSaver" {
		return DatabaseSaveState{}, fmt.Errorf("simulated save error")
	}
	if td.Name == "wrongNameSaver" {
		state := DatabaseSaveState{
			Name: "unexpectedName",
			Data: []byte{0, 1, 2, 3},
		}
		return state, nil
	}
	sum := uuid.NewSHA1(uuid.NameSpaceURL, []byte(td.Name))
	state := DatabaseSaveState{
		Name: td.Name,
		Data: sum[:],
	}

	return state, nil
}
func (td *TestDatabase) Load(state DatabaseSaveState) error {
	if td.Name == "badLoader" {
		return fmt.Errorf("simulated load error")
	}

	expected := uuid.NewSHA1(uuid.NameSpaceURL, []byte(td.Name))

	if state.Name != td.Name {
		return fmt.Errorf("unexpected state name: got %q", state.Name)
	}

	if len(state.Data) != len(expected) {
		return fmt.Errorf("unexpected state data length: got %d, want %d", len(state.Data), len(expected))
	}

	for i := range expected {
		if state.Data[i] != expected[i] {
			return fmt.Errorf("state data mismatch at byte %d", i)
		}
	}

	return nil
}

func reset() {
	firstTime_ = true
	allDatabases_ = make(map[string]Database)
	createDatabaseFuncs_ = make(map[string]func() (Database, error))
}

func TestRunner(t *testing.T) {
	tester := SerialTests{Test: t}
	reset()
	tester.TestInvalidDatabase()
	reset()
	tester.TestDatabaseRegistration()
	reset()
	tester.TestDuplicateDatabaseRegistration()
	reset()
	tester.TestBadDatabaseCreateFunc()
	reset()
	tester.TestDatabaseSaveLoad()
	reset()
	tester.TestDatabaseSaveError()
	reset()
	tester.TestDatabaseSaveWrongNameError()
	reset()
	tester.TestDatabaseLoadError()
}

type SerialTests struct{ Test *testing.T }

func (t *SerialTests) TestInvalidDatabase() {
	assert := assert.New(t.Test)
	bbDb, err := NewDatabase("booga booga")
	assert.Nil(bbDb, "Invalid database should not be created")
	assert.NotNil(err, "Invalid database creation did not report an error")
}

func (t *SerialTests) TestDatabaseRegistration() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("testdb", func() (Database, error) {
		return &TestDatabase{}, nil
	})
	assert.Nil(err, "Registering test database failed")

	found := HaveDatabase("testdb")
	assert.True(found, "Registered database not found")

	found = HaveDatabase("nonexistentdb")
	assert.False(found, "Non-registered database found")

	db, err := NewDatabase("testdb")
	assert.NotNil(db, "Creating registered database failed")
	assert.Nil(err, "Creating registered database reported an error")
}

func (t *SerialTests) TestDuplicateDatabaseRegistration() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("duplicatedb", func() (Database, error) {
		return &TestDatabase{}, nil
	})
	assert.Nil(err, "First registration of duplicatedb failed")

	err = RegisterDatabase("duplicatedb", func() (Database, error) {
		return &TestDatabase{}, nil
	})
	assert.NotNil(err, "Duplicate registration of duplicatedb did not report an error")
}

func (t *SerialTests) TestBadDatabaseCreateFunc() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("baddb", func() (Database, error) {
		return nil, &NotFoundError{"baddb"}
	})
	assert.NotNil(err, "Registering bad database did not report an error")
}

func (t *SerialTests) TestDatabaseSaveLoad() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("savedb", func() (Database, error) {
		return &TestDatabase{Name: "savedb"}, nil
	})
	assert.Nil(err, "Registering savedb database failed")
	err = RegisterDatabase("anotherdb", func() (Database, error) {
		return &TestDatabase{Name: "anotherdb"}, nil
	})
	assert.Nil(err, "Registering anotherdb database failed")

	db, err := NewDatabase("savedb")
	assert.Nil(err, "Creating savedb database failed")
	assert.NotNil(db, "Creating savedb database returned nil")

	anotherDb, err := NewDatabase("anotherdb")
	assert.Nil(err, "Creating anotherdb database failed")
	assert.NotNil(anotherDb, "Creating anotherdb database returned nil")

	states, err := Save()
	assert.Nil(err, "Saving databases failed")
	assert.NotNil(states, "Saving databases returned nil states")

	err = Load(states)
	assert.Nil(err, "Loading databases failed")
}

func (t *SerialTests) TestDatabaseSaveError() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("badSaver", func() (Database, error) {
		return &TestDatabase{Name: "badSaver"}, nil
	})
	assert.Nil(err, "Registering badSaver database failed")

	db, err := NewDatabase("badSaver")
	assert.Nil(err, "Creating badSaver database failed")
	assert.NotNil(db, "Creating badSaver database returned nil")

	_, err = Save()
	assert.NotNil(err, "Saving databases with bad saver did not report an error")
}

func (t *SerialTests) TestDatabaseSaveWrongNameError() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("wrongNameSaver", func() (Database, error) {
		return &TestDatabase{Name: "wrongNameSaver"}, nil
	})
	assert.Nil(err, "Registering wrongNameSaver database failed")

	db, err := NewDatabase("wrongNameSaver")
	assert.Nil(err, "Creating wrongNameSaver database failed")
	assert.NotNil(db, "Creating wrongNameSaver database returned nil")

	states, err := Save()
	assert.Nil(err, "Saving databases with wrong name saver failed")
	assert.NotNil(states, "Saving databases with wrong name saver returned nil states")

	err = Load(states)
	assert.NotNil(err, "Loading databases with wrong name saver did not report an error")
	assert.NotNil(states, "Loading databases with wrong name saver returned nil states")
}

func (t *SerialTests) TestDatabaseLoadError() {
	assert := assert.New(t.Test)
	err := RegisterDatabase("badLoader", func() (Database, error) {
		return &TestDatabase{Name: "badLoader"}, nil
	})
	assert.Nil(err, "Registering badLoader database failed")

	db, err := NewDatabase("badLoader")
	assert.Nil(err, "Creating badLoader database failed")
	assert.NotNil(db, "Creating badLoader database returned nil")

	states, err := Save()
	assert.Nil(err, "Saving databases with bad loader failed")
	assert.NotNil(states, "Saving databases with bad loader returned nil states")

	err = Load(states)
	assert.NotNil(err, "Loading databases with bad loader did not report an error")
}
