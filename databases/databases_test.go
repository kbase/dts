package databases

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewJDPDatabase(t *testing.T) {
	assert := assert.New(t) // binds assert to t
	orcid := os.Getenv("DTS_KBASE_TEST_ORCID")
	jdpDb, err := NewDatabase(orcid, "jdp")
	assert.NotNil(jdpDb, "JDP database not created")
	assert.Nil(err, "JDP database creation encountered an error")
}
