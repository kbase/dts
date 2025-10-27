package kbase

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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