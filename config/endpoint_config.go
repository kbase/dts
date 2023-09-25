package config

import (
	"github.com/google/uuid"
)

type endpointConfig struct {
	// descriptive name of the endpoint
	Name string `yaml:"name"`
	// the endpoint ID (uuid)
	Id uuid.UUID `yaml:"id"`
	// the name of the provider (e.g. "globus")
	Provider string `yaml:"provider"`
	// authentication/authorization data (client secret used to request access token)
	Auth authConfig `yaml:"auth"`
}
