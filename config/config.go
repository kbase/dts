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

package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// a type with service configuration parameters
type serviceConfig struct {
	// secret for decrypting access key authorization file
	Secret string `json:"secret,omitempty" yaml:"secret,omitempty"`
	// port on which the service listens
	Port int `json:"port,omitempty" yaml:"port,omitempty"`
	// maximum number of allowed incoming connections
	// default: 100
	MaxConnections int `json:"max_connections,omitempty" yaml:"max_connections,omitempty"`
	// maximum size of requested payload for transfer, past which transfer
	// requests are rejected (gigabytes)
	MaxPayloadSize float64 `json:"max_payload_size,omitempty" yaml:"max_payload_size,omitempty"`
	// polling interval for checking transfer statuses (milliseconds)
	// default: 1 minute
	PollInterval int `json:"poll_interval" yaml:"poll_interval"`
	// name of endpoint with access to local filesystem
	// (for generating and transferring manifests)
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	// name of existing directory in which DTS can store persistent data
	DataDirectory string `json:"data_dir" yaml:"data_dir,omitempty"`
	// name of existing directory in which DTS writes manifest files (must be
	// visible to endpoints)
	ManifestDirectory string `json:"manifest_dir" yaml:"manifest_dir"`
	// time after which information about a completed transfer is deleted (seconds)
	// default: 7 days
	DeleteAfter int `json:"delete_after" yaml:"delete_after"`
	// flag indicating whether debug logging and other tools are enabled
	Debug bool `json:"debug" yaml:"debug"`
	// flag indicating whether an endpoint double-checks that files are staged
	// (if not set, the endpoint will trust a database for staging status)
	DoubleCheckStaging bool `json:"double_check_staging" yaml:"double_check_staging"`
}

// global config variables
var Service serviceConfig
var Credentials map[string]credentialConfig
var Endpoints map[string]endpointConfig
var Databases map[string]databaseConfig

// This struct performs the unmarshalling from the YAML config file and then
// copies its fields to the globals above.
//
// NOTE: Instances of this struct are beginning to be used to pass configuration
// data around internally, but this is not yet complete. Once that is done, the
// global variables above can be removed.
type Config struct {
	Service     serviceConfig               `yaml:"service"`
	Credentials map[string]credentialConfig `yaml:"credentials"`
	Databases   map[string]databaseConfig   `yaml:"databases"`
	Endpoints   map[string]endpointConfig   `yaml:"endpoints"`
}

// This helper locates and reads the selected sections in a configuration file,
// returning an error indicating success or failure. All environment variables
// of the form ${ENV_VAR} are expanded.
func readConfig(bytes []byte, service, credentials, databases, endpoints bool) (Config, error) {
	// before we do anything else, expand any provided environment variables
	bytes = []byte(os.ExpandEnv(string(bytes)))

	var conf Config
	conf.Service.Port = 8080
	conf.Service.MaxConnections = 100
	conf.Service.MaxPayloadSize = 100.0 // gigabytes
	conf.Service.PollInterval = int(time.Minute / time.Millisecond)
	conf.Service.DeleteAfter = 7 * 24 * 3600

	err := yaml.Unmarshal(bytes, &conf)
	if err != nil {
		log.Printf("Couldn't parse configuration data: %s\n", err)
		return Config{}, err
	}

	if service {
		// copy the config data into place, performing any needed conversions
		Service = conf.Service
	}

	if credentials {
		Credentials = conf.Credentials
	}

	if endpoints {
		Endpoints = conf.Endpoints
		for name, endpoint := range Endpoints {
			if endpoint.Root == "" {
				endpoint.Root = "/"
				Endpoints[name] = endpoint
			}
		}
	}

	if databases {
		Databases = conf.Databases
	}

	return conf, err
}

func validateServiceParameters(params serviceConfig) error {
	if params.Port < 0 || params.Port > 65535 {
		return &InvalidServiceConfigError{
			Message: fmt.Sprintf("Invalid port: %d (must be 0-65535)", params.Port),
		}
	}
	if params.MaxConnections <= 0 {
		return &InvalidServiceConfigError{
			Message: fmt.Sprintf("Invalid max_connections: %d (must be positive)",
				params.MaxConnections),
		}
	}
	if params.Endpoint != "" {
		if _, found := Endpoints[params.Endpoint]; !found {
			return &InvalidServiceConfigError{
				Message: fmt.Sprintf("Invalid endpoint: %s", params.Endpoint),
			}
		}
	} else if len(Endpoints) > 0 {
		return &InvalidServiceConfigError{
			Message: "No service endpoint specified",
		}
	}
	if params.PollInterval <= 0 {
		return &InvalidServiceConfigError{
			Message: fmt.Sprintf("Non-positive poll interval specified: (%d s)",
				params.PollInterval),
		}
	}
	if params.DeleteAfter <= 0 {
		return &InvalidServiceConfigError{
			Message: fmt.Sprintf("Non-positive task deletion period specified: (%d h)",
				params.DeleteAfter),
		}
	}
	return nil
}

func validateCredentials(credentials map[string]credentialConfig) error {
	for name, credential := range credentials {
		if credential.Id == "" {
			return &InvalidCredentialConfigError{
				Credential: name,
				Message:    "Invalid credential ID",
			}
		}
	}
	return nil
}

func validateEndpoints(endpoints map[string]endpointConfig) error {
	if len(endpoints) == 0 {
		return &InvalidServiceConfigError{
			Message: "No endpoints configured",
		}
	}
	for name, endpoint := range endpoints {
		if endpoint.Id == uuid.Nil { // invalid endpoint UUID
			return &InvalidEndpointConfigError{
				Endpoint: name,
				Message:  "Invalid UUID",
			}
		}
		if endpoint.Provider == "" { // no provider given
			return &InvalidEndpointConfigError{
				Endpoint: name,
				Message:  "No provider specified",
			}
		}
	}
	return nil
}

func validateDatabases(databases map[string]databaseConfig) error {
	if len(databases) == 0 {
		return &InvalidServiceConfigError{
			Message: "No databases configured",
		}
	}
	for name, db := range databases {
		if db.Endpoint == "" && len(db.Endpoints) == 0 {
			return &InvalidDatabaseConfigError{
				Database: name,
				Message:  "No endpoints specified",
			}
		} else if db.Endpoint != "" && len(db.Endpoints) > 0 {
			return &InvalidDatabaseConfigError{
				Database: name,
				Message:  "EITHER endpoint OR endpoints may be specified, but not both",
			}
		} else if db.Endpoint != "" {
			// does the endpoint exist in our configuration?
			if _, found := Endpoints[db.Endpoint]; !found {
				return &InvalidDatabaseConfigError{
					Database: name,
					Message:  fmt.Sprintf("Invalid endpoint for database %s: %s", name, db.Endpoint),
				}
			}
		} else {
			// do all functional endpoints exist in our configuration?
			for functionalName, endpointName := range db.Endpoints {
				if _, found := Endpoints[endpointName]; !found {
					return &InvalidDatabaseConfigError{
						Database: name,
						Message:  fmt.Sprintf("Invalid %s endpoint for database %s: %s", functionalName, name, endpointName),
					}
				}
			}
		}
	}
	return nil
}

// This helper validates the given sections in the configuration, returning an
// error that indicates success or failure.
func (c Config) validateConfig(service, credentials, databases, endpoints bool) error {
	var err error
	if service {
		err = validateServiceParameters(c.Service)
		if err != nil {
			return err
		}
	}

	if credentials {
		err = validateCredentials(c.Credentials)
		if err != nil {
			return err
		}
	}

	if endpoints {
		err = validateEndpoints(c.Endpoints)
		if err != nil {
			return err
		}
	}

	if databases {
		err = validateDatabases(c.Databases)
	}
	return err
}

// Initializes the entire service configuration using the given YAML byte data.
func Init(yamlData []byte) error {
	_, err := InitSelected(yamlData, true, true, true, true)
	return err
}

// Returns a Config struct initialized from the given YAML byte data.
func NewConfig(yamlData []byte) (Config, error) {
	return InitSelected(yamlData, true, true, true, true)
}

// Initializes the selected sections in the service configuration using the
// given YAML byte data.
func InitSelected(yamlData []byte, service, credentials, databases, endpoints bool) (Config, error) {
	conf, err := readConfig(yamlData, service, credentials, databases, endpoints)
	if err != nil {
		return Config{}, err
	}
	err = conf.validateConfig(service, credentials, databases, endpoints)
	return conf, err
}
