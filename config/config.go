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

	"gopkg.in/yaml.v3"
)

// a type with service configuration parameters
type serviceConfig struct {
	// port on which the service listens
	Port int `json:"port,omitempty" yaml:"port,omitempty"`
	// maximum number of allowed incoming connections
	// default: 100
	MaxConnections int `json:"max_connections,omitempty" yaml:"max_connections,omitempty"`
	// maximum size of requested payload for transfer, past which transfer
	// requests are rejected (gigabytes)
	MaxPayloadSize int `json:"max_payload_size,omitempy" yaml:"max_payload_size,omitempty"`
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
}

// global config variables
var Service serviceConfig
var Endpoints map[string]endpointConfig
var Databases map[string]databaseConfig
var MessageQueues map[string]messageQueueConfig

// This struct performs the unmarshalling from the YAML config file and then
// copies its fields to the globals above.
type configFile struct {
	Service       serviceConfig                 `yaml:"service"`
	Databases     map[string]databaseConfig     `yaml:"databases"`
	Endpoints     map[string]endpointConfig     `yaml:"endpoints"`
	MessageQueues map[string]messageQueueConfig `yaml:"message_queues"`
}

// This helper locates and reads a configuration file, returning an error
// indicating success or failure. All environment variables of the form
// ${ENV_VAR} are expanded.
func readConfig(bytes []byte) error {
	// before we do anything else, expand any provided environment variables
	bytes = []byte(os.ExpandEnv(string(bytes)))

	var conf configFile
	conf.Service.Port = 8080
	conf.Service.MaxConnections = 100
	conf.Service.MaxPayloadSize = 100
	conf.Service.PollInterval = int(time.Minute / time.Millisecond)
	conf.Service.DeleteAfter = 7 * 24 * 3600
	err := yaml.Unmarshal(bytes, &conf)
	if err != nil {
		log.Printf("Couldn't parse configuration data: %s\n", err)
		return err
	}

	// copy the config data into place, performing any needed conversions
	Service = conf.Service

	Endpoints = conf.Endpoints
	for name, endpoint := range Endpoints {
		if endpoint.Root == "" {
			endpoint.Root = "/"
			Endpoints[name] = endpoint
		}
	}

	Databases = conf.Databases
	MessageQueues = conf.MessageQueues

	return err
}

func validateServiceParameters(params serviceConfig) error {
	if params.Port < 0 || params.Port > 65535 {
		return fmt.Errorf("Invalid port: %d (must be 0-65535)", params.Port)
	}
	if params.MaxConnections <= 0 {
		return fmt.Errorf("Invalid max_connections: %d (must be positive)",
			params.MaxConnections)
	}
	if params.Endpoint != "" {
		if _, endpointFound := Endpoints[params.Endpoint]; !endpointFound {
			return fmt.Errorf("Invalid service endpoint: %s", params.Endpoint)
		}
	}
	if params.PollInterval <= 0 {
		return fmt.Errorf("Non-positive poll interval specified: (%d s)",
			params.PollInterval)
	}
	if params.DeleteAfter <= 0 {
		return fmt.Errorf("Non-positive task deletion period specified: (%d h)",
			params.DeleteAfter)
	}
	return nil
}

func validateEndpoints(endpoints map[string]endpointConfig) error {
	for label, endpoint := range endpoints {
		if endpoint.Id.String() == "" { // invalid endpoint UUID
			return fmt.Errorf("Invalid UUID specified for endpoint '%s'", label)
		} else if endpoint.Provider == "" { // no provider given
			return fmt.Errorf("No provider specified for endpoint '%s'", label)
		}
	}
	return nil
}

func validateDatabases(databases map[string]databaseConfig) error {
	for name, db := range databases {
		if db.Endpoint == "" {
			return fmt.Errorf("No endpoint given for database '%s'", name)
		}
	}
	return nil
}

// This helper validates the given configfile, returning an error that indicates
// success or failure.
func validateConfig() error {
	err := validateServiceParameters(Service)
	if err != nil {
		return err
	}
	err = validateEndpoints(Endpoints)
	if err != nil {
		return err
	}
	err = validateDatabases(Databases)
	return err
}

// Initializes the ID mapping service configuration using the given YAML byte
// data.
func Init(yamlData []byte) error {
	err := readConfig(yamlData)
	if err != nil {
		return err
	}
	err = validateConfig()
	return err
}
