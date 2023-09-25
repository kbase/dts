package config

import (
	"fmt"
	"log"
	"os"

	//	"github.com/confluentinc/confluent-kafka-go/kafka"
	//	"github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

// a type with service configuration parameters
type serviceConfig struct {
	// port on which the service listens
	Port int `json:"port" yaml:"port"`
	// maximum number of allowed incoming connections
	MaxConnections int `json:"max_connections" yaml:"max_connections"`
	// polling interval for checking transfer statuses (seconds)
	PollInterval int `json:"poll_interval" yaml:"poll_interval"`
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
	conf.Service.PollInterval = 60000
	err := yaml.Unmarshal(bytes, &conf)
	if err != nil {
		log.Printf("Couldn't parse configuration data: %s\n", err)
		return err
	}

	// copy the config data into place
	Service = conf.Service
	Endpoints = conf.Endpoints
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
	return nil
}

func validateEndpoints(endpoints map[string]endpointConfig) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("No endpoints were provided!")
	}
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
	if len(databases) == 0 {
		return fmt.Errorf("No databases were provided!")
	}
	for name, db := range databases {
		if db.URL == "" {
			return fmt.Errorf("No URL given for database '%s'", name)
		}
	}
	return nil
}

// This helper validates the given configfile, returning an error that indicates
// success or failure.
func validateConfig() error {
	err := validateServiceParameters(Service)
	if err == nil {
		err = validateEndpoints(Endpoints)
		if err == nil {
			err = validateDatabases(Databases)
		}
	}

	return err
}

// Initializes the ID mapping service configuration using the given YAML byte
// data.
func Init(yamlData []byte) error {
	err := readConfig(yamlData)
	if err == nil {
		err = validateConfig()
	}
	return err
}
