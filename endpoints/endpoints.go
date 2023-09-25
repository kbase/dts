package endpoints

import (
	"fmt"

	"dts/config"
	"dts/core"
	"dts/endpoints/globus"
)

// we maintain a table of endpoint instances, identified by their names
var allEndpoints map[string]core.Endpoint = make(map[string]core.Endpoint)

// creates an endpoint based on the configured type, or returns an existing
// instance
func NewEndpoint(endpointName string) (core.Endpoint, error) {
	var err error

	// do we have one of these already?
	endpoint, found := allEndpoints[endpointName]
	if !found {
		// is it a Globus endpoint?
		if config.Endpoints[endpointName].Provider == "globus" {
			endpoint, err = globus.NewEndpoint(endpointName)
		}

		// stash it
		if endpoint != nil {
			allEndpoints[endpointName] = endpoint
		}
	} else {
		err = fmt.Errorf("Invalid endpoint: %s", endpointName)
	}
	return endpoint, err
}
