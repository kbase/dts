package services

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/kbase/dts/frictionless"
)

// This package-specific helper function writes a JSON payload to an
// http.ResponseWriter.
func writeJson(w http.ResponseWriter, data []byte, code int) {
	w.WriteHeader(code)
	if len(data) > 0 {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

// this type encodes a JSON object for responding to root queries
type ServiceInfoResponse struct {
	Name          string `json:"name" example:"DTS" doc:"The name of the service API"`
	Version       string `json:"version" example:"1.0.0" doc:"The version string (major.minor.patch)"`
	Uptime        int    `json:"uptime" example:"345600" doc:"The time the service has been up (seconds)"`
	Documentation string `json:"documentation" example:"/docs" doc:"The OpenAPI documentation endpoint"`
}

// This type holds information about an error that occurred responding to a
// request.
type ErrorResponse struct {
	// An HTTP error code
	Code int `json:"code"`
	// A descriptive error message
	Error string `json:"message"`
}

// This package-specific helper function writes an error to an
// http.ResponseWriter, giving it the proper status code, and encoding an
// ErrorResponse in the response body.
func writeError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	e := ErrorResponse{Code: code, Error: message}
	data, _ := json.Marshal(e)
	w.Write(data)
}

// a response for a database-related query (GET)
type DatabaseResponse struct {
	Id           string `json:"id" example:"jdp" `
	Name         string `json:"name" example:"JGI Data portal"`
	Organization string `json:"organization" example:"Joint Genome Institute"`
	URL          string `json:"url" example:"https://data.jgi.doe.gov"`
}

// a response for a query (GET)
type SearchResultsResponse struct {
	// name of organization database
	Database string `json:"database" example:"jdp" doc:"the database searched"`
	// ElasticSearch query string
	Query string `json:"query" example:"prochlorococcus" doc:"the given query string"`
	// Resources matching the query
	Resources []frictionless.DataResource `json:"resources" doc:"an array of Frictionless DataResources"`
}

// a request for a file transfer (POST)
type TransferRequest struct {
	// name of source database
	Source string `json:"source" example:"jdp" doc:"source database identifier"`
	// identifiers for files to be transferred
	FileIds []string `json:"file_ids" doc:"source-specific identifiers for files to be transferred"`
	// name of destination database
	Destination string `json:"destination" example:"kbase" doc:"destination database identifier"`
}

// a response for a file transfer request (POST)
type TransferResponse struct {
	// transfer job ID
	Id uuid.UUID `json:"id" doc:"a UUID for the requested transfer"`
}

// a response for a file transfer status request (GET)
type TransferStatusResponse struct {
	// transfer job ID
	Id string `json:"id"`
	// transfer job status
	Status string `json:"status"`
	// message (if any) related to status
	Message string `json:"message,omitempty"`
	// number of files being transferred
	NumFiles int `json:"num_files"`
	// number of files that have been completely transferred
	NumFilesTransferred int `json:"num_files_transferred"`
}

// TransferService defines the interface for our data transfer service.
type TransferService interface {
	// Starts the service on the selected port, returning an error that indicates
	// success or failure.
	Start(port int) error
	// Gracefully shuts down the service without interrupting active connections.
	Shutdown(ctx context.Context) error
	// Closes down the service, freeing all resources.
	Close()
}
