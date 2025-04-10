package services

import (
	"context"

	"github.com/google/uuid"
)

// this type encodes a JSON object for responding to root queries
type ServiceInfoResponse struct {
	Name          string `json:"name" example:"DTS" doc:"The name of the service API"`
	Version       string `json:"version" example:"1.0.0" doc:"The version string (major.minor.patch)"`
	Uptime        int    `json:"uptime" example:"345600" doc:"The time the service has been up (seconds)"`
	Documentation string `json:"documentation" example:"/docs" doc:"The OpenAPI documentation endpoint"`
}

// a response for a database-related query (GET)
type DatabaseResponse struct {
	Id           string `json:"id" example:"jdp" `
	Name         string `json:"name" example:"JGI Data portal"`
	Organization string `json:"organization" example:"Joint Genome Institute"`
	URL          string `json:"url" example:"https://data.jgi.doe.gov"`
}

// a response for a file search query (GET)
type SearchResultsResponse struct {
	// name of organization database
	Database string `json:"database" example:"jdp" doc:"the database searched"`
	// ElasticSearch query string
	Query string `json:"query" example:"prochlorococcus" doc:"the given query string"`
	// resources matching the query
	Descriptors []map[string]any `json:"resources" doc:"an array of validated Frictionless descriptors"`
}

// a response for a file metadata query (GET)
type FileMetadataResponse struct {
	// name of organization database
	Database string `json:"database" example:"jdp" doc:"the database searched"`
	// resources corresponding to given file IDs
	Descriptors []map[string]any `json:"resources" doc:"an array of validated Frictionless descriptors"`
}

// a request for a file transfer (POST)
type TransferRequest struct {
	// user ORCID
	Orcid string `json:"orcid" example:"0000-0002-9227-8514" doc:"ORCID for user requesting transfer"`
	// name of source database
	Source string `json:"source" example:"jdp" doc:"source database identifier"`
	// identifiers for files to be transferred
	FileIds []string `json:"file_ids" example:"[\"fileid1\", \"fileid2\"]" doc:"source-specific identifiers for files to be transferred"`
	// name of destination database
	Destination string `json:"destination" example:"kbase" doc:"destination database identifier"`
	// a Markdown description of the transfer request
	Description string `json:"description,omitempty" example:"# title\n* type: assembly\n" doc:"Markdown task description"`
	// machine-readable instructions for processing a payload at the destination site
	Instructions map[string]any `json:"instructions,omitempty" doc:"JSON object containing machine-readable instructions for processing payload at destination"`
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
