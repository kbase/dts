package services

import (
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humamux"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/net/netutil"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/tasks"
)

// Version numbers
var majorVersion = 0
var minorVersion = 1
var patchVersion = 0

// Version string
var version = fmt.Sprintf("%d.%d.%d", majorVersion, minorVersion, patchVersion)

// This type implements the TransferService interface, allowing file transfers
// from JGI (via the JGI Data Portal) to KBase via Globus.
type prototype struct {
	// name of the service
	Name string
	// service version identifier
	Version string
	// time which the service was started
	StartTime time.Time
	// port on which the service currently runs
	Port int
	// router for REST endpoints
	Router *mux.Router
	// API wrapper
	API huma.API
	// HTTP server.
	Server *http.Server
}

// authorize clients for the DTS, returning the client's ORCID ID and an error
// describing any issue encountered
func authorize(authorizationHeader string) (string, error) {
	if !strings.Contains(authorizationHeader, "Bearer") {
		return "", fmt.Errorf("Invalid authorization header")
	}
	b64Token := authorizationHeader[len("Bearer "):]
	accessTokenBytes, err := base64.StdEncoding.DecodeString(b64Token)
	if err != nil {
		return "", huma.Error401Unauthorized(err.Error())
	}
	accessToken := strings.TrimSpace(string(accessTokenBytes))

	// check the access token against the KBase auth server
	// and fetch the first ORCID associated with it
	authServer, err := auth.NewKBaseAuthServer(accessToken)
	var orcid string
	var orcids []string
	if err == nil {
		orcids, err = authServer.Orcids()
		if err == nil {
			orcid = orcids[0]
		}
	}
	if err != nil {
		return orcid, huma.Error401Unauthorized(err.Error())
	} else {
		return orcid, nil
	}
}

type ServiceInfoOutput struct {
	Body ServiceInfoResponse `doc:"information about the service itself"`
}

// handler method for root
func (service *prototype) getRoot(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization"`
	}) (*ServiceInfoOutput, error) {

	_, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	slog.Info("Querying root endpoint...")
	return &ServiceInfoOutput{
		Body: ServiceInfoResponse{
			Name:          service.Name,
			Version:       service.Version,
			Uptime:        int(service.uptime()),
			Documentation: "/docs",
		},
	}, nil
}

type DatabaseOutput struct {
	Body DatabaseResponse `doc:"Information about the requested available database"`
}

type DatabasesOutput struct {
	Body []DatabaseResponse `doc:"A list of information about available databases"`
}

// handler method for querying all databases
func (service *prototype) getDatabases(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization"`
	}) (*DatabasesOutput, error) {

	_, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	slog.Info("Querying organizational databases...")
	output := &DatabasesOutput{
		Body: make([]DatabaseResponse, 0),
	}
	for dbName, db := range config.Databases {
		output.Body = append(output.Body, DatabaseResponse{
			Id:           dbName,
			Name:         db.Name,
			Organization: db.Organization,
		})
	}
	slices.SortFunc(output.Body, func(db1, db2 DatabaseResponse) int { // sort by name
		return cmp.Compare(db1.Name, db2.Name)
	})
	return output, err
}

// handler method for querying a single database for its metadata
func (service *prototype) getDatabase(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization" doc:"Authorization header with encoded access token"`
		Id            string `path:"db" example:"jdp" doc:"the abbreviated name of a database"`
	}) (*DatabaseOutput, error) {

	_, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	slog.Info(fmt.Sprintf("Querying database %s...", input.Id))
	db, ok := config.Databases[input.Id]
	if !ok {
		return nil, huma.Error404NotFound(fmt.Sprintf("Database %s not found", input.Id))
	}
	return &DatabaseOutput{
		Body: DatabaseResponse{
			Id:           input.Id,
			Name:         db.Name,
			Organization: db.Organization,
		},
	}, nil
}

type SearchResultsOutput struct {
	Body SearchResultsResponse `doc:"Search results containing matching files that match the given query"`
}

// handle search queries for files of interest
func (service *prototype) searchDatabase(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization" doc:"Authorization header with encoded access token"`
		Database      string `query:"database" example:"jdp" doc:"The ID of the database to search"`
		Query         string `query:"query" example:"prochlorococcus" doc:"A query used to search the database for matching files"`
		Offset        int    `query:"offset" example:"100" doc:"Search results begin at the given offset"`
		Limit         int    `query:"limit" example:"50" doc:"Limits the number of search results returned"`
	}) (*SearchResultsOutput, error) {

	orcid, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// is the database valid?
	_, ok := config.Databases[input.Database]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", input.Database)
	}

	slog.Info(fmt.Sprintf("Searching database %s for files...", input.Database))
	db, err := databases.NewDatabase(orcid, input.Database)
	if err != nil {
		return nil, err
	}
	results, err := db.Search(databases.SearchParameters{
		Query: input.Query,
		Pagination: databases.SearchPaginationParameters{
			Offset: input.Offset,
			MaxNum: input.Limit,
		},
	})
	if err != nil {
		return nil, err
	}
	return &SearchResultsOutput{
		Body: SearchResultsResponse{
			Database:  input.Database,
			Query:     input.Query,
			Resources: results.Resources,
		},
	}, nil
}

type TransferOutput struct {
	Body   TransferResponse `doc:"A UUID for the requested transfer"`
	Status int
}

// handler method for initiating a file transfer operation
func (service *prototype) createTransfer(ctx context.Context,
	input *struct {
		Authorization string          `header:"Authorization" doc:"Authorization header with encoded access token"`
		Body          TransferRequest `doc:"The body of a POST request for a file transfer"`
		ContentType   string          `header:"Content-Type" doc:"Content-Type header (must be application/json)"`
	}) (*TransferOutput, error) {

	orcid, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	taskId, err := tasks.Create(orcid, input.Body.Source,
		input.Body.Destination, input.Body.FileIds)
	if err != nil {
		return nil, err
	}
	return &TransferOutput{
		Body: TransferResponse{
			Id: taskId,
		},
		Status: http.StatusCreated,
	}, nil
}

// convert a transfer status code to a nice human-friendly string
func statusAsString(statusCode endpoints.TransferStatusCode) string {
	switch statusCode {
	case endpoints.TransferStatusStaging:
		return "staging"
	case endpoints.TransferStatusActive:
		return "active"
	case endpoints.TransferStatusInactive:
		return "inactive"
	case endpoints.TransferStatusFinalizing:
		return "finalizing"
	case endpoints.TransferStatusSucceeded:
		return "succeeded"
	case endpoints.TransferStatusFailed:
		return "failed"
	}
	return "unknown"
}

type TransferStatusOutput struct {
	Body TransferStatusResponse `doc:"A status message for the transfer task with the given ID"`
}

// handler method for getting the status of a transfer
func (service *prototype) getTransferStatus(ctx context.Context,
	input *struct {
		Authorization string    `header:"authorization" doc:"Authorization header with encoded access token"`
		Id            uuid.UUID `path:"id" example:"de9a2d6a-f5c9-4322-b8a7-8121d83fdfc2" doc:"the UUID for the requested transfer"`
	}) (*TransferStatusOutput, error) {

	_, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// fetch the status for the job using the appropriate task data
	status, err := tasks.Status(input.Id)
	if err != nil {
		return nil, huma.Error404NotFound(err.Error())
	}
	return &TransferStatusOutput{
		Body: TransferStatusResponse{
			Id:                  input.Id.String(),
			Status:              statusAsString(status.Code),
			Message:             status.Message,
			NumFiles:            status.NumFiles,
			NumFilesTransferred: status.NumFilesTransferred,
		},
	}, nil
}

type TaskDeletionOutput struct {
	Status int
}

// handler method for deleting (canceling) an existing transfer
func (service *prototype) deleteTransfer(ctx context.Context,
	input *struct {
		Authorization string    `header:"authorization" doc:"Authorization header with encoded access token"`
		Id            uuid.UUID `path:"id" example:"de9a2d6a-f5c9-4322-b8a7-8121d83fdfc2" doc:"the UUID for the requested transfer"`
	}) (*TaskDeletionOutput, error) {

	// request that the task be canceled
	err := tasks.Cancel(input.Id)
	if err != nil {
		return nil, err
	}
	return &TaskDeletionOutput{
		Status: http.StatusAccepted,
	}, nil
}

// returns the uptime for the service in seconds
func (service *prototype) uptime() float64 {
	return time.Since(service.StartTime).Seconds()
}

// constructs a prototype file transfer service given our configuration
func NewDTSPrototype() (TransferService, error) {

	// validate our configuration
	if config.Service.Endpoint == "" {
		return nil, fmt.Errorf("No service endpoint was specified.")
	}
	if len(config.Databases) == 0 {
		return nil, fmt.Errorf("No databases were specified.")
	}
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("No endpoints were specified.")
	}

	service := new(prototype)
	service.Name = "DTS prototype"
	service.Version = version
	service.Port = -1

	// set up routing
	service.Router = mux.NewRouter()
	api := humamux.New(service.Router, huma.DefaultConfig(service.Name, service.Version))
	huma.Get(api, "/", service.getRoot)

	// API v1
	huma.Get(api, "/api/v1/databases", service.getDatabases)
	huma.Get(api, "/api/v1/databases/{db}", service.getDatabase)
	huma.Get(api, "/api/v1/files", service.searchDatabase)
	huma.Post(api, "/api/v1/transfers", service.createTransfer)
	huma.Get(api, "/api/v1/transfers/{id}", service.getTransferStatus)
	huma.Delete(api, "/api/v1/transfers/{id}", service.deleteTransfer)

	return service, nil
}

// starts the prototype data transfer service
func (service *prototype) Start(port int) error {
	slog.Info(fmt.Sprintf("Starting %s service on port %d...", service.Name, port))
	slog.Info(fmt.Sprintf("(Accepting up to %d connections)", config.Service.MaxConnections))

	service.StartTime = time.Now()

	// create a listener that limits the number of incoming connections
	service.Port = port
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer listener.Close()
	listener = netutil.LimitListener(listener, config.Service.MaxConnections)

	// start tasks processing
	err = tasks.Start()
	if err != nil {
		return err
	}

	// start the server
	service.Server = &http.Server{
		Handler: service.Router}
	err = service.Server.Serve(listener)

	// we don't report the server closing as an error
	if err != http.ErrServerClosed {
		return err
	}
	return nil
}

// gracefully shuts down the service without interrupting active connections
func (service *prototype) Shutdown(ctx context.Context) error {
	tasks.Stop()
	if service.Server != nil {
		return service.Server.Shutdown(ctx)
	}
	return nil
}

// closes down the service abruptly, freeing all resources
func (service *prototype) Close() {
	tasks.Stop()
	if service.Server != nil {
		service.Server.Close()
	}
}
