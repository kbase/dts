package services

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humamux"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/net/netutil"

	"github.com/kbase/dts/auth"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/databases"
	"github.com/kbase/dts/endpoints"
	"github.com/kbase/dts/tasks"
)

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

// constructs a prototype file transfer service given our configuration
func NewDTSPrototype() (TransferService, error) {

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
	huma.Get(api, "/api/v1/databases/{db}/search-parameters", service.getDatabaseSearchParameters)
	huma.Get(api, "/api/v1/files", service.searchDatabase)
	huma.Post(api, "/api/v1/files", service.searchDatabaseWithSpecificParams)
	huma.Get(api, "/api/v1/files/by-id", service.fetchFileMetadata)
	huma.Post(api, "/api/v1/transfers", service.createTransfer)
	huma.Get(api, "/api/v1/transfers/{id}", service.getTransferStatus)
	huma.Delete(api, "/api/v1/transfers/{id}", service.deleteTransfer)

	return service, nil
}

// starts the prototype data transfer service
func (service *prototype) Start(port int) error {
	slog.Info(fmt.Sprintf("Starting %s v%s on port %d...", service.Name, version, port))
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

//-----------
// Internals
//-----------

// Version numbers
var majorVersion = 0
var minorVersion = 8
var patchVersion = 2

// Version string
var version = fmt.Sprintf("%d.%d.%d", majorVersion, minorVersion, patchVersion)

// Authorize clients for the DTS, returning information about the user
// corresponding to the token in the header (or an error describing any issue
// encountered). This returns either an auth.User (if authorized via the DTS Authenticator) or
// an auth.Client (if authorized via the KBase auth2 server).
func authorize(authorizationHeader string) (any, error) {
	if !strings.Contains(authorizationHeader, "Bearer ") {
		return auth.User{}, fmt.Errorf("Invalid authorization header")
	}
	b64Token := authorizationHeader[len("Bearer "):]
	accessTokenBytes, err := base64.StdEncoding.DecodeString(b64Token)
	if err != nil {
		return auth.Client{}, huma.Error401Unauthorized(err.Error())
	}
	accessToken := strings.TrimSpace(string(accessTokenBytes))

	var client auth.Client

	// first, check the access token against the DTS authenticator
	authenticator, err := auth.NewAuthenticator()
	if err == nil {
		var user auth.User
		user, err = authenticator.GetUser(accessToken)
		if err == nil {
			return user, nil
		}
	}
	if err != nil {
		slog.Debug(fmt.Sprintf("authenticator: %s", err.Error()))
		slog.Debug("Falling back to KBase authentication.")

		// maybe it's a KBase dev token, so check with the KBase auth server
		authServer, err := auth.NewKBaseAuthServer(accessToken)
		if err != nil {
			return auth.Client{}, huma.Error401Unauthorized(err.Error())
		}
		client, err = authServer.Client()
		if err != nil {
			return client, huma.Error401Unauthorized(err.Error())
		}
	}

	// the client needs at least one associated ORCID
	if client.Orcid == "" {
		return client, huma.Error403Forbidden("The DTS client has no associated ORCID!")
	}
	return client, nil
}

type ServiceInfoOutput struct {
	Body ServiceInfoResponse `doc:"information about the service itself"`
}

// handler method for root (no authorization needed for this one)
func (service *prototype) getRoot(ctx context.Context,
	input *struct{}) (*ServiceInfoOutput, error) {

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

type SearchParametersOutput struct {
	Body map[string]any `doc:"a JSON object whose fields are search parameters and whose values indicate their type"`
}

// We map database-specific search parameters to JSON according to the following
// rules:
// * fields are search parameter names
// * values can be
//   - zeroed primitives, indicating user-supplied parameters
//   - slices, indicating parameters selected from a list (e.g. a pulldown)
//
// We annotate each parameter with its type, to facilitate the client's
// handling of the JSON object. This treatment may seem delicate and full of
// boilerplate, but it's an easy and straightforward way of performing a
// mapping from a minimal data structure to a self-describing representation.
func mapSearchParamsToJson(params map[string]any) map[string]any {
	obj := make(map[string]any) // map that becomes the JSON response

	for field, value := range params {
		switch val := value.(type) {
		case int:
			entry := struct {
				Type  string `json:"type"`
				Value int    `json:"value"`
			}{
				Type:  "number",
				Value: val,
			}
			obj[field] = entry
		case float64:
			entry := struct {
				Type  string  `json:"type"`
				Value float64 `json:"value"`
			}{
				Type:  "number",
				Value: val,
			}
			obj[field] = entry
		case bool:
			entry := struct {
				Type  string `json:"type"`
				Value bool   `json:"value"`
			}{
				Type:  "boolean",
				Value: val,
			}
			obj[field] = entry
		case string:
			entry := struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			}{
				Type:  "string",
				Value: val,
			}
			obj[field] = entry
		case []string:
			entry := struct {
				Type  string   `json:"type"`
				Value []string `json:"value"`
			}{
				Type:  "array(string)",
				Value: val,
			}
			obj[field] = entry
		case []int:
			entry := struct {
				Type  string `json:"type"`
				Value []int  `json:"value"`
			}{
				Type:  "array(number)",
				Value: val,
			}
			obj[field] = entry
		}
	}
	return obj
}

// method for querying a single database for its specific search parameters
func (service *prototype) getDatabaseSearchParameters(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization" doc:"Authorization header with encoded access token"`
		Database      string `path:"db" example:"jdp" doc:"the abbreviated name of a database"`
	}) (*SearchParametersOutput, error) {

	_, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// is the database valid?
	_, ok := config.Databases[input.Database]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", input.Database)
	}
	db, err := databases.NewDatabase(input.Database)
	if err != nil {
		return nil, err
	}

	// Fish the database-specific search parameters out of the database
	// and encode them in a JSON object.
	params := db.SpecificSearchParameters() // parameters to pack into response
	return &SearchParametersOutput{
		Body: mapSearchParamsToJson(params),
	}, nil
}

type SearchResultsOutput struct {
	Body SearchResultsResponse `doc:"Search results containing matching files that match the given query"`
}

type SearchDatabaseInputWithoutHeader struct {
	Database string `json:"database" query:"database" example:"jdp" doc:"The ID of the database to search"`
	Orcid    string `json:"orcid" query:"orcid" example:"1234-5678-9101=112X" doc:"The ORCID of the user searching for files"`
	Query    string `json:"query" query:"query" example:"prochlorococcus" doc:"A query used to search the database for matching files"`
	Status   string `json:"status" query:"status" example:"\"staged\"" doc:"(Optional) The staged or unstaged status of the desired files"`
	Offset   int    `json:"offset" query:"offset" example:"100" doc:"Search results begin at the given offset"`
	Limit    int    `json:"limit" query:"limit" example:"50" doc:"Limits the number of search results returned"`
}

type SearchDatabaseInput struct {
	Authorization string `header:"authorization" doc:"Authorization header with encoded access token"`
	SearchDatabaseInputWithoutHeader
}

// routes database-related errors through Huma
func databaseError(err error) error {
	if err != nil {
		slog.Error(err.Error())
		switch err.(type) {
		case *databases.InvalidSearchParameter:
			return huma.Error400BadRequest(err.Error(), err)
		case *databases.UnavailableError:
			return huma.Error503ServiceUnavailable(err.Error(), err)
		case *databases.PermissionDeniedError, *databases.UnauthorizedError:
			return huma.Error401Unauthorized(err.Error(), err)
		case *databases.NotFoundError, *databases.ResourcesNotFoundError, *databases.ResourceEndpointNotFoundError:
			return huma.Error404NotFound(err.Error(), err)
		default:
			return huma.Error500InternalServerError(err.Error(), err)
		}
	}
	return nil
}

// implements database search for both GET and POST requests
func searchDatabase(_ context.Context,
	input *SearchDatabaseInput,
	specific map[string]json.RawMessage) (*SearchResultsOutput, error) {

	userOrClient, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// is the database valid?
	_, ok := config.Databases[input.Database]
	if !ok {
		return nil, databaseError(&databases.NotFoundError{Database: input.Database})
	}

	// check the requested file status
	var fileStatus databases.SearchFileStatus
	switch input.Status {
	case "":
		fileStatus = databases.SearchFileStatusAny
	case "staged", "STAGED":
		fileStatus = databases.SearchFileStatusStaged
	case "unstaged", "UNSTAGED":
		fileStatus = databases.SearchFileStatusUnstaged
	default:
		return nil, fmt.Errorf("Invalid status parameter: %s", input.Status)
	}

	// unmarshal database-specific parameters
	dbSpecific := make(map[string]any)
	for key, jsonValue := range specific {
		var value any
		err := json.Unmarshal(jsonValue, &value)
		if err != nil {
			return nil, &databases.InvalidSearchParameter{
				Database: "JDP",
				Message:  "Invalid JDP sort order given (must be string)",
			}
		}
		switch v := value.(type) {
		case string:
			dbSpecific[key] = v
		case float64: // JSON number -- can be float or integer
			if v-math.Floor(v) > 0.0 {
				dbSpecific[key] = v
			} else {
				dbSpecific[key] = int(v)
			}
		case bool:
			dbSpecific[key] = v
		default:
			return nil, fmt.Errorf("Invalid database-specific parameter: %s", key)
		}
	}

	// FIXME: for now, if a user ORCID is not specified, use the user/client's ORCID
	orcid := input.Orcid
	if orcid == "" {
		switch u := userOrClient.(type) {
		case auth.User:
			orcid = u.Orcid
		case auth.Client:
			orcid = u.Orcid
		}
	}

	slog.Info(fmt.Sprintf("Searching database %s for files...", input.Database))
	db, err := databases.NewDatabase(input.Database)
	if err != nil {
		return nil, databaseError(err)
	}

	results, err := db.Search(orcid, databases.SearchParameters{
		Query:  input.Query,
		Status: fileStatus,
		Pagination: databases.SearchPaginationParameters{
			Offset: input.Offset,
			MaxNum: input.Limit,
		},
		Specific: dbSpecific,
	})
	if err != nil {
		return nil, databaseError(err)
	}
	// validate the descriptors and send them along
	for _, descriptor := range results.Descriptors {
		err = validator.Validate(descriptor, "data-resource", validator.MustInMemoryRegistry())
		if err != nil {
			slog.Error(err.Error())
			return nil, err
		}
	}
	return &SearchResultsOutput{
		Body: SearchResultsResponse{
			Database:    input.Database,
			Query:       input.Query,
			Descriptors: results.Descriptors,
		},
	}, nil
}

// handle search queries for files of interest (GET, no DB-specific parameters)
func (service *prototype) searchDatabase(ctx context.Context,
	input *SearchDatabaseInput) (*SearchResultsOutput, error) {
	return searchDatabase(ctx, input, nil)
}

// handle search queries for files of interest (POST, DB-specific parameters)
// NOTE: all parameters are extracted from the body of the POST; no URL
// NOTE: parameters are accepted
func (service *prototype) searchDatabaseWithSpecificParams(ctx context.Context,
	input *struct {
		Authorization string          `header:"authorization" doc:"Authorization header with encoded access token"`
		Body          json.RawMessage `doc:"Contains all search parameters (including any database-specific parameters) given as key-value pairs in a JSON object" contentType:"application/json"`
		ContentType   string          `header:"Content-Type" doc:"Content-Type header (must be application/json)"`
	}) (*SearchResultsOutput, error) {
	var body struct {
		SearchDatabaseInputWithoutHeader
		Specific map[string]json.RawMessage `json:"specific" doc:"database-specific search parameters in a JSON object"`
	}
	err := json.Unmarshal(input.Body, &body)
	if err != nil {
		return nil, err
	}
	searchInput := SearchDatabaseInput{
		Authorization: input.Authorization,
		SearchDatabaseInputWithoutHeader: SearchDatabaseInputWithoutHeader{
			Database: body.Database,
			Orcid:    body.Orcid,
			Query:    body.Query,
			Status:   body.Status,
			Offset:   body.Offset,
			Limit:    body.Limit,
		},
	}
	return searchDatabase(ctx, &searchInput, body.Specific)
}

type FileMetadataOutput struct {
	Body FileMetadataResponse `doc:"Metadata for files with the given IDs"`
}

// fetches file metadata given a list of file identifiers
func (service *prototype) fetchFileMetadata(ctx context.Context,
	input *struct {
		Authorization string `header:"authorization" doc:"Authorization header with encoded access token"`
		Database      string `json:"database" query:"database" example:"jdp" doc:"The ID of the database for which file metadata is fetched"`
		Orcid         string `json:"orcid" query:"orcid" example:"1234-5678-9101-112X" doc:"The ORCID of the user requesting metadata"`
		Ids           string `json:"ids" query:"ids" example:"JDP:6101cc0f2b1f2eeea564c978" doc:"A comma-separated list of file IDs"`
		Offset        int    `json:"offset" query:"offset" example:"100" doc:"Metadata records begin at the given offset"`
		Limit         int    `json:"limit" query:"limit" example:"50" doc:"Limits the number of metadata records returned"`
	}) (*FileMetadataOutput, error) {

	userOrClient, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// is the database valid?
	_, ok := config.Databases[input.Database]
	if !ok {
		return nil, fmt.Errorf("Database %s not found", input.Database)
	}

	// have we been given any IDs?
	if strings.TrimSpace(input.Ids) == "" {
		return nil, huma.Error400BadRequest("No file IDs were provided!")
	}
	ids := strings.Split(input.Ids, ",")

	slog.Info(fmt.Sprintf("Fetching file metadata for %d files in database %s...",
		len(ids), input.Database))
	db, err := databases.NewDatabase(input.Database)
	if err != nil {
		return nil, err
	}

	// FIXME: for now, if a user ORCID is not specified, use the client's ORCID
	orcid := input.Orcid
	if orcid == "" {
		switch u := userOrClient.(type) {
		case auth.User:
			orcid = u.Orcid
		case auth.Client:
			orcid = u.Orcid
		}
	}

	descriptors, err := db.Descriptors(orcid, ids)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// validate the descriptors and send them along
	for _, descriptor := range descriptors {
		err = validator.Validate(descriptor, "data-resource", validator.MustInMemoryRegistry())
		if err != nil {
			slog.Error(err.Error())
			return nil, err
		}
	}
	return &FileMetadataOutput{
		Body: FileMetadataResponse{
			Database:    input.Database,
			Descriptors: descriptors,
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

	userOrClient, err := authorize(input.Authorization)
	if err != nil {
		return nil, err
	}

	// fetch information about the requesting user
	user, isUser := userOrClient.(auth.User)
	if !isUser {
		client := userOrClient.(auth.Client)
		user = auth.User{
			Name:         client.Name,
			Email:        client.Email,
			Orcid:        client.Orcid,
			Organization: client.Organization,
		}
	}

	if input.Body.Orcid == "" {
		return nil, huma.Error401Unauthorized("No user ORCID was provided")
	}
	if !isUser {
		// Override the client's ORCID
		user.Orcid = input.Body.Orcid
	} else {
		// a user is requesting a transfer on behalf of another user
		// FIXME: for now, we only extract the ORCID and keep everything else the same, but at length
		// FIXME: we should fill in the other fields with the ORCID public record
		user.Orcid = input.Body.Orcid
	}

	// Check whether the destination is a "custom transfer", available only to Special People.
	if strings.Contains(input.Body.Destination, ":") {
		_, err := endpoints.ParseCustomSpec(input.Body.Destination)
		if err != nil {
			return nil, huma.Error400BadRequest(fmt.Sprintf("Invalid destination: %s", input.Body.Destination))
		}
		if !user.IsSuper { // not allowed to do custom transfers
			return nil, huma.Error400BadRequest(fmt.Sprintf("Invalid destination: %s", input.Body.Destination))
		}
	}

	taskId, err := tasks.Create(tasks.Specification{
		User:         user,
		Source:       input.Body.Source,
		Destination:  input.Body.Destination,
		FileIds:      input.Body.FileIds,
		Description:  input.Body.Description,
		Instructions: input.Body.Instructions,
	})
	if err != nil {
		slog.Error(err.Error())
		switch err.(type) {
		case *tasks.NoFilesRequestedError:
			return nil, huma.Error400BadRequest(err.Error())
		case *databases.NotFoundError:
			return nil, huma.Error404NotFound(err.Error())
		default:
			return nil, huma.Error500InternalServerError(err.Error())
		}
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
