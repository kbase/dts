package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	//	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/net/netutil"

	"dts/auth"
	"dts/config"
	"dts/core"
	"dts/databases"
	// "dts/endpoints"
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
	// HTTP server.
	Server *http.Server

	// manager of transfer tasks
	Tasks *core.TaskManager
}

// validates a header, ensuring that it uses the HTTP Bearer authentication
// method with a valid token, and returns
// 1. the decoded access token
// 2. the ORCID ID associated with the access token
// 3. any error encountered in the process
func getAuthInfo(header http.Header) (string, string, error) {

	// make sure we're using the Bearer method and that we can get an access token
	authData := header.Get("Authorization")
	if !strings.Contains(authData, "Bearer") {
		return "", "", fmt.Errorf("Invalid authorization header")
	}
	b64Token := authData[len("Bearer "):]
	accessTokenBytes, err := base64.StdEncoding.DecodeString(b64Token)
	if err != nil {
		return "", "", err
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
	return accessToken, orcid, err
}

// this type encodes a JSON object for responding to root queries
type RootResponse struct {
	Name          string `json:"name"`
	Version       string `json:"version"`
	Uptime        int    `json:"uptime"`
	Documentation string `json:"documentation,omitempty"`
}

// handler method for root
func (service *prototype) getRoot(w http.ResponseWriter,
	r *http.Request) {

	_, _, err := getAuthInfo(r.Header)
	if err != nil {
		log.Print(err.Error())
		writeError(w, err.Error(), 401)
		return
	}

	log.Printf("Querying root endpoint...")
	data := RootResponse{
		Name:    service.Name,
		Version: service.Version,
		Uptime:  int(service.uptime())}
	if HaveDocEndpoints {
		data.Documentation = "/docs"
	}
	jsonData, _ := json.Marshal(data)
	writeJson(w, jsonData)
}

// type holding database metadata
type dbMetadata struct {
	Id           string `json:"id"`
	Name         string `json:"name"`
	Organization string `json:"organization"`
	URL          string `json:"url"`
}

// handler method for querying all databases
func (service *prototype) getDatabases(w http.ResponseWriter,
	r *http.Request) {

	_, _, err := getAuthInfo(r.Header)
	if err != nil {
		log.Print(err.Error())
		writeError(w, err.Error(), 401)
		return
	}

	log.Printf("Querying organizational databases...")
	dbs := make([]dbMetadata, 0)
	for dbName, db := range config.Databases {
		dbs = append(dbs, dbMetadata{
			Id:           dbName,
			Name:         db.Name,
			Organization: db.Organization,
			URL:          db.URL,
		})
	}
	// FIXME: sort by name
	jsonData, _ := json.Marshal(dbs)
	writeJson(w, jsonData)
}

// handler method for querying a single database for its metadata
func (service *prototype) getDatabase(w http.ResponseWriter,
	r *http.Request) {

	_, _, err := getAuthInfo(r.Header)
	if err != nil {
		log.Print(err.Error())
		writeError(w, err.Error(), 401)
		return
	}

	vars := mux.Vars(r)
	dbName := vars["db"]

	log.Printf("Querying database %s...", dbName)
	db, ok := config.Databases[dbName]
	if !ok {
		errStr := fmt.Sprintf("Database %s not found", dbName)
		log.Print(errStr)
		writeError(w, errStr, 404)
	} else {
		data, _ := json.Marshal(dbMetadata{
			Id:           dbName,
			Name:         db.Name,
			Organization: db.Organization,
			URL:          db.URL,
		})
		writeJson(w, data)
	}
}

// this helper translates an array of engines.SearchResults to a JSON object
// containing search results for the query (including the database name)
func jsonFromSearchResults(dbName string,
	query string, results core.SearchResults) ([]byte, error) {

	data := ElasticSearchResponse{
		Database:  dbName,
		Query:     query,
		Resources: results.Resources,
	}

	return json.Marshal(data)
}

// helper for extracting search parameters
func extractSearchParams(r *http.Request) (core.SearchParameters, error) {
	var params core.SearchParameters
	params.Query = r.FormValue("query")
	if params.Query == "" {
		return params, fmt.Errorf("Query string not given!")
	}
	v := r.URL.Query()
	offsetVal := v.Get("offset")
	if offsetVal != "" {
		var err error
		params.Pagination.Offset, err = strconv.Atoi(offsetVal)
		if err != nil {
			return params, fmt.Errorf("Error: Invalid results offset: %s", offsetVal)
		} else if params.Pagination.Offset < 0 {
			return params, fmt.Errorf("Error: Invalid results offset: %d", params.Pagination.Offset)
		}
	}
	NVal := v.Get("limit")
	if NVal != "" {
		var err error
		params.Pagination.MaxNum, err = strconv.Atoi(NVal)
		if err != nil {
			return params, fmt.Errorf("Invalid results limit: %s", NVal)
		} else if params.Pagination.MaxNum <= 0 {
			return params, fmt.Errorf("Invalid results limit: %d", params.Pagination.MaxNum)
		}
	}
	return params, nil
}

// handle ElasticSearch queries
func (service *prototype) searchDatabase(w http.ResponseWriter,
	r *http.Request) {

	_, orcid, err := getAuthInfo(r.Header)
	if err != nil {
		log.Print(err.Error())
		writeError(w, err.Error(), 401)
		return
	}

	// fetch search parameters
	dbName := r.FormValue("database")

	// is the database valid?
	_, ok := config.Databases[dbName]
	if !ok {
		errStr := fmt.Sprintf("Database %s not found", dbName)
		log.Print(errStr)
		writeError(w, errStr, 404)
		return
	}

	// are we asked to return a subset of our results?
	params, err := extractSearchParams(r)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	}

	log.Printf("Searching database %s for files...", dbName)
	db, err := databases.NewDatabase(orcid, dbName)
	if err != nil {
		writeError(w, err.Error(), 404)
		return
	}
	results, err := db.Search(params)
	if err != nil {
		writeError(w, err.Error(), 400)
		return
	} else {
		// return our results to the caller
		jsonData, _ := jsonFromSearchResults(dbName, params.Query, results)
		writeJson(w, jsonData)
	}
}

func getTransferRequest(r *http.Request) (TransferRequest, error) {
	var req TransferRequest
	var err error

	_, orcid, err := getAuthInfo(r.Header)
	if err == nil {
		// is this a JSON request?
		if r.Header.Get("Content-Type") != "application/json" {
			err = fmt.Errorf("Request content type must be \"application/json\".")
		} else {
			// read the request body
			var rBody []byte
			rBody, err = io.ReadAll(r.Body)
			if err == nil {
				err = json.Unmarshal(rBody, &req)
				if err == nil {
					// validate source and destination databases
					if _, ok := config.Databases[req.Source]; !ok {
						err = fmt.Errorf("Unknown source database: %s", req.Source)
					} else if _, ok := config.Databases[req.Destination]; !ok {
						err = fmt.Errorf("Unknown destination database: %s", req.Destination)
					}

					// make sure there's at least one file in the request
					if len(req.FileIds) == 0 {
						err = fmt.Errorf("No file IDs specified for transfer!")
					}

					req.Orcid = orcid
				}
			}
		}
	}

	return req, err
}

// handler method for initiating a file transfer operation
func (service *prototype) createTransfer(w http.ResponseWriter,
	r *http.Request) {

	// extract and validate request data
	request, err := getTransferRequest(r)
	if err != nil {
		writeError(w, err.Error(), 401)
		return
	}

	// fetch source and destination databases
	var source, destination core.Database
	source, err = databases.NewDatabase(request.Orcid, request.Source)
	if err == nil {
		destination, err = databases.NewDatabase(request.Orcid, request.Destination)
	}
	if err != nil {
		writeError(w, err.Error(), 404)
		return
	}

	taskId, err := service.Tasks.Add(source, destination, request.FileIds)
	if err == nil {
		jsonData, _ := json.Marshal(TransferResponse{Id: taskId})
		writeJson(w, jsonData)
	} else {
		writeError(w, err.Error(), 500)
	}
}

// convert a transfer status code to a nice human-friendly string
func statusAsString(statusCode core.TransferStatusCode) string {
	switch statusCode {
	case core.TransferStatusStaging:
		return "staging"
	case core.TransferStatusActive:
		return "active"
	case core.TransferStatusInactive:
		return "inactive"
	case core.TransferStatusSucceeded:
		return "succeeded"
	case core.TransferStatusFailed:
		return "failed"
	}
	return "unknown"
}

// handler method for getting the status of a transfer
func (service *prototype) getTransferStatus(w http.ResponseWriter,
	r *http.Request) {

	_, _, err := getAuthInfo(r.Header)
	if err != nil {
		log.Print(err.Error())
		writeError(w, err.Error(), 401)
		return
	}

	// Extract the transfer ID from the request.
	vars := mux.Vars(r)
	xferId, err := uuid.Parse(vars["id"])
	if err != nil {
		errStr := fmt.Sprintf("Invalid transfer ID: %s", xferId)
		writeError(w, errStr, 400)
		return
	}

	// fetch the status for the job using the appropriate task data
	status, err := service.Tasks.Status(xferId)
	if err != nil {
		errCode := 500
		if strings.Contains(err.Error(), "not found") {
			errCode = 404
		}
		writeError(w, err.Error(), errCode)
		return
	}
	resp := TransferStatusResponse{
		Id:                  xferId.String(),
		Status:              statusAsString(status.Code),
		NumFiles:            status.NumFiles,
		NumFilesTransferred: status.NumFilesTransferred,
	}
	jsonData, _ := json.Marshal(resp)
	writeJson(w, jsonData)
}

// returns the uptime for the service in seconds
func (service *prototype) uptime() float64 {
	return time.Since(service.StartTime).Seconds()
}

// constructs a prototype file transfer service given our configuration
func NewDTSPrototype() (TransferService, error) {
	service := new(prototype)
	service.Name = "DTS prototype"
	service.Version = core.Version
	service.Port = -1

	// set up routing
	r := mux.NewRouter()
	r.HandleFunc("/", service.getRoot).Methods("GET")

	// serve documentation endpoints
	AddDocEndpoints(r)

	// API calls are routed through /api
	api := r.PathPrefix("/api").Subrouter()
	api.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	// v1
	api_v1 := api.PathPrefix("/v1").Subrouter()
	api_v1.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	api_v1.HandleFunc("/databases", service.getDatabases).Methods("GET")
	api_v1.HandleFunc("/databases/{db}", service.getDatabase).Methods("GET")
	api_v1.HandleFunc("/files", service.searchDatabase).Methods("GET")
	api_v1.HandleFunc("/transfers", service.createTransfer).Methods("POST")
	api_v1.HandleFunc("/transfers/{id}", service.getTransferStatus).Methods("GET")
	// TODO: add DELETE mechanism for /transfers/{id}
	service.Router = r

	return service, nil
}

// starts the prototype data transfer service
func (service *prototype) Start(port int) error {
	log.Printf("Starting %s service on port %d...", service.Name, port)
	log.Printf("(Accepting up to %d connections)", config.Service.MaxConnections)

	service.StartTime = time.Now()

	// create a listener that limits the number of incoming connections
	service.Port = port
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer listener.Close()
	listener = netutil.LimitListener(listener, config.Service.MaxConnections)

	service.Tasks, err = core.NewTaskManager(time.Duration(config.Service.PollInterval) * time.Millisecond)
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
	} else {
		return nil
	}
}

// gracefully shuts down the service without interrupting active connections
func (service *prototype) Shutdown(ctx context.Context) error {
	err := service.Server.Shutdown(ctx)
	return err
}

// Closes down the service, freeing all resources.
func (service *prototype) Close() {
	service.Server.Close()
}
