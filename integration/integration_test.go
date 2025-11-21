// Copyright (c) 2025 The KBase Project and its Contributors
// Copyright (c) 2025 Cohere Consulting, LLC
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

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kbase/dts/config"
	"github.com/kbase/dts/services"
	"github.com/stretchr/testify/assert"
)

const (
	testServiceURL = "http://localhost:8080"
	testToken      = "d72936dcb7db05c6d890df30ea173a38"
)

var databases = map[string]struct {
	Name         string
	Organization string
	URL          string
}{
	"db-foo": {
		Name:         "The Foo Database",
		Organization: "Foo International",
		URL:          "",
	},
	"db-bar": {
		Name:         "The Bar Database",
		Organization: "Bar Enterprises",
		URL:          "",
	},
	"db-baz": {
		Name:         "The Baz Database",
		Organization: "Baz LLC",
		URL:          "",
	},
}

func addAuthHeader(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+base64.StdEncoding.EncodeToString([]byte(testToken)))
}

func TestGetDatabases(t *testing.T) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/databases", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	addAuthHeader(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to perform request: %v", err)
	}
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var dbs []struct {
		Id           string `json:"id"`
		Name         string `json:"name"`
		Organization string `json:"organization"`
		URL          string `json:"url"`
	}
	err = json.NewDecoder(resp.Body).Decode(&dbs)
	if err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	assert.Equal(t, len(databases), len(dbs), "unexpected number of databases returned")
	for _, db := range dbs {
		expected, ok := databases[db.Id]
		if !ok {
			t.Errorf("unexpected database ID: %s", db.Id)
			continue
		}
		assert.Equal(t, db.Id, db.Id, "unexpected database ID for ID %s", db.Id)
		assert.Equal(t, expected.Name, db.Name, "unexpected database name for ID %s", db.Id)
		assert.Equal(t, expected.Organization, db.Organization, "unexpected organization for ID %s", db.Id)
		// FIXME: The URL field is always empty in the current implementation, even if a URL is set in the config.
		assert.Equal(t, expected.URL, db.URL, "unexpected URL for ID %s", db.Id)
	}
}

func TestGetDatabaseByID(t *testing.T) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	for dbID, expected := range databases {
		req, err := http.NewRequest("GET", testServiceURL+"/api/v1/databases/"+dbID, nil)
		if err != nil {
			t.Fatalf("failed to create request for database %s: %v", dbID, err)
		}
		addAuthHeader(req)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("failed to perform request for database %s: %v", dbID, err)
		}
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code for database %s", dbID)

		var db struct {
			Id           string `json:"id"`
			Name         string `json:"name"`
			Organization string `json:"organization"`
			URL          string `json:"url"`
		}
		err = json.NewDecoder(resp.Body).Decode(&db)
		if err != nil {
			t.Fatalf("failed to decode response body for database %s: %v", dbID, err)
		}
		assert.Equal(t, dbID, db.Id, "unexpected database ID for database %s", dbID)
		assert.Equal(t, expected.Name, db.Name, "unexpected database name for database %s", dbID)
		assert.Equal(t, expected.Organization, db.Organization, "unexpected organization for database %s", dbID)
		// FIXME: The URL field is always empty in the current implementation, even if a URL is set in the config.
		assert.Equal(t, expected.URL, db.URL, "unexpected URL for database %s", dbID)
	}
}

func TestDatabaseSearchParameters(t *testing.T) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/databases/db-foo/search-parameters", nil)
	if err != nil {
		t.Fatalf("failed to create request for database search parameters: %v", err)
	}
	addAuthHeader(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to perform request for database search parameters: %v", err)
	}
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code for database search parameters")

	// S3 databases currently do not support search parameters, so expect an empty list
	var params map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&params)
	if err != nil {
		t.Fatalf("failed to decode response body for database search parameters: %v", err)
	}
	assert.Equal(t, 0, len(params), "expected no search parameters for S3 database")
}

func TestDatabaseFilesGet(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/files?database=db-foo", nil)
	assert.Nil(err, "failed to create request for database files")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for database files")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for database files")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for database files")

	var results services.SearchResultsResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err, "failed to unmarshal response body for database files")
	assert.Equal("db-foo", results.Database, "unexpected database ID in search results")
	assert.Equal("", results.Query, "unexpected query in search results")
	assert.Equal(6, len(results.Descriptors), "unexpected number of file descriptors in search results")
	expectedFileNames := map[string]bool{
		"file1.txt":              true,
		"file2.txt":              true,
		"dir1/file3.txt":         true,
		"dir1/file4.txt":         true,
		"dir2/file5.txt":         true,
		"dir2/subdir1/file6.txt": true,
	}
	for _, desc := range results.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in search results: %s", path)
	}
}

func TestDatabaseFilesGetWithPrefix(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/files?database=db-foo&query=dir1/", nil)
	assert.Nil(err, "failed to create request for database files with prefix")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for database files with prefix")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for database files with prefix")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for database files with prefix")

	var results services.SearchResultsResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err, "failed to unmarshal response body for database files with prefix")
	assert.Equal("db-foo", results.Database, "unexpected database ID in search results")
	assert.Equal("dir1/", results.Query, "unexpected query in search results")
	assert.Equal(2, len(results.Descriptors), "unexpected number of file descriptors in search results")
	expectedFileNames := map[string]bool{
		"dir1/file3.txt": true,
		"dir1/file4.txt": true,
	}
	for _, desc := range results.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in search results: %s", path)
	}
}

func TestDatabaseFilesPost(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	reqBody, err := json.Marshal(map[string]any{
		"database": "db-foo",
		"query":    "",
	})
	assert.Nil(err, "failed to marshal request body for database files")
	req, err := http.NewRequest("POST", testServiceURL+"/api/v1/files", bytes.NewBuffer(reqBody))
	assert.Nil(err, "failed to create request for database files")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for database files")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for database files")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for database files")

	var results services.SearchResultsResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err, "failed to unmarshal response body for database files")
	assert.Equal("db-foo", results.Database, "unexpected database ID in search results")
	assert.Equal("", results.Query, "unexpected query in search results")
	assert.Equal(6, len(results.Descriptors), "unexpected number of file descriptors in search results")
	expectedFileNames := map[string]bool{
		"file1.txt":              true,
		"file2.txt":              true,
		"dir1/file3.txt":         true,
		"dir1/file4.txt":         true,
		"dir2/file5.txt":         true,
		"dir2/subdir1/file6.txt": true,
	}
	for _, desc := range results.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in search results: %s", path)
	}
}

func TestDatabaseFilesPostWithPrefix(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	reqBody, err := json.Marshal(map[string]any{
		"database": "db-foo",
		"query":    "dir2/",
	})
	assert.Nil(err, "failed to marshal request body for database files with prefix")
	req, err := http.NewRequest("POST", testServiceURL+"/api/v1/files", bytes.NewBuffer(reqBody))
	assert.Nil(err, "failed to create request for database files with prefix")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for database files with prefix")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for database files with prefix")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for database files with prefix")

	var results services.SearchResultsResponse
	err = json.Unmarshal(respBody, &results)
	assert.Nil(err, "failed to unmarshal response body for database files with prefix")
	assert.Equal("db-foo", results.Database, "unexpected database ID in search results")
	assert.Equal("dir2/", results.Query, "unexpected query in search results")
	assert.Equal(2, len(results.Descriptors), "unexpected number of file descriptors in search results")
	expectedFileNames := map[string]bool{
		"dir2/file5.txt":         true,
		"dir2/subdir1/file6.txt": true,
	}
	for _, desc := range results.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in search results: %s", path)
	}
}

func TestDatabaseFetchMetadata(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/files/by-id?database=db-foo&ids=file1.txt,dir2/file5.txt", nil)
	assert.Nil(err, "failed to create request for database fetch metadata")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for database fetch metadata")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for database fetch metadata")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for database fetch metadata")

	var metadata services.SearchResultsResponse
	err = json.Unmarshal(respBody, &metadata)
	assert.Nil(err, "failed to unmarshal response body for database fetch metadata")
	assert.Equal("db-foo", metadata.Database, "unexpected database ID in metadata response")
	assert.NotNil(metadata.Descriptors, "missing file descriptor in metadata response")
	assert.Equal(2, len(metadata.Descriptors), "unexpected number of file descriptors in metadata response")
	expectedFileNames := map[string]bool{
		"file1.txt":      true,
		"dir2/file5.txt": true,
	}
	for _, desc := range metadata.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in metadata response: %s", path)
	}
}

func getTransferStatus(t *testing.T, client *http.Client, transferId uuid.UUID) services.TransferStatusResponse {
	req, err := http.NewRequest("GET", testServiceURL+"/api/v1/transfers/"+transferId.String(), nil)
	assert.Nil(t, err, "failed to create request for transfer status")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(t, err, "failed to perform request for transfer status")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code for transfer status")

	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(t, err, "failed to read response body for transfer status")

	var statusResponse services.TransferStatusResponse
	err = json.Unmarshal(respBody, &statusResponse)
	assert.Nil(t, err, "failed to unmarshal response body for transfer status")

	return statusResponse
}

func TestTransfer(t *testing.T) {
	assert := assert.New(t)
	client := &http.Client{
		Timeout: 1000 * time.Second,
	}
	transfer, err := json.Marshal(services.TransferRequest{
		Orcid:       "0000-0000-1234-0000",
		Source:      "db-foo",
		Destination: "db-bar",
		FileIds:     []string{"file1.txt", "file2.txt"},
	})
	assert.Nil(err, "failed to marshal transfer request")
	req, err := http.NewRequest("POST", testServiceURL+"/api/v1/transfers", bytes.NewBuffer(transfer))
	assert.Nil(err, "failed to create request for transfer")
	addAuthHeader(req)

	resp, err := client.Do(req)
	assert.Nil(err, "failed to perform request for transfer")
	defer resp.Body.Close()

	assert.Equal(http.StatusCreated, resp.StatusCode, "unexpected status code for transfer")
	respBody, err := io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for transfer")

	var transferResponse services.TransferResponse
	err = json.Unmarshal(respBody, &transferResponse)
	assert.Nil(err, "failed to unmarshal response body for transfer")
	transferId := transferResponse.Id
    transferIdString := transferId.String()
	slog.Info("Created transfer", "id", transferIdString)

	status := getTransferStatus(t, client, transferId)
	assert.Equal(transferIdString, status.Id, "unexpected transfer ID in status response")
	assert.Equal(2, status.NumFiles, "unexpected number of files in status response")

	// wait for transfer to complete
	time.Sleep(5 * time.Second)

	status = getTransferStatus(t, client, transferId)
	assert.Equal(transferId.String(), status.Id, "unexpected transfer ID in status response after wait")
	assert.Equal(2, status.NumFiles, "unexpected number of files in status response after wait")
	assert.Equal(2, status.NumFilesTransferred, "unexpected number of files transferred in status response after wait")
	assert.Equal("finalizing", status.Status, "unexpected transfer status after wait")

	// make sure the file is now in the destination database
	file1path := "local-user/dts-" + transferIdString + "/file1.txt"
	file2path := "local-user/dts-" + transferIdString + "/file2.txt"
	req, err = http.NewRequest("GET", testServiceURL+"/api/v1/files/by-id?database=db-bar&ids="+file1path+","+file2path, nil)
	assert.Nil(err, "failed to create request for destination database fetch metadata")
	addAuthHeader(req)

	resp, err = client.Do(req)
	assert.Nil(err, "failed to perform request for destination database fetch metadata")
	defer resp.Body.Close()

	assert.Equal(http.StatusOK, resp.StatusCode, "unexpected status code for destination database fetch metadata")
	respBody, err = io.ReadAll(resp.Body)
	assert.Nil(err, "failed to read response body for destination database fetch metadata")

	var metadata services.SearchResultsResponse
	err = json.Unmarshal(respBody, &metadata)
	assert.Nil(err, "failed to unmarshal response body for destination database fetch metadata")
	assert.Equal("db-bar", metadata.Database, "unexpected database ID in destination metadata response")
	assert.NotNil(metadata.Descriptors, "missing file descriptor in destination metadata response")
	assert.Equal(2, len(metadata.Descriptors), "unexpected number of file descriptors in destination metadata response")
	expectedFileNames := map[string]bool{
		file1path: true,
		file2path: true,
	}
	for _, desc := range metadata.Descriptors {
		path, ok := desc["path"].(string)
		assert.True(ok, "file descriptor missing 'path' field or it is not a string")
		_, ok = expectedFileNames[path]
		assert.True(ok, "unexpected file path in destination metadata response: %s", path)
	}
}

func setup() services.TransferService {
	// reset the S3 test buckets
	ResetMinioTestBuckets()

	// create a manifests directory if it doesn't exist
	if _, err := os.Stat("manifests"); os.IsNotExist(err) {
		if err := os.Mkdir("manifests", 0755); err != nil {
			panic("unable to create manifests directory: " + err.Error())
		}
	}
	// create a local-fs directory if it doesn't exist
	if _, err := os.Stat("local-fs"); os.IsNotExist(err) {
		if err := os.Mkdir("local-fs", 0755); err != nil {
			panic("unable to create local-fs directory: " + err.Error())
		}
	}
	// remove any existing .gob or .db files in the server-data directory
	files, err := os.ReadDir("fixtures/server-data")
	if err != nil {
		panic("unable to read server-data directory: " + err.Error())
	}
	for _, file := range files {
		if !file.IsDir() {
			name := file.Name()
			if len(name) > 4 && (name[len(name)-4:] == ".gob" || name[len(name)-3:] == ".db") {
				err := os.Remove("fixtures/server-data/" + name)
				if err != nil {
					panic("unable to remove file "+name+" from server-data directory: " + err.Error())
				}
			}
		}
	}

	// enable logging
	logLevel := new(slog.LevelVar)
	logLevel.Set(slog.LevelDebug)
	handler := slog.NewJSONHandler(os.Stdout,
		&slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(handler))

	// read the config file
	file, err := os.Open("fixtures/test-config.yaml")
	if err != nil {
		panic("unable to open test config file: " + err.Error())
	}
	defer file.Close()
	b, err := io.ReadAll(file)
	if err != nil {
		panic("unable to read test config file: " + err.Error())
	}
	err = config.Init(b)
	if err != nil {
		panic("unable to parse test config file: " + err.Error())
	}
	conf, err := config.NewConfig(b)
	if err != nil {
		panic("unable to create config from test config file: " + err.Error())
	}

	service, err := services.NewDTSPrototype()
	if err != nil {
		panic("unable to create transfer service: " + err.Error())
	}

	go func(conf config.Config) {
		err := service.Start(conf)
		if err != nil {
			panic("unable to start service: " + err.Error())
		}
	}(conf)

	// wait a bit for the service to start
	time.Sleep(2 * time.Second)

	return service
}

func teardown(service services.TransferService) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	service.Shutdown(ctx)
}

func TestMain(m *testing.M) {
	service := setup()
	code := m.Run()
	teardown(service)
	os.Exit(code)
}
