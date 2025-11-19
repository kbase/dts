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
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

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
		assert.Equal(t, expected.Name, db.Name, "unexpected database name for ID %s", db.Id)
		assert.Equal(t, expected.Organization, db.Organization, "unexpected organization for ID %s", db.Id)
		assert.Equal(t, expected.URL, db.URL, "unexpected URL for ID %s", db.Id)
	}
}

func setup() services.TransferService {
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
