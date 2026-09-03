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

package kbase_lakehouse

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// The Minio Management Service (MMS) provide authentication information for a user
// given a valid KBase token for that user

type MMSRecord struct {
	Username            string `json:"username"`
	S3AccessKey         string `json:"s3_access_key"`
	S3SecretKey         string `json:"s3_secret_key"`
	PolarisClientId     string `json:"polaris_client_id"`
	PolarisClientSecret string `json:"polaris_client_secret"`
}

type MMS struct {
	Resource string
	Client   http.Client
}

// retrieves the MMS record for the given user KBase token
func (mms MMS) fetchRecord(token string) (MMSRecord, error) {
	resource := mms.Resource + "/credentials/"
	request, err := http.NewRequest(http.MethodGet, resource, http.NoBody)
	if err != nil {
		return MMSRecord{}, err
	}
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	resp, err := mms.Client.Do(request)
	if err != nil {
		return MMSRecord{}, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return MMSRecord{}, err
	}
	resp.Body.Close()

	var record MMSRecord
	err = json.Unmarshal(body, &record)
	return record, err
}
