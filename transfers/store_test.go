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

package transfers

import (
	"cmp"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// We attach the tests to this type, which runs them one by one.
type StoreTests struct{ Test *testing.T }

func (t *StoreTests) TestStartAndStop() {
	assert := assert.New(t.Test)
	err := store.Start()
	assert.Nil(err)
	err = store.Stop()
	assert.Nil(err)
}

func (t *StoreTests) TestNewTransfer() {
	assert := assert.New(t.Test)

	err := store.Start()
	assert.Nil(err)

	spec := Specification{
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2", "file3"},
	}
	transferId, err := store.NewTransfer(spec)
	assert.Nil(err)
	assert.NotEqual(uuid.UUID{}, transferId)

	spec1, err := store.GetSpecification(transferId)
	assert.Nil(err)
	assert.Equal(spec, spec1)

	var desc1 []map[string]any
	for _, d := range testDescriptors {
		desc1 = append(desc1, d)
	}
	slices.SortFunc(desc1, func(a, b map[string]any) int {
		return cmp.Compare(a["id"].(string), b["id"].(string))
	})
	descriptors, err := store.GetDescriptors(transferId)
	assert.Nil(err)
	assert.Equal(desc1, descriptors)

	status, err := store.GetStatus(transferId)
	assert.Nil(err)
	assert.Equal(TransferStatusUnknown, status.Code)

	err = store.Stop()
	assert.Nil(err)
}

func (t *StoreTests) TestSetStatus() {
	assert := assert.New(t.Test)

	err := store.Start()
	assert.Nil(err)

	spec := Specification{
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2", "file3"},
	}
	transferId, err := store.NewTransfer(spec)
	assert.Nil(err)

	status, _ := store.GetStatus(transferId)
	err = store.SetStatus(transferId, status)
	assert.Nil(err)
	status1, err := store.GetStatus(transferId)
	assert.Nil(err)
	assert.Equal(status1, status)

	status, err = store.GetStatus(uuid.New())
	assert.NotNil(err)

	err = store.Stop()
	assert.Nil(err)
}

func (t *StoreTests) TestRemove() {
	assert := assert.New(t.Test)

	err := store.Start()
	assert.Nil(err)

	spec := Specification{
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2", "file3"},
	}
	transferId, err := store.NewTransfer(spec)

	err = store.Remove(transferId)
	assert.Nil(err)

	err = store.Stop()
	assert.Nil(err)
}
