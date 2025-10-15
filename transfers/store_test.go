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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStartAndStopStore(t *testing.T) {
	assert := assert.New(t)
	err := store.Start()
	assert.Nil(err)
	err = store.Stop()
	assert.Nil(err)
}

func TestStoreNewTransfer(t *testing.T) {
	assert := assert.New(t)

	err := store.Start()
	assert.Nil(err)

	spec := Specification{
	}
	transferId, numFiles, err := store.NewTransfer(spec)
	assert.Nil(err)
	assert.NotEqual(uuid.UUID{}, transferId)
	assert.Greater(0, numFiles)

	spec1, err := store.GetSpecification(transferId)
	assert.Nil(err)
	assert.Equal(spec, spec1)

	desc1 := make([]map[string]any, 0)
	descriptors, err := store.GetDescriptors(transferId)
	assert.Nil(err)
	assert.Equal(desc1, descriptors)

	status, err := store.GetStatus(transferId)
	assert.Nil(err)
	status.Code = TransferStatusStaging
	err = store.SetStatus(transferId, status)
	assert.Nil(err)
	status1, err := store.GetStatus(transferId)
	assert.Nil(err)
	assert.Equal(status1, status)

	err = store.Stop()
	assert.Nil(err)
}
