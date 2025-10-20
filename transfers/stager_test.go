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
	"time"

	"github.com/stretchr/testify/assert"
)

// We attach the tests to this type, which runs them one by one using logic in transfers_test.go.
type StagerTests struct{ Test *testing.T }

func (t *StagerTests) TestStartAndStop() {
	assert := assert.New(t.Test)
	err := stager.Start()
	assert.Nil(err)
	err = stager.Stop()
	assert.Nil(err)
}

func (t *StagerTests) fTestStageFiles() {
	assert := assert.New(t.Test)
	err := stager.Start()
	assert.Nil(err)
	err = store.Start() // need one of these too
	assert.Nil(err)

	// add a transfer to the store and begin staging the files
	spec := Specification{
		Source:      "test-source",
		Destination: "test-destination",
		FileIds:     []string{"file1", "file2", "file3"},
	}
	transferId, err := store.NewTransfer(spec)
	assert.Nil(err)

	err = stager.StageFiles(transferId)
	assert.Nil(err)

	time.Sleep(10 * time.Second)

	err = stager.Stop()
	assert.Nil(err)
	err = store.Stop()
	assert.Nil(err)
}
