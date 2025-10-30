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

package nmdc

import (
	"fmt"
)

// This error type indicates an unsupported workflow type was specified.
type UnsupportedWorkflowTypeError struct {
	WorkflowId string
}

func (e UnsupportedWorkflowTypeError) Error() string {
	return fmt.Sprintf("unsupported NMDC workflow ID: %s", e.WorkflowId)
}

// This error type indicates raw data files were returned during a workflow retrieval
// operation when they were not expected.
type UnexpectedRawDataFilesError struct {
	WorkflowID string
}

func (e UnexpectedRawDataFilesError) Error() string {
	return fmt.Sprintf("unexpected raw data files returned for NMDC workflow ID: %s", e.WorkflowID)
}

// This error type indicates too many records were returned.
type TooManyRecordsError struct {
	ResourceType string
	Identifier   string
	Count        int
}

func (e TooManyRecordsError) Error() string {
	return fmt.Sprintf("too many NMDC records returned for %s with identifier %s: %d",
		e.ResourceType, e.Identifier, e.Count)
}

// This error type indicates unsupported extra fields were included in a NMDC search.
type ExtraFieldsInSearchError struct {
	StudyID string
	Fields  []string
}

func (e ExtraFieldsInSearchError) Error() string {
	return fmt.Sprintf("unsupported extra fields included in NMDC search for study ID: %s, fields: %v",
		e.StudyID, e.Fields)
}
