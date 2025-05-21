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
	"context"
	"fmt"
	"log/slog"

	"github.com/deliveryhero/pipeline/v2" 
)

func cancelCause(cause error) {
}

func JdpToKBase(in <-chan Specification, statusUpdateChan chan<- TransferStatusUpdate) {
	// create the various pipeline stages
	first := FirstStage()
	scatter := ScatterStage()
	prepare := PrepareStage()
	transfer := GlobusTransferStage()
	gather := GatherStage()
	manifest := ManifestStage()

	// assemble and run the pipeline
	p1 := pipeline.Join(first, scatter)
	p2 := pipeline.Sequence(prepare, transfer)
	p3 := pipeline.Join(gather, manifest)

	ctx := context.TODO() // FIXME: 
	newTransfer := pipeline.Process(ctx, p1, in)
	scatteredTasks := pipeline.Split(newTransfer)
	processedTasks := pipeline.Process(ctx, p2, scatteredTasks)
	gatheredTasks := pipeline.Merge(processedTasks)
	completed := pipeline.Process(ctx, p3, gatheredTasks)

	// FIXME: ^^^ almost there! Just need to wire up the channels properly.
}
