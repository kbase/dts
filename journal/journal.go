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

package journal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/kbase/dts/config"
)

// This is the DTS transfer journal, which logs all transfer activity. The journal is a table of
// transfer records (one per transfer).

// a record storing all information relevant to a transfer
type Record struct {
	// UUID associated with the transfer
	Id uuid.UUID `json:"id"`
	// the source and destination associated with the transfer
	Source      string `json:"source"`
	Destination string `json:"destination"`
	// the ORCID associated with the transfer
	Orcid string `json:"orcid"`
	// times at which the transfer was requested and at which it completed
	StartTime time.Time `json:"start_time"`
	StopTime  time.Time `json:"stop_time"`
	// status of the transfer ("succeeded", "failed", or "canceled")
	Status string `json:"status"`
	// size of the transfer's payload in bytes
	PayloadSize int64 `json:"payload_size"`
	// number of files in the transfer's payload
	NumFiles int `json:"num_files"`
	// manifest containing metadata for the transfer's payload (stored separate from record)
	Manifest *datapackage.Package `json:"-"`
}

// initialize the DTS transfer journal
func Init() error {
	if !IsOpen() {
		go transferJournalProcess()
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// saves and closes the DTS transfer journal (if it's been opened)
func Finalize() error {
	if IsOpen() {
		channels_.Input.Shutdown <- struct{}{}
		closeChannels()
	}
	return nil
}

// returns true if the journal is open for writing, false if not
func IsOpen() bool {
	if channels_.Open { // has Init() been called?
		channels_.Input.CheckIfOpen <- struct{}{}
		select {
		case isOpen := <-channels_.Output.IsOpen:
			return isOpen
		case <-time.After(1 * time.Second): // after a second, we assume the goroutine has crashed
			closeChannels()
			return false
		}
	}
	return false
}

// records a completed transfer
// record: the record containing all transfer information
func RecordTransfer(record Record) error {
	switch record.Status {
	case "succeeded", "failed", "canceled":
		// pass-through (see below)
	default:
		return &NewRecordError{
			Id:      record.Id,
			Message: fmt.Sprintf("Invalid status: %s", record.Status),
		}
	}

	if !IsOpen() {
		return &NotOpenError{}
	}

	channels_.Input.CreateRecord <- record
	return <-channels_.Output.Error
}

// retrieves records for transfers that started and finished within the time range with the given
// (inclusive) bounds
// start: the beginning of the time period of interest
// stop: the end of the time period of interest
func Records(start, stop time.Time) ([]Record, error) {
	if !IsOpen() {
		return nil, &NotOpenError{}
	}
	channels_.Input.FetchRecords <- TimeRange{Start: start, Stop: stop}
	var records []Record
	var err error
	select {
	case records = <-channels_.Output.Records:
		return records, err
	case err = <-channels_.Output.Error:
		return records, err
	}
}

//-----------
// Internals
//-----------

// The transfer journal gets its own goroutine so it doesn't bring down the entire service if it
// crashes. Here we define "input" channels (main process -> goroutine) and "output" channels
// (goroutine -> main process) for passing data back and forth

type TimeRange struct {
	Start, Stop time.Time
}

var channels_ struct {
	Open  bool // true if channels are open, false if not
	Input struct {
		CreateRecord chan Record    // for creating new records
		CheckIfOpen  chan struct{}  // for checking to see whether the database is open
		FetchRecords chan TimeRange // for fetching records within a time range
		Shutdown     chan struct{}  // for shutting down the database
	}

	Output struct {
		Records chan []Record // for returning records
		Error   chan error    // for returning errors
		IsOpen  chan bool     // for answering queries about whether the database is open
	}
}

func transferJournalProcess() {

	// open the database, creating the schema if necessary
	dbPath := filepath.Join(config.Service.DataDirectory, "transfer_journal.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		channels_.Output.Error <- &CantOpenError{
			Message: err.Error(),
		}
	}

	// set up buckets for transfer records and manifests
	db.Update(func(tx *bolt.Tx) error {
		for _, bucketName := range []string{"transfers", "manifests"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return err
			}
		}
		return nil
	})

	openChannels()

	// handle requests
	running := true
	for running {
		select {

		case <-channels_.Input.CheckIfOpen:
			channels_.Output.IsOpen <- true // always true if this goroutine is running!

		case record := <-channels_.Input.CreateRecord:
			err := createRecord(db, record)
			channels_.Output.Error <- err

		case timeRange := <-channels_.Input.FetchRecords:
			records, err := fetchRecords(db, timeRange.Start, timeRange.Stop)
			if err != nil {
				channels_.Output.Error <- err
			} else {
				channels_.Output.Records <- records
			}

		case <-channels_.Input.Shutdown:
			err := db.Close()
			if err != nil {
				channels_.Output.Error <- &CantCloseError{
					Message: err.Error(),
				}
			}
			running = false
		}
	}
}

func openChannels() {
	channels_.Open = true
	channels_.Input.CreateRecord = make(chan Record)
	channels_.Input.CheckIfOpen = make(chan struct{})
	channels_.Input.FetchRecords = make(chan TimeRange)
	channels_.Input.Shutdown = make(chan struct{})
	channels_.Output.Records = make(chan []Record)
	channels_.Output.Error = make(chan error)
	channels_.Output.IsOpen = make(chan bool)
}

func closeChannels() {
	channels_.Open = false
	close(channels_.Input.CreateRecord)
	close(channels_.Input.CheckIfOpen)
	close(channels_.Input.FetchRecords)
	close(channels_.Input.Shutdown)
	close(channels_.Output.Records)
	close(channels_.Output.Error)
	close(channels_.Output.IsOpen)
}

func createRecord(db *bolt.DB, record Record) error {
	startTime := record.StartTime.Format(time.RFC3339)

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// store the transfer record, indexing it by its start time
	bucket := tx.Bucket([]byte("transfers"))

	jsonBytes, err := json.Marshal(&record)
	if err != nil {
		return err
	}
	err = bucket.Put([]byte(startTime), jsonBytes)
	if err != nil {
		return err
	}

	// if the transfer succeeded, store its manifest (indexed by UUID)
	if record.Manifest != nil {
		jsonManifest, err := json.Marshal(record.Manifest.Descriptor())
		if err != nil {
			return &NewRecordError{
				Id:      record.Id,
				Message: err.Error(),
			}
		}
		bucket := tx.Bucket([]byte("manifests"))
		bucket.Put([]byte(record.Id.String()), jsonManifest)
	}

	return tx.Commit()
}

func fetchRecords(db *bolt.DB, start, stop time.Time) ([]Record, error) {
	records := make([]Record, 0)
	err := db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("transfers")).Cursor()

		startTime := []byte(start.Format(time.RFC3339))
		stopTime := []byte(stop.Format(time.RFC3339))

		for k, v := c.Seek(startTime); k != nil && bytes.Compare(k, stopTime) <= 0; k, v = c.Next() {
			var record Record
			err := json.Unmarshal(v, &record)
			if err != nil {
				return err
			}
			records = append(records, record)
		}

		// get manifests for each successfully completed transfer (this can be slow)
		bucket := tx.Bucket([]byte("manifests"))
		for i := range records {
			if records[i].Status == "succeeded" {
				m := bucket.Get([]byte(records[i].Id.String()))
				var err error
				if m != nil {
					records[i].Manifest, err = datapackage.FromString(string(m), "manifest.json", validator.InMemoryLoader())
				}
				if m == nil || err != nil {
					return &InvalidRecordError{
						Id:      records[i].Id,
						Message: "unable to retrieve manifest for successful transfer",
					}
				}
			}
		}
		return nil
	})

	return records, err
}
