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

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/datapackage-go/validator"
	bolt "go.etcd.io/bbolt"

	"github.com/kbase/dts/journal"
)

func main() {
	if len(os.Args) < 2 {
		usage("extract_records")
		return
	}

	filename, startTime, stopTime, err := parseArgs()
	if err != nil {
		panic(err.Error())
	}

	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		panic(fmt.Sprintf("Couldn't open transfer journal '%s'.", filename))
	}
	defer db.Close()

	records, err := readRecords(db, startTime, stopTime)
	if err != nil {
		panic(fmt.Sprintf("Couldn't read transfer records: '%s'.", err.Error()))
	}

	summarize(records, startTime, stopTime)
}

func usage(program_name string) {
	fmt.Printf("%s: Extracts transfer records from a journal.\n", program_name)
	fmt.Printf("Usage:\n%s journal.db [start-time] [end-time]\n", program_name)
}

func parseArgs() (filename string, start, stop time.Time, err error) {
	flag.Parse()
	filename = flag.Args()[0]
	if len(flag.Args()) > 1 {
		start, err = time.Parse(time.RFC3339, flag.Args()[1])
		if err != nil {
			return
		}
		if len(flag.Args()) > 2 {
			stop, err = time.Parse(time.RFC3339, flag.Args()[2])
			if err != nil {
				return
			}
		} else {
			stop = time.Now().AddDate(1, 0, 0)
		}
	} else {
		start = time.Now().AddDate(-1, 0, 0)
		stop = time.Now().AddDate(1, 0, 0)
	}
	if start.After(stop) {
		err = fmt.Errorf("start time '%s' is after stop time '%s'", start.Format(time.RFC3339), stop.Format(time.RFC3339))
	}
	return
}

func readRecords(db *bolt.DB, start, stop time.Time) ([]journal.Record, error) {
	var records []journal.Record
	err := db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("transfers")).Cursor()
		startTime := []byte(start.Format(time.RFC3339))
		stopTime := []byte(stop.Format(time.RFC3339))
		for k, v := c.Seek(startTime); k != nil && bytes.Compare(k, stopTime) <= 0; k, v = c.Next() {
			var record journal.Record
			err := json.Unmarshal(v, &record)
			if err != nil {
				return err
			}
			records = append(records, record)
		}

		manifests := tx.Bucket([]byte("manifests"))
		for i := range records {
			if records[i].Status == "succeeded" {
				m := manifests.Get([]byte(records[i].Id.String()))
				var err error
				if m != nil {
					records[i].Manifest, err = datapackage.FromString(string(m), "manifest.json", validator.InMemoryLoader())
				}
				if m == nil || err != nil {
					return fmt.Errorf("unable to retrieve manifest for successful transfer %s", records[i].Id.String())
				}
			}
		}
		return nil
	})
	return records, err
}

func summarize(records []journal.Record, start, stop time.Time) {
	var numBytes, numTransfers, numFiles uint64
	distinctFileDirs := make(map[string]bool)
	distinctDOIs := make(map[string]bool)
	distinctGrantIds := make(map[string]bool)
	distinctLicenseURLs := make(map[string]bool)

	for _, record := range records {
		numTransfers++
		numFiles += uint64(record.NumFiles)
		numBytes += record.PayloadSize

		// citation stats
		if record.Manifest != nil {
			resources := record.Manifest.Resources()
			for _, resource := range resources {
				desc := resource.Descriptor()
				path := desc["path"].(string)
				dir := filepath.Dir(path)
				distinctFileDirs[dir] = true

				if entry, ok := desc["related_identifiers"]; ok {
					if relatedIds, ok := entry.([]map[string]any); ok {
						for _, relatedId := range relatedIds {
							if d, ok := relatedId["description"]; ok {
								description := d.(string)
								if strings.Contains(description, "DOI") {
									if id, ok := desc["id"]; ok {
										if doi, ok := id.(string); ok {
											distinctDOIs[doi] = true
										}
									}
								}
							}
						}
					}
				}

				if entry, ok := desc["funding"]; ok {
					if fundingSources, ok := entry.([]map[string]any); ok {
						for _, fundingSource := range fundingSources {
							if entry, ok := fundingSource["grant_id"]; ok {
								if grantId, ok := entry.(string); ok {
									distinctGrantIds[grantId] = true
								}
							}
						}
					}
				}

				if entry, ok := desc["license"]; ok {
					if license, ok := entry.(map[string]any); ok {
						if entry, ok := license["url"]; ok {
							if licenseUrl, ok := entry.(string); ok {
								distinctLicenseURLs[licenseUrl] = true
							}
						}
					}
				}
			}
		}
	}

	fmt.Printf("Transfer Statistics for %s - %s:\n\n", start.Format(time.RFC3339), stop.Format(time.RFC3339))
	fmt.Printf("# transfers: %d\n", numTransfers)
	fmt.Printf("# files: %d\n", numFiles)
	fmt.Printf("# bytes: %d\n", numBytes)
	fmt.Printf("# distinct source directories: %d\n", len(distinctFileDirs))
	fmt.Printf("# distinct DOIs: %d\n", len(distinctDOIs))
	fmt.Printf("# distinct grant IDs: %d\n", len(distinctGrantIds))
	fmt.Printf("# distinct license URLs: %d\n", len(distinctLicenseURLs))
}
