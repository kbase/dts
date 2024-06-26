package jdp

// The types and functions in this file allow the JDP database implementation
// to interact with JAMO to fill in some holes in required features.
// For information about JAMO, see
// https://docs.jgi.doe.gov/pages/viewpage.action?pageId=65897565

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// this type represents a request to JAMO's pagequery endpoint
type jamoPageQuery struct {
	Query     string `json:"query"`
	Requestor string `json:"requestor"`
}

// this type represents an individual JAMO file record returned within a
// page query response
type jamoFileRecord struct {
	Id              string   `json:"_id"`
	Inputs          []string `json:"inputs"`
	FileType        []string `json:"file_type"`
	AddedDate       string   `json:"added_date"`
	FilePermissions string   `json:"file_permissions"`
	FileStatus      string   `json:"file_status"`
	FileId          int      `json:"file_id"`
	FileSize        int      `json:"file_size"`
	PurgeDate       string   `json:"dt_to_purge"`
	FileGroup       string   `json:"file_group"`
	FileOwner       string   `json:"file_owner"`
	Group           string   `json:"group"`
	FileStatusId    int      `json:"file_status_id"`
	FileDate        string   `json:"file_date"`
	Metadata        struct {
		JatLabel string `json:"jat_label"`
		Portal   struct {
			DisplayLocation []string `json:"display_location"`
		} `json:"portal"`
		Compression      string `json:"compression"`
		FileFormat       string `json:"file_format"`
		TemplateName     string `json:"template_name"`
		MycocosmPortalId string `json:"mycocosm_portal_id"`
		PublishTo        string `json:"publish_to"`
		JatKey           string `json:"jat_key"`
		JatPublishFlag   bool   `json:"jat_publish_flag"`
	} `json:"metadata"`
	FileName             string `json:"file_name"`
	FilePath             string `json:"file_path"`
	User                 string `json:"user"`
	MD5Sum               string `json:"md5_sum"`
	ModifiedDate         string `json:"modified_date"`
	MetadataModifiedData string `json:"metadata_modified_date"`
	GCS                  struct {
		ModifiedDate string `json:"modified_date"`
		UploadDate   string `json:"upload_date"`
	} `json:"gcs"`
}

// here's the type representing the pagequery response itself
type jamoPageQueryResponse struct {
	CursorId    string           `json:"cursor_id"`
	Timeout     int              `json:"timeout"`
	Start       int              `json:"start"`
	End         int              `json:"end"`
	RecordCount int              `json:"record_count"`
	Records     []jamoFileRecord `json:"records"`
}

// override this to attach a VCR recorder to JAMO requests
var jamoClient = &http.Client{
	Timeout: time.Second * 10,
}

// This function gathers and returns all jamo file records that correspond to
// the given list of file IDs. The list of files is returned in the same order
// as the list of file IDs.
func queryJamo(fileIds []string) ([]jamoFileRecord, error) {
	const jamoBaseUrl = "https://jamo-dev.jgi.doe.gov/"

	// prepare a JAMO query with the desired file IDs
	// (also record the indices of each file ID so we can preserve their order)
	fileIdsString := "( "
	indexForFileId := make(map[string]int)
	for i, fileId := range fileIds {
		if i == len(fileIds)-1 {
			fileIdsString += fmt.Sprintf("%s )", fileId)
		} else {
			fileIdsString += fmt.Sprintf("%s, ", fileId)
		}
		indexForFileId[fileId] = i
	}
	payload, err := json.Marshal(jamoPageQuery{
		Query: fmt.Sprintf("select "+
			"_id, file_name, file_path, metadata.file_format, file_size, md5_sum "+
			"where _id in %s", fileIdsString),
		Requestor: "dts@kbase.us",
	})
	if err != nil {
		return nil, err
	}

	// do the initial POST to JAMO and fetch results
	const jamoApiUrl = jamoBaseUrl + "api/metadata/"
	const jamoPageQueryURL = jamoApiUrl + "pagequery"
	req, err := http.NewRequest(http.MethodPost, jamoPageQueryURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-type", "application/json; charset=utf-8")
	resp, err := jamoClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results jamoPageQueryResponse
	err = json.Unmarshal(body, &results)
	if err != nil {
		return nil, err
	}

	// sift file results into place and fetch remaining records
	records := make([]jamoFileRecord, len(fileIds))
	for err == nil {
		for i := 0; i < results.End-results.Start+1; i++ {
			if index, found := indexForFileId[results.Records[i].Id]; found {
				records[index] = results.Records[i]
			} else {
				err = fmt.Errorf("Unrequested record for file ID %s returned!",
					results.Records[i].Id)
				break
			}
		}
		if err != nil {
			break
		}

		// go back for more records
		if results.End < results.RecordCount {
			jamoNextPageUrl := fmt.Sprintf("%snextpage/%s", jamoApiUrl, results.CursorId)
			req, err = http.NewRequest(http.MethodGet, jamoNextPageUrl, http.NoBody)
			if err != nil {
				break
			}
			resp, err = jamoClient.Do(req)
			if err != nil {
				break
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				break
			}
			err = json.Unmarshal(body, &results)
			if err != nil {
				break
			}
			// give the ape some time to respond
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	return records, err
}
