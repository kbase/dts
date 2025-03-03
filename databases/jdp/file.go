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

package jdp

import (
	"encoding/json"
)

// This type represents metadata about an organism associated with one or
// more files.
type Organism struct {
	Id    string `json:"id"`
	Name  string `json:"name"`
	Title string `json:"title"`
	Files []File `json:"files"`
}

// This type represents a single file entry in a JDP ElasticSearch result.
type File struct {
	// unique ID used by the DTS to manipulate the file
	Id string `json:"_id"`
	// name of the file (excluding Path)
	Name string `json:"file_name"`
	// directory in which the file sits
	Path string `json:"file_path"`
	// file size (bytes)
	Size int `json:"file_size"`
	// file metadata
	Metadata Metadata `json:"metadata"`
	// name of the user that owns the file
	Owner string `json:"file_owner"`
	// date that the file was added
	AddedDate string `json:"added_date"`
	// date of last modification to the file
	ModifiedDate string `json:"modified_date"`
	// date file will be purged
	PurgeDate string `json:"dt_to_purge"`
	// file origination date
	Date string `json:"file_date"`
	// string describing the status of the file
	Status string `json:"file_status"`
	// type (or list of types) corresponding to this file
	Type json.RawMessage `json:"file_type"`
	// MD5 checksum
	MD5Sum string `json:"md5sum"`
	// user with access to the file
	User string `json:"user"`
	// name of UNIX group with access to the file
	Group string `json:"file_group"`
	// UNIX file permissions (a string containing the octal representation)
	Permissions string `json:"file_permissions"`
	// name of the group that produced the file's data
	DataGroup string `json:"data_group"`
}

// this type represents metadata associated with a File ^^^
type Metadata struct {
	AnalysisProject struct {
		Status string `json:"status"`
	} `json:"analysis_project"`
	// analysis project ID, sometimes used as ITS project ID. This type can be a
	// list or a number, so we have to unmarshal it into a RawMessage
	AnalysisProjectId    json.RawMessage `json:"analysis_project_id"`
	ContentType          string          `json:"content_type"`
	FinalDeliveryProject struct {
		Name                  string `json:"final_deliv_product_name"`
		ProductSearchCategory string `json:"product_search_category"`
	} `json:"final_deliv_project"`
	GoldData struct {
		// stamp ID
		StampId string `json:"gold_stamp_id"`
		// project URL
		ProjectURL string `json:"gold_project_url"`
		// display name
		DisplayName string `json:"display_name"`
	} `json:"gold_data"`
	IMG struct {
		// TaxonOID can be either a number or a string, because who cares, apparently
		TaxonOID          json.RawMessage `json:"taxon_oid"`
		Database          string          `json:"database"`
		AddDate           string          `json:"add_date"`
		FileType          string          `json:"file_type"`
		Domain            string          `json:"domain"`
		TaxonDisplayName  string          `json:"taxon_display_name"`
		NScaffolds        int             `json:"n_scaffolds"`
		JgiProjectId      int             `json:"jgi_project_id"`
		GcPercent         float64         `json:"gc_percent"`
		TotalBases        int             `json:"total_bases"`
		Assembled         string          `json:"assembled"`
		AnalysisProjectId string          `json:"analysis_project_id"`
	} `json:"img,omitempty"`
	NCBITaxon struct {
		Order   string `json:"ncbi_taxon_order"`
		Family  string `json:"ncbi_taxon_family"`
		Genus   string `json:"ncbi_taxon_genus"`
		Species string `json:"ncbi_taxon_species"`
	} `json:"ncbi_taxon"`
	NCBITaxonId int `json:"ncbi_taxon_id"`
	Portal      struct {
		DisplayLocation []string `json:"display_location"`
		JdpKingdom      string   `json:"jdp_kingdom"`
	} `json:"portal"`
	PmoProject struct {
		ScientificProgram string `json:"scientific_program"`
		PiName            string `json:"pi_name"`
		Name              string `json:"name"`
	} `json:"pmo_project"`
	Proposal struct {
		// DOI of the awarded proposal
		AwardDOI string `json:"award_doi"`
		// info about the Principal Investigator
		PI struct {
			// PI's last name
			LastName string `json:"last_name"`
			// PI's first name
			FirstName string `json:"first_name"`
			// PI's middle name (if any)
			MiddleName string `json:"middle_name"`
			// PI's email address
			EmailAddress string `json:"email_address"`
			// name of academic or industrial institution
			Institution string `json:"institution"`
			// country of institution/PI
			Country string `json:"country"`
		} `json:"pi"`
		// date of proposal approval
		DateApproved string `json:"date_approved"`
		// proposal DOI
		DOI string `json:"doi"`
	} `json:"proposal"`
	ProposalId int `json:"proposal_id"`
	// sequencing project metadata
	SequencingProject struct {
		// name of scientific program to which project belongs
		ScientificProgramName string `json:"scientific_program_name"`
	} `json:"sequencing_project"`
	// sequencing project ID, sometimes used as ITS project ID. This type can be a
	// list or a number, so we have to unmarshal it into a RawMessage
	SequencingProjectId json.RawMessage `json:"sequencing_project_id"`
}
