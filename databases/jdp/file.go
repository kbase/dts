package jdp

import (
	"encoding/json"
)

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
	// integer ID representing the status of the file
	StatusId int `json:"file_status_id"`
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

// this type represents metadata associated with a jdpFile
type Metadata struct {
	// proposal info
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
	// status indicating whether data is "Restricted" or "Unrestricted"
	DataUtilizationЅtatus string `json:"data_utilization_status"`
	// GOLD-related metadata
	GoldData struct {
		// stamp ID
		StampId string `json:"gold_stamp_id"`
		// project URL
		ProjectURL string `json:"gold_project_url"`
		// display name
		DisplayName string `json:"display_name"`
	} `json:"gold_data"`
	// sequencing project metadata
	SequencingProject struct {
		// name of scientific program to which project belongs
		ScientificProgramName string `json:"scientific_program_name"`
	} `json:"sequencing_project"`
	// sequencing project ID, sometimes used as ITS project ID. This type can be a
	// list or a number, so we have to unmarshal it into a RawMessage
	SequencingProjectId json.RawMessage `json:"sequencing_project_id"`
	// NCBI taxon metadata
	NCBITaxon struct {
		Order   string `json:"ncbi_taxon_order"`
		Family  string `json:"ncbi_taxon_family"`
		Genus   string `json:"ncbi_taxon_genus"`
		Species string `json:"ncbi_taxon_species"`
	} `json:"ncbi_taxon"`
	// NCBI taxon identifier
	NCBITaxonId int `json:"ncbi_taxon_id"`
	// portal metadata
	Portal struct {
		DisplayLocation []string `json:"display_location"`
	} `json:"portal"`
	// final project delivery metadata
	FinalDeliveryProject struct {
		ProductSearchCategory string `json:"product_search_category"`
	} `json:"final_deliv_project"`
	// analysis project ID, sometimes used as ITS project ID. This type can be a
	// list or a number, so we have to unmarshal it into a RawMessage
	AnalysisProjectId json.RawMessage `json:"analysis_project_id"`
}
