package credit

/*
 * Represents a contributor to the resource.

Contributors must have a 'contributor_type', either 'Person' or 'Organization', and
one of the 'name' fields: either 'given_name' and 'family_name' (for a person), or 'name' (for an organization or a person).

The 'contributor_role' field takes values from the DataCite and CRediT contributor
roles vocabularies. For more information on these resources and choosing
appropriate roles, please see the following links:

DataCite contributor roles: [https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#7a-contributortype](https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#7a-contributortype)

CRediT contributor role taxonomy: [https://credit.niso.org](https://credit.niso.org).

 */
type Contributor struct {
	/*
	 * Must be either 'Person' or 'Organization'.
	 */
	ContributorType string `json:"contributor_type"`
	/*
	 * Persistent unique identifier for the contributor; this might be an ORCID for an individual, or a ROR ID for an organization.
	 */
	ContributorId string `json:"contributor_id"`
	/*
	 * Contributor name. For organizations, this should be the full (unabbreviated) name; can also be used for a person if the given name/family name format is not applicable.
	 */
	Name string `json:"name"`
	/*
	 * The given name(s) of the contributor.
	 */
	GivenName string `json:"given_name"`
	/*
	 * The family name(s) of the contributor.
	 */
	FamilyName string `json:"family_name"`
	/*
	 * List of organizations with which the contributor is affiliated. For contributors that represent an organization, this may be a parent organization (e.g. KBase, US DOE; Arkin lab, LBNL).
	 */
	Affiliations []Organization `json:"affiliations"`
	/*
	 * List of roles played by the contributor when working on the resource.
	 */
	ContributorRoles string `json:"contributor_roles"`
}

/*
 * Source of any kind of data.
 */
type DataSource struct {
	/*
	 * Unix timestamp for when the data was retrieved from that source.
	 */
	AccessTimestamp int `json:"access_timestamp"`
	/*
	 * The date when the data source was last updated, if available.
	 */
	SourceDataUpdated string `json:"source_data_updated"`
	/*
	 * Where the data was retrieved from.
	 */
	SourceName string `json:"source_name"`
	/*
	 * URL from whence data was retrieved.
	 */
	SourceUrl string `json:"source_url"`
}

/*
 * Textual information about the resource being represented.
 */
type Description struct {
	/*
	 * The text content of the informational element.
	 */
	DescriptionText string `json:"description_text"`
	/*
	 * The type of text being represented.
	 */
	DescriptionType string `json:"description_type"`
	/*
	 * The language in which the description is written, using the appropriate IETF BCP-47 notation.
	 */
	Language string `json:"language"`
}

/*
 * Represents an event in the lifecycle of a resource and the date it occurred on.

See [DataCite documentation on date events](https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#8-date) for more information on the events.

 */
type EventDate struct {
	/*
	 * The date associated with the event. The date may be in the format YYYY, YYYY-MM, or YYYY-MM-DD.
	 */
	Date string `json:"date"`
	/*
	 * The nature of the resource-related event that occurred on that date.
	 */
	Event string `json:"event"`
}

/*
 * Represents a funding source for a resource, including the funding body and the grant awarded.

One (or more) of the fields 'grant_id', 'grant_url', or 'funder.organization_name' is required; others are optional.

Recommended resources for organization identifiers include:
  - [Research Organization Registry](http://ror.org)
  - [International Standard Name Identifier](https://isni.org)
  - [Crossref Funder Registry](https://www.crossref.org/services/funder-registry/) (to be subsumed into ROR)

Some organizations may have a digital object identifier (DOI).

 */
type FundingReference struct {
	/*
	 * The funder for the grant or award.
	 */
	Funder Organization `json:"funder"`
	/*
	 * Code for the grant, assigned by the funder.
	 */
	GrantId string `json:"grant_id"`
	/*
	 * Title for the grant.
	 */
	GrantTitle string `json:"grant_title"`
	/*
	 * URL for the grant.
	 */
	GrantUrl string `json:"grant_url"`
}

/*
 * License information for the resource.
 */
type License struct {
	/*
	 * String representing the license, from the [SPDX license identifiers](https://spdx.org/licenses/).
	 */
	Id string `json:"id"`
	/*
	 * URL for the license.
	 */
	Url string `json:"url"`
}

/*
 * Metadata for the credit metadata, including the schema version used, who submitted it, and the date of submission. When the credit metadata for a resource is added or updated, this additional metadata must be provided along with the credit information.
 */
type Metadata struct {
	/*
	 * The version of the credit metadata schema used.
	 */
	CreditMetadataSchemaVersion string `json:"credit_metadata_schema_version"`
	/*
	 * Information about where the credit metadata was sourced from.
	 */
	CreditMetadataSource []DataSource `json:"credit_metadata_source"`
	/*
	 * PID for the user who added this entry.
	 */
	SavedBy string `json:"saved_by"`
	/*
	 * Unix timestamp for the addition of this set of credit metadata.
	 */
	Timestamp int `json:"timestamp"`
}

/*
 * Represents an organization.

Recommended resources for organization identifiers include:
  - [Research Organization Registry](http://ror.org)
  - [International Standard Name Identifier](https://isni.org)
  - [Crossref Funder Registry](https://www.crossref.org/services/funder-registry/) (to be subsumed into ROR)

For example, the US DOE would be entered as:
  organization_name: United States Department of Energy
  organization_id:   ROR:01bj3aw27

 */
type Organization struct {
	/*
	 * Persistent unique identifier for the organization in the format <database name>:<identifier within database>.
	 */
	OrganizationId string `json:"organization_id"`
	/*
	 * Common name of the organization; use the name recommended by ROR if possible.
	 */
	OrganizationName string `json:"organization_name"`
}

/*
 * Represents a persistent unique identifier for an entity and its relationship to some other entity.

The 'id' field and 'relationship_type' fields are required.

The values in the 'relationship_type' field come from controlled vocabularies maintained by DataCite and Crossref. See the documentation links below for more details.

[DataCite relation types](https://support.datacite.org/docs/datacite-metadata-schema-v44-recommended-and-optional-properties#12b-relationtype)

[Crossref relation types](https://www.crossref.org/documentation/schema-library/markup-guide-metadata-segments/relationships/)

 */
type PermanentID struct {
	/*
	 * Persistent unique ID for an entity. Should be in the format <database name>:<identifier within database>.
	 */
	Id string `json:"id"`
	/*
	 * Description of that entity.
	 */
	Description string `json:"description"`
	/*
	 * The relationship between the ID and some other entity. For example, when a PermanentID class is used to represent objects in the CreditMetadata field 'related_identifiers', the 'relationship_type' field captures the relationship between the resource being registered and this ID.
	 */
	RelationshipType string `json:"relationship_type"`
}

/*
 * Represents the title or name of a resource, the type of that title, and the language used (if appropriate).

The 'title' field is required; 'title_type' is only necessary if the text is not the primary title.

 */
type Title struct {
	/*
	 * The language in which the title is written, using the appropriate IETF BCP-47 notation.
	 */
	Language string `json:"language"`
	/*
	 * A string used as a title for a resource.
	 */
	Title string `json:"title"`
	/*
	 * A descriptor for the title for cases where the contents of the 'title' field is not the primary name or title.
	 */
	TitleType string `json:"title_type"`
}

/*
 * Represents the credit metadata associated with an object.

In the following documentation, 'resource' is used to refer to the object
that the CM pertains to, for example, a KBase Workspace object; a
sample from NMDC or ESS-DIVE; sequence data from IMG.

The 'resource_type' field should be filled using values from the [DataCite
resourceTypeGeneral field](https://support.datacite.org/docs/datacite-metadata-schema-v44-mandatory-properties#10a-resourcetypegeneral).

Currently this schema only supports credit metadata for objects of type
'dataset'; anything else will return an error.

The license may be supplied either as an URL pointing to licensing information for
the resource, or using an SPDX license identifier from the list maintained at [https://spdx.org/licenses](https://spdx.org/licenses).

Required fields are:
- identifier
- resource_type
- versioning information: if the resource does not have an explicit version number,
one or more dates should be supplied: ideally the date of resource publication and
the last update (if applicable).
- contributors (one or more required)
- titles (one or more required)
- meta

The resource_type field is required, but as there is currently only a single valid
value, 'dataset', it is automatically populated if no value is supplied.

 */
type CreditMetadata struct {
	/*
	 * List of strings of freeform text providing extra information about this credit metadata.
	 */
	Comment string `json:"comment"`
	/*
	 * The URL of the content of the resource.
	 */
	ContentUrl string `json:"content_url"`
	/*
	 * A list of people and/or organizations who contributed to the resource.
	 */
	Contributors []Contributor `json:"contributors"`
	/*
	 * A list of relevant lifecycle events for the resource. Note that these dates apply only to the resource itself, and not to the creation or update of the credit metadata record for the resource.
	 */
	Dates []EventDate `json:"dates"`
	/*
	 * A brief description or abstract for the resource being represented.
	 */
	Descriptions []Description `json:"descriptions"`
	/*
	 * Funding sources for the resource.
	 */
	Funding []FundingReference `json:"funding"`
	/*
	 * Resolvable persistent unique identifier for the resource. Should be in the format <database name>:<identifier within database>.
	 */
	Identifier string `json:"identifier"`
	/*
	 * Usage license for the resource. Use one of the SPDX license identifiers or provide a link to the license text if no SPDX ID is available.

	 */
	License License `json:"license"`
	/*
	 * Metadata for this credit information, including submitter, schema version, and timestamp.
	 */
	Meta Metadata `json:"meta"`
	/*
	 * The publisher of the resource. For a dataset, this is the repository where it is stored.
	 */
	Publisher Organization `json:"publisher"`
	/*
	 * Other resolvable persistent unique IDs related to the resource.
	 */
	RelatedIdentifiers []PermanentID `json:"related_identifiers"`
	/*
	 * The broad type of the source data for this object. 'dataset' is currently the only valid value supported by this schema.
	 */
	ResourceType string `json:"resource_type"`
	/*
	 * One or more titles for the resource.
	 */
	Titles []Title `json:"titles"`
	/*
	 * The URL of the resource.
	 */
	Url string `json:"url"`
	/*
	 * The version of the resource. This must be an absolute version, not a relative version like 'latest'.
	 */
	Version string `json:"version"`
}


