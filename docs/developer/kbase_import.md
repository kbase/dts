# KBase Narrative Import Process

[KBase](https://kbase.us) offers useful tools for analyzing genomics data within [Narratives](https://docs.kbase.us/getting-started/quick-start).
By adding some metadata to your DTS transfer request, you can easily import your data into a KBase
narrative.

Here we describe a schema for the `instructions` field of a [DTS transfer POST request](https://dts.kbase.us/docs#/operations/post-api-v1-transfers).
This field is embedded in the `manifest.json` file written to the root of the destination folder for
a payload, and holds a JSON object that tells KBase's staging service how to import the contents of
the payload into a KBase narrative. The schema is described in detail
[here](https://github.com/kbase/staging_service/blob/develop/import_specifications/schema/dts_manifest_schema.json).

What we describe here is relevant only to KBase Narrative imports--the structure of the `instructions`
field is specific to the destination of a transfer to allow the DTS to interact reasonably with
specific systems and organizations.

## JSON Object Structure

The `instructions` field included in the transfer POST request has the following fields:

* `protocol`: contains the string `"KBase narrative import"`
* `objects`: contains an object interpreted as a dictionary whose **keys are supported KBase data
  types** and whose **values are lists of JSON objects**, each with fields specific to that data
  type. Each data types is described in the following section.

Here's a brief example of such a JSON object that annotates two Genbank genome files:

```
{
    "instructions": {
        "protocol": "KBase narrative import",
        "objects": [
            {
                "data_type": "genbank_genome",
                "parameters": {
                    "staging_file_subdir_path": "path/to/some_genome.gbk",
                    "genome_name": "some_genome"
                    "generate_missing_genes": 0,
                    "genome_type": null,
                    "source": null,
                    "release": null,
                    "genetic_code": null,
                    "scientific_name": null,
                    "generate_ids_if_needed": null
                }
            },
            {
                "data_type": "genbank_genome",
                "parameters": {
                    "staging_file_subdir_path": "path/to/some_other_genome.gbk",
                    "genome_name": "some_other_genome"
                    "generate_missing_genes": 0,
                    "genome_type": null,
                    "source": null,
                    "release": null,
                    "genetic_code": null,
                    "scientific_name": null,
                    "generate_ids_if_needed": null
                }
            }
        ]
    }
}
```

!!! note "An object does not necessarily correspond to a single file!"

    Some data types are associated with multiple files. For example, the `gff_genome` type has both
    a FASTA file and a GFF file. These files appear in their respective fields in a single object
    within the `objects` array.

## Supported KBase Data Types

The data types supported by the KBase staging service are listed below with their fields. These data
types are based on the [KBase staging service's import specification templates](https://github.com/kbase/staging_service/tree/master/import_specifications/templates).

!!! note "All fields are required!"

    Every object of a given data type must have all of its fields specified, with `null` values
    indicating empty fields. While inconvenient, this complete specification is required by the
    current implementation of the KBase staging service.

Fields that must be non-`null` are marked **bold** below.

#### Assembly (`assembly`)

* **`assembly_name`**: the name of the assembly object
* `min_contig_length`: an integer containing the minimum length of a contig within the assembly
* **`staging_file_subdir_path`**: a string containing the path to the file, rooted in the directory
  containing the manifest
* `type`: one of the following strings: `"draft isolate"`, `"finished isolate"`, `"mag"`, `"sag"`, `"virus"`, `"plasmid"`, `"construct"`, `"metagenome"`

### Genbank genome (`genbank_genome`)

* **`genome_name`**: the name of the genome object
* `generate_ids_if_needed`: a string
* `genetic_code`: an integer
* **`generate_missing_genes`**: 0 or 1
* `genome_type`: one of the following strings: `"draft isolate"`, `"finished isolate"`, `"mag"`, `"sag"`, `"virus"`, `"plasmid"`, `"construct"`
* `release`: a string
* `scientific_name`: the scientific name of the genome
* `source`: one of the following strings: `"RefSeq user"`, `"Ensembl user"`, `"Other"`
* **`staging_file_subdir_path`**: a string containing the path to the file, rooted in the directory
  containing the manifest

### GFF+FASTA genome (`gff_genome`)

* **`fasta_file`**: a string containing the path to a FASTA file, rooted in the directory
  containing the manifest
* **`generate_missing_genes`**: 0 or 1
* **`gff_file`**: a string containing the path to a GFF file, rooted in the directory
  containing the manifest
* `genetic_code`: an integer
* **`genome_name`**: the name of the genome object
* `genome_type`: one of the following strings: `"draft isolate"`, `"finished isolate"`, `"fungi"`, `"mag"`, `"other Eukaryote"`, `"plant"`, `"sag"`, `"virus"`, `"plasmid"`, `"construct"`
* `release`: a string
* `scientific_name`: the scientic name of the genome
* `source`: one of the following strings: `"RefSeq user"`, `"Ensembl user"`, `"JGI"`, `"Other"`
* `taxon_wsname`: a string

### GFF+FASTA metagenome (`gff_metagenome`)

* **`fasta_file`**: a string containing the path to a FASTA file, rooted in the directory
  containing the manifest
* **`generate_missing_genes`**: 0 or 1
* `genetic_code`: an integer
* **`genome_name`**: the name of the genome object
* **`gff_file`**: a string containing the path to a GFF file, rooted in the directory
  containing the manifest
* `release`: a string
* `source`: one of the following strings: `"EBI user"`, `"IMG user"`, `"JGI user"`, `"BGI user"`, `"Other"`

### Interleaved FASTQ reads (`fastq_reads_interleaved`)

* **`fastq_fwd_staging_file_name`**: a string containing the path to the file, rooted in the directory
  containing the manifest
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* `insert_size_mean`: a float indicating the mean of the insert size distribution
* **`name`**: the name of the interleaved FASTQ reads object
* `read_orientation_outward`: a string
* `sequencing_tech`: one of the following strings: `"Illumina"`, `"PacBio CLR"`, `"PacBio CCS"`, `"IonTorrent"`, `"NanoPore"`, `"Unknown"`
* `single_genome`: a string

### Noninterleaved FASTQ reads (`fastq_reads_noninterleaved`)

* **`fastq_fwd_staging_file_name`**: a string containing the path to the forward reads file, rooted
  in the directory containing the manifest
* **`fastq_rev_staging_file_name`**: a string containing the path to the reverse reads file, rooted
  in the directory containing the manifest
* `insert_size_mean`: a float indicating the mean of the insert size distribution
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* **`name`**: the name of the noninterleaved FASTQ reads object
* `read_orientation_outward`: a string
* `sequencing_tech`: one of the following strings: `"Illumina"`, `"PacBio CLR"`, `"PacBio CCS"`, `"IonTorrent"`, `"NanoPore"`, `"Unknown"`
* `single_genome`: a string

### SRA reads (`sra_reads`)

* `insert_size_mean`: a float indicating the mean of the insert size distribution
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* **`name`**: the name of the SRA reads object
* `read_orientation_outward`: a string
* **`sra_staging_file_name`**: a string containing the path to the SRA staging file, rooted in the
  directory containing the manifest
* `sequencing_tech`: one of the following strings: `"Illumina"`, `"PacBio CLR"`, `"PacBio CCS"`, `"IonTorrent"`, `"NanoPore"`, `"Unknown"`
* `single_genome`: a string
