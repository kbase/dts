# DTS Pipelines

Rules:

* One pipeline per transfer operation
    * Each transfer (sub)task traverses the pipeline
    * A pipeline can have multiple paths, depending on the needs of the tasks
      underlying its transfer
    * The pipeline lives as long as the transfer it performs
    * A pipeline can be saved ("persisted") in GOB format and recreated later
        * A "transfer table" is updated whenever a pipeline stage completes, and
          this table is saved and restored in GOB format
    * A specific transfer struct completely and unambiguously defines a pipeline
        * A factory function is used to create the pipeline from the transfer
* Types of pipeline stages:
    * **File preparation** - restoration from tape, moving into place for transfer
    * **Globus transfer** - a transfer from a Globus source to a Globus destination
    * **HTTP transfer** - a transfer from an HTTP source to a destination
    * **Archive extraction** - extracting of files from a ZIP or TAR archive(?)
    * **Manifest generation** - generation of a transfer manifest
    * **CDM (Parquette) postprocessing** - transformation of payload to CDM format

* JDP -> KBase pipeline
    * File preparation
    * Globus transfer
    * Manifest generation

* NMDC -> KBase pipeline
    * Globus transfer
    * Manifest generation

* USGS -> KBase pipeline
    * HTTP transfer
    * Globus transfer
    * Manifest generation

