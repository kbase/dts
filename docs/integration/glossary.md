# DTS Integration Glossary

We use the following terms in the DTS Integration Guide.

* **DTS**: The [BER data transfer system](https://kbase.github.io/dts/)
* **Frictionless DataPackage**: A [representation](https://specs.frictionlessdata.io/data-package/)
  for a collection of **Frictionless DataResources**. The DTS generates **transfer
  manifests** in this format.
* **Frictionless DataResource**: A [representation](https://specs.frictionlessdata.io/data-resource/)
  of metadata for an individual resource (file)
* **Resource**: A file, with a unique identifier, that can be transferred from
* **Resource endpoint**: an endpoint provided by your database that accepts an
  HTTP `GET` request with a list of file IDs, and provides a response containing
  essential informatіon (the file's location, its type and other important
  metadata) for each of the corresponding files. Read [this](resources.md) for
  more detailed information.
* **Search endpoint**: an endpoint provided by your database that accepts an
  HTTP `GET` request with a query string, and produces a response containing a
  list of matching file IDs. Read [this](search.md) for more detailed
  information.
* **Staging area**: A filesystem or portion of a filesystem on a system
  controlled or provisioned by your organization where you can place files
  for transfer by a bulk transfer provider such as [Globus](https://www.globus.org/),
  [Amazon S3](https://aws.amazon.com/s3/), or [iRods](https://irods.org/).
* **Staging endpoint**: An endpoint provided by your database that accepts an
  HTTP `POST` request with a list of resource IDs and returns a UUID that can
  be used to query the status of a file staging operation. More information is
  available [here](stage_files.md).
* **Staging task**: A process by which files with specific IDs can be
  copied into your organization's **staging area** in preparation for transfer.
  This process can occur over a brief or extended period, depending on the
  number of requested files and the prevailing circumstances, so a staging
  task is assigned a **UUID** that can be used to query its status.
* **Staging status endpoint**: An endpoint provided by your database that
  accepts an HTTP `GET` request with a staging request UUID (obtained from a
  staging endpoint), producing a response that indicates whether the staging
  process has completed. Read [this](staging_status.md) for more information.
* **Transfer Manifest**: a file (usually named `manifest.json`) containing a
  machine-readable **Frictionless DataPackage** containing metadata for a set of
  files transferred by the DTS. The DTS deposits a transfer manifest at the top
  level of the directory structure transferred to the destination database.
  a source database to a destination database by the DTS
* **User federation endpoint**: An endpoint provided by your database that
  accepts an HTTP `GET` request with an ORCID ID and produces a response
  containing the corresponding username for an account within your system.
  Read more about this endpoint [here](local_user.md).
* **UUID**: A [universally unique identifier](https://en.wikipedia.org/wiki/Universally_unique_identifier)
  used by the DTS to represent staging tasks, transfers, and other transient-
  but-possibly-long-running operations
