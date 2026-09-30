# The `databases` Package

This package contains data structures and interfaces defining the functions of data sources for the
DTS. Specific implementations of these data source interfaces live in subpackages within this
package. Supported data sources are:

* `jdp`: [The JGI Data Portal](https://data.jgi.doe.gov/)
* `kbase`: [The KBase Workspace Servіce](https://kbase.us/services/ws/docs/index.html)
* `kbase_lakehouse`: [The KBase Data Lakehouse](https://hub.berdl.kbase.us/hub/)
* `nmdc`: [The DOE National Microbiome Data Collaborative](https://www.genomicscience.energy.gov/nmdc/)

## Selected Topics

### KBase User Federation

The `Database` interface includes a `LocalUser` method that maps an [ORCID](https://orcid.org/) to
a local username for the data source. This is needed to determine the final destination of the files
in a transfer payload.

For KBase-related data sources, this method is implemented by a simple
[user federation goroutine](https://github.com/kbase/dts/blob/main/databases/kbase/user_federation.go)
that reads a spreadsheet file (`kbase_user_orcids.csv`) from the DTS local data directory. The DTS
reads this file on startup and then at the top of each hour, in case it has been updated. The file
itself has two or three columns, which can appear in either possible order:

* `username`: the local username for each KBase user
* `orcid`: the ORCID corresponding to the local user in the same record
* `globusid`: optionally, the Globus ID corresponding to the local user in the same record

Obviously, it would be good for this mapping to be performed automatically by a system of record.
We use this file currently to allow the DTS to perform a transfer on behalf of a user different from
the one whose credentials were used to authorize the request. This capability is used in the KBase
WSS data source. The KBase Lakehouse data source, by contrast, requires that any user requesting a
transfer provide their own KBase token. If this assumption is adopted throughout the DTS, user
federation can be performed at the time of request authentication, eliminating the need for this
kind of machinery.

Why the Globus ID? A KBase user that wishes to perform transfers to the KBase Data Lakehouse
via Globus must provide both S3 credentials and a valid Globus ID in order to use the Globus
S3 connector. If a KBase user has no associated Globus ID, they will not be able to perform
transfers directly to the lakehouse.
