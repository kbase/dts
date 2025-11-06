# Data Transfer Service (DTS)

![build](https://github.com/kbase/dts/actions/workflows/autotest_prs.yml/badge.svg)
[![codecov](https://codecov.io/gh/kbase/dts/graph/badge.svg?token=188OWRPPK6)](https://codecov.io/gh/kbase/dts)

The Data Transfer Service is a web service that handles requests for file
transfers between participating organizations interested in exchanging
biological and bioinformatical data. The DTS coordinates provides a single
point of access for these organizations, allowing an end user or another service
to

* search for datasets / files within any participating organization based on
  criteria specified in an [ElasticSearch](https://www.elastic.co/elasticsearch/)
  style query
* select any or all files from a search and request a transfer from the source
  organization to another participating organization

In its current form, DTS is a prototype that specifically allows transfers from
the [JGI Data Portal](https://data.jgi.doe.gov/) to the [KBase](https://www.kbase.us/)
Workspace, to bring the capabilities of the latter organization to bear on the
data available from the former.

DTS is designed for easy deployment and maintenance behind a gateway that
provides TLS/SSL encryption. Requests to the DTS include headers with
authentication information, so these requests rely on the HTTPS protocol to
protect this information.

It's very easy to deploy DTS in a Docker environment and configure it using
environment variables.

## Building and Testing DTS

DTS is written in [Go](https://go.dev/), so you'll need a working Go compiler
to build, test, and run it locally. If you have a Go compiler, you can clone
this repository and build it from the top-level directory:

```
go build
```

### Running Unit Tests

DTS comes with several unit tests that demonstrate its capabilities. Some tests
require a Minio test instance to be running. You can start one with docker
or podman:

```
docker run -d -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"
```

Then you can run these tests as you would any other Go project:

```
go test ./...
```

You can add a `-v` flag to see output from the tests.

To spin down Minio:
```
docker container stop abc1234
```
(where `abc1234` is the container ID returned when you started the Minio)

Because DTS is primarily an orchestrator of network resources, its unit tests
must be able to connect to and utilize these resources. Accordingly, you must
set the following environment variables to make sure DTS can do what it needs
to do:

* `DTS_KBASE_DEV_TOKEN`: a KBase development token (available to
  [KBase developers](https://docs.kbase.us/development/create-a-kbase-developer-account)
  used to connect to the KBase Auth Server, which provides a context for
  authenticating and authorizing DTS for its basic operations
* `DTS_KBASE_TEST_ORCID`: an [ORCID](https://orcid.org/) identifier that can be
  used to run DTS's unit test. This identifier must match a registered ORCID
  associated with a [KBase user account](https://narrative.kbase.us/#signup).
* `DTS_KBASE_TEST_USER`: the KBase user associated with the ORCID specified
  by `DTS_KBASE_TEST_ORCID`.
* `DTS_GLOBUS_CLIENT_ID`: a client ID registered using the
  [Globus Developers](https://docs.globus.org/globus-connect-server/v5/use-client-credentials/#register-application)
  web interface. This ID must be registered specifically for an instance of DTS.
* `DTS_GLOBUS_CLIENT_SECRET`: a client secret associated with the client ID
  specified by `DTS_GLOBUS_CLIENT_ID`
* `DTS_GLOBUS_TEST_ENDPOINT`: a Globus endpoint used to test DTS's transfer
  capabilities
* `DTS_JDP_SECRET`: a string containing a shared secret that allows the DTS to
  authenticate with the JGI Data Portal
