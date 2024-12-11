# Configuring the DTS

You can configure a DTS instance by creating a [YAML](https://yaml.org/) text
file similar to [dts.yaml.example](https://github.com/kbase/dts/blob/main/dts.yaml.example)
in the repository. Typically this file is named `dts.yaml`, and is passed as an
argument to the `dts` executable. Here we describe the different sections in
this file and how they affect your DTS instance.

## Configuration File Sections

Click on any of the links below to see the relevant details for a section.

* [service](config.md#service): configureѕ settings for the DTS web service
  such as the port on which it listens, the maximum number of connections,
  intervals for polling and scrubbing completed tasks, data directories, and
  diagnostics
* [endpoints](config.md#endpoints): configures the endpoints used to transfer
  files from one place to another
* [databases](config.md#databases): configures databases for organizations that
  integrate with the DTS

Each of these sections is described below, with a motivating example.

## `service`

```yaml
service:
  port: 8080
  max_connections: 100
  max_payload_size: 50
  poll_interval:   60000
  endpoint: globus-local
  data_dir: /path/to/dir
  manifest_dir: /path/to/dir
  delete_after: 604800
  debug: true
  double_check_staging: false
```

The `service` section contains parameters that control nuts-and-bolts behavior
of the web service portion of the Data Transfer Service. The fields in this
section are:

* `port`: the port on which the service listens
* `max_connections`: the maximum number of connections that are simultaneously
  available for DTS clients. If a client sends a request to the DTS when all
  connections are occupied, the request is denied.
* `max_payload_size`: the maximum payload size (in GB) allowed by the service.
  If a client requests the transfer of a payload larger than this size, the
  request is denied.
* `poll_interval`: the interval (in milliseconds) at which the DTS checks for
  progress in any ongoing transfers. Because the file transfers orchestrated by
  the DTS typically take a long time, it's reasonable to set this parameter to
  a minute (60000 ms) or even longer. However, sometimes it's useful to have a
  smaller polling interval, like when you're testing a feature. This parameter
  is optional and defaults to 60000 ms.
* `endpoint`: the name of an endpoint (defined in the [endpoints](config.md#endpoints)
  section) used by the DTS to generate and transfer manifests to destination
  endpoints. This endpoint must have access to the file system to which the DTS
  writes its manifests.
* `data_dir`: a path to a directory on the local file system that the DTS uses
  for its own storage. The DTS should have read/write access to this directory.
* `manifest_dir`: a path to a directory on the local file system in which the
  DTS writes transfer manifests. The endpoint named in the `endpoint` parameter
  must have read access to this directory in order to send the manifest to its
  destination.
* `delete_after`: the interval (in seconds) after which the DTS deletes the
  record for a completed transfer, whether the transfer completed successfully
  or unsuccessfully. This makes it possible for users to query the status of
  completed transfers for the given interval. This parameter is optional and
  defaults to 7 days (604800 seconds).
* `debug`: an optional parameter that, if set to `true`, enables more detailed
  logging and other features that are helpful for troubleshooting and
  development work. The default value is `false`.
* `double_check_staging`: an optional parameter that, if set to `true`, performs
  additional checks for staged files. This parameter can be useful for figuring
  out the appropriate `root` for an endpoint.

## `endpoints`

```yaml
endpoints:
  globus-local:
    name: name-of-local-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
  globus-jdp:
    name: name-of-jdp-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
  globus-kbase:
    name: name-of-kbase-endpoint
    id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    provider: globus
    auth:
      client_id: <ID of client with authentication secret>
      client_secret: <secret>
```

This section is a mapping (set of key-value pairs) that associates the names
of endpoints (keys) with sets of parameters that define their behaviors
(values). The endpoints defined here can be referred to in the other sections.
The fields that define the behavior of each endpoint are:

* `name`: a human-readable name for the endpoint, which can be helpful in
  diagnostic and error-related messages
* `id`: a UUID that uniquely identifies the endpoint in a (provider-specific)
  way that allows the DTS to access it
* `provider`: the name of the service providing the endpoint capability.
  Valid values for this parameter are:
    * `globus`: identifies the endpoint as a Globus Collection (in which case
      the `id` parameter is the corresponding UUID)
    * `local`: identifies the endpoint as a local endpoint with access only to
      the DTS's local file system. This type of endpoint is only useful for
      testing.
* `auth`: this optional parameter provides authentication information to the
  endpoint's provider if necessary. Its fields are:
    * `client_id`: an ID that identifies the DTS to the endpoint's provider as
      a client
    * `client_secret`: a string containing a secret corresponding to the ID
      provided by the `client_id` parameter
* `root`: this optional parameter specifies the root directory used by DTS to
  refer to files on the underlying filesystem of the endpoint. If left blank,
  the root directory is set to `/`.

## `databases`

```yaml
databases:
  jdp:
    name: JGI Data Portal
    organization: Joint Genome Institute
    endpoint: globus-jdp
  kbase:
    name: KBase Workspace Service (KSS)
    organization: KBase
    endpoint: globus-kbase
```

This section is a mapping (set of key-value pairs) that associates the names
of databases (keys) with sets of parameters that define the databases themselves
(at least, as far as the DTS is concerned). These databases are the sources and
destinations for all file transfers performed by the DTS. The keys in this
section identify the databases that are configured for the DTS, and are referred
to in transfer requests specified by DTS clients. Supported databases are:

* `jdp`: the [Joint Genome Institute Data Portal](https://data.jgi.doe.gov/)
* `kbase`: the [Department of Energy Systems Biology Knowledgebase (KBase)](https://www.kbase.us/)

Valid fields for each database are:

* `name`: a human-readable name for the database, useful in diagnostic and
  error-related messages
* `organization`: a human-readable name for the organization that provides the
  database (again, purely informational)
* `endpoint`: the name of the endpoint defined in the [endpoints](config.md#endpoints)
  section that provides the DTS with access to the file staging area for the
  database

