# The `endpoints` Package

This package contains interfaces and data structures representing transfer endpoints used by services
like [Globus](https://www.globus.org/) and [Amazon S3](https://aws.amazon.com/s3/). These endpoints
are used to initiate file transfers, check their status, and cancel them as needed.

Currently, there are implementations for Globus, S3, and "local" endpoints. The bulk of the logic in
these implementations is related to their providers, with logic for elementary operations kept
separate from DTS business logic where practical.

## Selected Topics

### Globus Manifest Uploads

The DTS generates a file manifest for each transferred payload, uploading it to the destination with
the rest of its files. The generation and transfer of this manifest is a little different from the
rest of the file transfer process, because the DTS writes this file itself, which means it needs
access to a local file system. How, then, does the DTS upload the manifest? If the local file system
exists within a Globus collection or share, the manifest can be transferred like any other file. But
this isn't always possible.

In cases where the DTS doesn't have local access to a file system that belongs to an existing
Globus share, the DTS queries the destination endpoint to see if it supports
[HTTPS access](https://docs.globus.org/globus-connect-server/v5.4/https-access-collections/). If
HTTPS access is available, the DTS uses it to upload the manifest directly to the endpoint.

### Premium Globus Connectors (e.g. S3)

Globus offers several [premium storage connectors](https://www.globus.org/connectors) that allow
file transfers between Globus endpoints and those maintained by other providers. We use a Globus
S3 connector to transfer files to the KBase Data Lakehouse environment.

How does the connector determine whether a user is authorized to transfer to one of these special
endpoints? The Globus endpoint uses the [Globus Connect Manager Server API](https://docs.globus.org/globus-connect-server/v5.4/api/)
to register the [user's corresponding credentials](https://docs.globus.org/globus-connect-server/v5.4/api/openapi_User_Credentials/)
on the Connector endpoint, and then Globus seamlessly handles the file transfer.
