# The `auth` Package

This package includes representations of users and credentials, along with a handful of
authentication capabilities:

* a proxy for the KBase auth server, which is used to authenticate users with KBase tokens
* a proxy for the KBase Minio Management Service (MMS), which maps a KBase tokens to any additional
  associated user credentials, like S3 for the KBase Lakehouse environment
* a standalone DTS authenticator that reads data from an encrypted file on the local file system

## A Few Remarks

The DTS standalone authenticator was created to allow users without KBase dev tokens to request
transfers in the early days of the project. Given that KBase (in its old and new incarnations) is
the only destination for DTS file transfers so far, it seems likely that this standalone
authenticator is not strictly necessary. But it's pretty simple.

**Who gets to request a file transfer?** Every resource-burdened request to the DTS must have an
`Authorization` header with a valid KBase or DTS token. In a file transfer to the KBase WSS, this
token need not belong to the user to whom the files are transferred -- each transfer accepts an
ORCID that is mapped to a local user on the destination system, so an authenticated user can
request file transfers on behalf of other users.

In the KBase Lakehouse, the authenticated user is the only one authorized to receive the transferred
files. Technically, this is because the KBase auth server passes this user's token to the MMS and
returns the user's S3 credentials, which are needed to complete the transfer.

A user's S3 credentials are stored in a set of named credentials under the `auth.User`'s
`ConnectionCredentials` field. The keys in this field are providers for whom credentials are
stored (`s3`, `globus`, etc), and the values are credentials with IDs, usernameѕ, and secrets.
