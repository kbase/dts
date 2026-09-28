# The `auth` Package

This package includes representations of users and credentials, along with a handful of
authentication capabilities:

* a proxy for the KBase auth server, which is used to authenticate users with KBase tokens
* a proxy for the KBase Minio Management Service (MMS), which maps a KBase tokens to any additional
  associated user credentials, like S3 for the KBase Lakehouse environment
* a standalone DTS authenticator that reads data from an encrypted file on the local file system

## Selected Topics

