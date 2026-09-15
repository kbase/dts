# DTS Developer Guide

This guide contains technical information about the Data Transfer Service (DTS).

## Start Here

* [Repository Overview](repository_overview.md): purpose, architecture,
  architectural drivers, runtime model, extension points, and the major
  directories in this repository

## Architecture Decision Records

[Here](adrs/index.md) we've recorded design decisions in a succinct format that
connects technical issues, decisions, and consequences in a transparent way.

## Code Organization

The following [packages](https://go.dev/doc/code) implement the features in
the Data Transfer Service.

* [auth](auth.md): handles the authorization of the DTS using KBase's
  authentication/authorization server
* [config](config.md): handles the parsing of the DTS [YAML configuration
  file](../admin/config.md), placing the data into read-only global variables
  for use by other packages
* [credit](credit.md): defines metadata types used by the Credit Engine to
  establish the provenance of transferred data
* [databases](databases.md): defines database types that implement the
  integration of DTS with database providers
* [endpoints](endpoints.md): defines endpoint types for file transfer
  providers used by DTS, such as [Globus](https://globus.org)
* [frictionless](frictionless.md): defines [data structures](https://frictionlessdata.io/)
  that describe data for [individual files](https://specs.frictionlessdata.io/data-resource/)
  and [packages containing multiple files](https://specs.frictionlessdata.io/data-package/)
* [services](services.md): defines types that implement the REST endpoints
  provided by the DTS
* [tasks](tasks.md): implements the "heart" of the DTS, which creates and
  manages transfer tasks through their entire lifecycle

## Special Topics

* [KBase Narrative import process](kbase_import.md)
