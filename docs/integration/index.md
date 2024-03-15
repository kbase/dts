# DTS Integration Guide

This document lists all of the things you must do in order to integrate your
database with the BER Data Transfer System (DTS) in order to take advantage of
its file and metadata transfer capabilities.

We have tried to cover all the necessary topics comprehensively here, but
there's no substitute for a real person when confusion arises, so please don't
hesitate to contact the KBase DTS development team with your questions.

The DTS provides a file transfer capability whose organizational unit is
**individual files**. We're not in the business of telling researchers how to
do their jobs, and everyone in the business knows how to use a filesystem.

Take a look at the [DTS Glossary](../common/glossary.md) for an explanation of
the terminology used in this guide.

## Overview

If you're reading this, you're probably interested in making your data available
to the DTS, and/or being able to receive data from other participating
databases. How exactly does the DTS communicate with these databases? Here's
what the DTS needs to navigate your organization's database.

1. **Every file (resource) in the database has a unique identifier.** The
   identifier can be any string, as long as that string refers to exactly one
   file. The DTS prepends an abbreviation for your organization or database to
   the string to create its own namespaced unique identifier. For example, JGI's
   Data Portal (JDP) has a file with the identifier `615a383dcc4ff44f36ca5ba2`,
   and the DTS refers to this file as `JDP:615a383dcc4ff44f36ca5ba2`.
2. **Your database can provide information about a file (resource) given its
   unique identifier.** Specifically, the database provides an **resources
   endpoint** that accepts an HTTP GET request with a list of file IDs, and
   provides a response containing essential informatіon (the file's location,
   its type and other important metadata) for each of the corresponding files.
3. **Given a search query, your database can identify matching files and
   return a list of IDs for these files.** In other words, the database provides
   a **search endpoint** that accepts an HTTP GET request with a query string,
   and produces a response containing a list of matching file IDs. This endpoint
   allows a DTS user to select a set of files expediently.
4. **If necessary, your database can move requested files (resources) to a
   staging area where the DTS can access them for transfer.** If your
   organization archives data to long-term storage (tapes, usually), the DTS
   needs to be able to request that this data be restored to a staging area
   before it can get at them. Your database must provide a **staging endpoint**
   that accepts an HTTP POST request with a list of resource IDs and returns
   a UUID that can be used to query the status of the staging operation.
   Additionally, your database must provide a **staging status endpoint** that
   accepts an HTTP GET request with a staging request UUID and produces a
   response that indicates whether the staging process has completed.
5. **Your database can map ORCID IDs to local users within your organization.**
   Every DTS user must authenticate with an ORCID ID to connect to the service.
   To establish a connection between the ORCID ID and a specific user account
   for your organization, your database must provide a **federation endpoint**
   that accepts an HTTP GET request with an ORCID ID and produces a response
   containing the corresponding username for an account within your system.
   This federation process allows DTS to associate a transfer operation with
   user accounts in the organizations for the source and destination databases.

Item 1 is entirely a matter of policy enforced within your organization. The
other items have technical components which are discussed in more detail in the
sections below.

## Contents

* [Provide Unique IDs and Metadata for Your Files](metadata.md)
* [Make Your Files Searchable](search.md)
* [Provide a Staging Area for Your Files](endpoint.md)
* [Stage Your Files on Request](staging.md)
* [Map ORCID IDs to Local User Accounts](orcid.md)
