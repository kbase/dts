# DTS Integration Guide

This document lists all of the components that the Data Transfer System (DTS)
expects from any database with which it interacts. Your organization must
implement each of these components in order to integrate its database(s) with
the DTS in order to take advantage of its file and metadata transfer
capabilities.

We have tried to cover all the necessary topics comprehensively here, but
there's no substitute for a real person when confusion arises, so please don't
hesitate to [contact the KBase DTS development team](mailto:engage@kbase.us)
with your questions. Take a look at the [DTS Integration Glossary](glossary.md)
for an explanation of the terminology used in this guide.

The guidance we present here is not intended to be prescriptive. We provide
suggestions and examples of technical components to illustrate how your
organization can integrate a database with the DTS, but in actuality the DTS
is designed to be flexible and can accommodate various implementations. For
example, we may be able to adapt existing capabilities for DTS integration in
certain situations.

**NOTE**: Currently, the DTS supports the transfer of **public flies only**.
We're gathering information and starting to plan for transferring private and
embargoed data, though, and we'd love to get your input, so if you're interested
in this capability, please [reach out to us](mailto:engage@kbase.us)!

## Overview

The DTS provides a file transfer capability whose organizational unit is
**individual files**. We're not in the business of telling researchers how to
do their jobs, and everyone in the business knows how to use a filesystem. If
your organization's data is stored directly in a database and not as files,
[the DTS team](mailto:engage@kbase.us) can work with you to find the most
appropriate way to write data to files upon request for transfer.

If you're reading this, you're probably interested in making your data available
to the DTS, and/or being able to receive data from other participating
databases. How exactly does the DTS communicate with these databases? Here's
what the DTS needs to navigate your organization's database.

Here's a brief summary of what's needed to connect your database with the DTS.
The sections that follow describe each of these items in more detail.

1. **Every file (resource) in the database has a unique identifier.** The
   identifier can be any string (including a sequence of digits), as long as
   that string refers to exactly one file. The DTS prepends an abbreviation for
   your organization or database to the string to create its own namespaced
   unique identifier. For example, JGI's Data Portal (JDP) has a file with the
   identifier `615a383dcc4ff44f36ca5ba2`, and the DTS refers to this file as
   `JDP:615a383dcc4ff44f36ca5ba2`. This is mostly a matter of policy, but it's
   important to establish an unambiguous way to refer to individual files.
2. **Your database can provide information about a file (resource) given its
   unique identifier.** Specifically, the database provides a **resources
   endpoint** that accepts an HTTP request with a list of file IDs, and
   provides a response containing essential informatіon (the file's location,
   its type and other important metadata) for each of the corresponding files.
3. **Given a search query, your database can identify matching files and
   return a list of IDs for these files.** In other words, the database provides
   a **search endpoint** that accepts an HTTP request with a query string,
   and produces a response containing a list of matching file IDs. This endpoint
   allows a DTS user to select a set of files expediently.
4. **Your database must provide a staging area visible to a supported file
   transfer provider, such as Globus.** The DTS coordinates file transfers, but
   does not handle the transfer of data by itself. For this, it relies on
   commercial providers like [Globus](https://www.globus.org/),
   [Amazon S3](https://aws.amazon.com/s3/), and [iRods](https://irods.org/).
   In order for the DTS to be able to transfer your organization's data, you
   must make a **staging area** available for transferred files that is visible
   to one of these providers.
5. **If necessary, your database can move requested files (resources) to its
   staging area where the DTS can access them for transfer.** If your
   organization archives data to long-term storage (tapes, usually), the DTS
   needs to be able to request that this data be restored to a staging area
   before it can get at them. Your database must provide a **staging endpoint**
   that accepts an HTTP request with a list of resource IDs and returns
   a UUID that can be used to query the status of the staging task.
   Additionally, your database must provide a **staging status endpoint** that
   accepts an HTTP request with a staging request UUID and produces a
   response that indicates whether the staging process has completed.
6. **Your database can map ORCIDs to local users within your organization.**
   Every DTS user must authenticate with an ORCID to connect to the service.
   To establish a connection between the ORCID and a specific user account
   for your organization, your database must provide a **user federation
   endpoint** that accepts an HTTP request with an ORCID and produces a response
   containing the corresponding username for an account within your system. This
   federation process allows DTS to associate a transfer operation with user
   accounts in the organizations for the source and destination databases.

If your organization has existing services that provide similar capabilities but
use different conventions, or if you have other technical considerations, please
[contact the DTS team](mailto:engage@kbase.us) to discuss how we can make the
best of what you have.

## Contents

* [Provide Unique IDs and Metadata for Your Files](resources.md)
* [Make Your Files Searchable](search.md)
* [Provide a Staging Area for Your Files](staging_area.md)
* [Stage Your Files on Request](stage_files.md)
* [Provide a Way to Monitor File Staging](staging_status.md)
* [Map ORCIDs to Local User Accounts](local_user.md)
