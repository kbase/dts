# DTS Integration Guide

This document lists all of the things you must do in order to integrate your
database with the BER Data Transfer System (DTS) in order to take advantage of
its file and metadata transfer capabilities.

We have tried to cover all the necessary topics comprehensively here, but
there's no substitute for a real person when confusion arises, so please don't
hesitate to contact the KBase DTS development team with your questions.
Take a look at the [DTS Integration Glossary](glossary.md) for an explanation of
the terminology used in this guide.

The guidance we present here is not intended to be prescriptive. We provide
suggestions and examples of technical components to illustrate how your
organization might go about interating with the DTS, but in actuality the DTS
is very flexible and can accommodate various implementations. For example,
we may be able to adapt existing capabilities for DTS integration in certain
situations.

## Overview

The DTS provides a file transfer capability whose organizational unit is
**individual files**. We're not in the business of telling researchers how to
do their jobs, and everyone in the business knows how to use a filesystem.

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
6. **Your database can map ORCID IDs to local users within your organization.**
   Every DTS user must authenticate with an ORCID ID to connect to the service.
   To establish a connection between the ORCID ID and a specific user account
   for your organization, your database must provide a **user federation
   endpoint** that accepts an HTTP request with an ORCID ID and produces
   a response containing the corresponding username for an account within your
   system. This federation process allows DTS to associate a transfer operation
   with user accounts in the organizations for the source and destination
   databases.

Item 1 is entirely a matter of policy enforced within your organization. The
other items have technical components which are discussed in more detail in the
sections below.

## Contents

* [Provide Unique IDs and Metadata for Your Files](resources.md)
* [Make Your Files Searchable](search.md)
* [Provide a Staging Area for Your Files](staging_area.md)
* [Stage Your Files on Request](stage_files.md)
* [Provide a Way to Monitor File Staging](staging_status.md)
* [Map ORCID IDs to Local User Accounts](local_user.md)
