# The `endpoints` Package

This package contains interfaces and data structures representing transfer endpoints used by services
like [Globus](https://www.globus.org/) and [Amazon S3](https://aws.amazon.com/s3/). These endpoints
are used to initiate file transfers, check their status, and cancel them as needed.

Currently, there are implementations for Globus, S3, and "local" endpoints. The bulk of the logic in
these implementations is related to their providers, with logic for elementary operations kept
separate from DTS business logic where practical.
