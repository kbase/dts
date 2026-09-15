# Repository Overview

The Data Transfer Service (DTS) is a Go service that gives clients one API for
searching remote data holdings, collecting file metadata, and requesting file
transfers between participating systems.

The core design assumption in this repository is that DTS must work with a
highly heterogeneous, continuously changing set of unrelated systems. Rather
than requiring a single storage model or transfer stack, DTS isolates
system-specific behavior behind pluggable database and endpoint interfaces and
coordinates transfers through a shared workflow.

## Purpose

DTS sits between clients and external data providers. It is responsible for:

* authenticating requests and associating them with users
* exposing REST endpoints for search, metadata lookup, transfer creation, and
  transfer status
* translating those requests into calls against configured source and
  destination systems
* coordinating staging, transfer, manifest generation, finalization, and
  persistence of transfer state

In practice, this means the service can present a stable API even when source
databases, destination databases, and transfer providers all have different
capabilities and conventions.

## High-level architecture

DTS is organized into a few layers:

1. **Service/API layer** (`main.go`, `services/`): starts the process, loads
   configuration, exposes the HTTP API, and validates user requests.
2. **Workflow/orchestration layer** (`transfers/`): owns transfer lifecycle
   management, persistence, status updates, and coordination between staging,
   moving, and finalization.
3. **Integration layer** (`databases/`, `endpoints/`, `auth/`): adapts
   database-specific, endpoint-specific, and authentication-specific behavior to
   common interfaces.
4. **Support packages** (`config/`, `journal/`, `credit/`): provide
   configuration loading, transfer journaling, and metadata/provenance support.

This separation lets the API and workflow logic stay mostly stable while new
systems are introduced through integration packages and configuration.

## Transfer lifecycle

A typical transfer moves through these steps:

1. The API receives a search, metadata, or transfer request.
2. The service authenticates the caller and validates the request.
3. The transfer dispatcher records the request and determines whether files must
   be staged first.
4. If staging is needed, the stager polls the source database until files are
   ready.
5. The mover asks the configured transfer endpoint provider to move files to the
   destination.
6. The manifestor writes transfer metadata and places it with the payload.
7. The destination database finalizes the transfer, and the journal records the
   outcome.

State is persisted so transfers can survive service restarts.

## Extensibility model

The main extensibility points are:

* **Databases** implement search, metadata lookup, optional staging, user
  federation, and finalization behavior for a participating system.
* **Endpoints** implement the actual file movement mechanism, such as Globus,
  S3, or local filesystem transfers.
* **Configuration** binds named databases and endpoints to concrete provider
  implementations and credentials.

This model is what allows DTS to adapt to unrelated systems with different
identifiers, metadata schemas, staging requirements, authentication needs, and
transfer technologies.

## Repository layout

Key areas of the repository are:

* `/main.go`: process startup and shutdown
* `/services`: REST API handlers and transport-facing types
* `/transfers`: dispatcher, store, stager, mover, manifestor, and transfer
  state management
* `/databases`: shared database interface plus concrete integrations such as
  JDP, KBase, NMDC, and S3-backed databases
* `/endpoints`: shared endpoint interface plus concrete transfer providers
* `/auth`: request authentication and user identity lookup
* `/config`: YAML configuration loading and validation
* `/journal`: transfer record persistence
* `/docs`: MkDocs site for operators, integrators, and developers
* `/integration`, `/dtstest`: integration-test support and test helpers

## Why the architecture looks this way

The repository favors orchestration over deep normalization. DTS does not try to
force every external system into the same internal storage or execution model.
Instead, it defines a narrow common contract for the capabilities it needs and
lets each integration satisfy that contract in its own way.

That approach keeps the service practical for environments where participating
systems are independently managed, evolve at different speeds, and may only
share a small set of interoperable behaviors.
