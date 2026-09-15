# Repository Overview

The Data Transfer Service (DTS) is a Go service that gives clients one API for
searching remote data holdings, collecting file metadata, and requesting file
transfers between participating systems.

The core design assumption in this repository is that DTS must work with a
highly heterogeneous, continuously changing set of unrelated systems. Rather
than requiring a single storage model, metadata shape, identity model, or
transfer stack, DTS isolates system-specific behavior behind pluggable database
and endpoint interfaces and coordinates transfers through a shared workflow.

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

At a conceptual level, DTS is neither a storage platform nor a transfer engine
in its own right. It is an orchestration service. It accepts a user-facing
request, identifies which participating systems must be involved, and drives
those systems through a normalized lifecycle that is simple enough for clients
but flexible enough for operators and integrators.

## Architectural drivers

Several design pressures explain why the codebase looks the way it does:

* **Heterogeneity is expected, not exceptional.** Participating systems may use
  different identifiers, metadata structures, staging workflows, trust models,
  and transfer technologies.
* **Integrations evolve independently.** DTS assumes external systems are owned
  by different organizations and may change on different release cycles.
* **The interoperable contract is intentionally narrow.** DTS only standardizes
  the capabilities it truly needs: search, metadata lookup, staging, transport,
  finalization, and user mapping.
* **Transfers are long-running.** The service must continue work outside the
  lifetime of a single HTTP request and recover from restarts.
* **Provenance matters alongside payload delivery.** A completed transfer is not
  just copied files; it also includes manifest and metadata production.

These drivers lead the repository to emphasize interfaces, orchestration
components, and persisted state rather than a single deep domain model shared by
all integrations.

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

Another useful lens is to think about two different kinds of stability:

* the **northbound** surface is stable and client-oriented: a small REST API,
  predictable transfer states, and a uniform request model
* the **southbound** surface is adaptable and integration-oriented: provider
  implementations, credentials, endpoint details, and local policy

The repository exists to keep those two surfaces decoupled.

## Core runtime model

At runtime, the service follows a coordinator pattern:

* `main.go` loads configuration, initializes logging, creates the service, and
  owns process startup and graceful shutdown.
* `services/` exposes the HTTP API and translates requests into transfer-domain
  operations.
* `transfers/` runs the asynchronous transfer subsystem and owns the long-lived
  state machine for each transfer.

Inside `transfers/`, several cooperating components divide responsibility:

* the **dispatcher** accepts requests, creates transfer records, and decides
  which step comes next
* the **store** is the system of record for transfer specifications,
  descriptors, payload size, and status
* the **stager** handles source-side preparation when files are not immediately
  transferable
* the **mover** delegates payload copy to endpoint providers and monitors
  progress
* the **manifestor** builds and transfers the manifest that describes the
  payload and its metadata

These components communicate through channels and background goroutines. That is
an important architectural choice: DTS treats transfers as asynchronous
workflows rather than request-scoped operations.

## Transfer lifecycle

A typical transfer moves through these steps:

1. The API receives a search, metadata, or transfer request.
2. The service authenticates the caller and validates the request.
3. The dispatcher records the transfer specification and inspects the source and
   destination requirements.
4. If staging is required, the stager requests source-side preparation and polls
   until the files are ready.
5. The mover groups payload files by source endpoint and asks the relevant
   endpoint provider to move them to the destination.
6. The manifestor generates a Frictionless-style manifest describing the
   transfer and places that manifest with the payload.
7. The destination-side finalization logic completes any system-specific
   post-transfer processing.
8. The journal records the outcome, and completed state remains available until
   retention rules remove it.

This lifecycle is deliberately broken into stages because different source and
destination systems become ready at different times. Some payloads can be moved
immediately; others must first be restored from archival storage, staged onto a
filesystem, or associated with a destination-specific import step. DTS handles
that variation by advancing transfers through coarse-grained states instead of
assuming one synchronous copy operation.

## Abstraction boundaries

Two interfaces carry most of the repository's heterogeneity.

### Database integrations

The `databases.Database` interface represents a participating data system from
the perspective of DTS. A database implementation answers questions such as:

* what files match a search query?
* what descriptors and metadata correspond to a set of file IDs?
* which transfer endpoints are valid for this system?
* do files need staging, and if so, what is the staging status?
* how is an ORCID mapped to a local user?
* what finalization work is needed after transfer?

This interface deliberately combines search/discovery concerns with
transfer-preparation concerns because real participating systems often expose
both through the same service boundary.

### Endpoint integrations

The `endpoints.Endpoint` interface represents a transport-capable location. An
endpoint implementation knows how to:

* determine whether files are staged and valid at that location
* start a transfer to another endpoint
* poll for transfer status
* cancel a transfer

This keeps transport mechanics separate from data-system semantics. A database
can say *what* must move, while an endpoint can say *how* it moves.

## Extensibility model

The main extensibility points are:

* **Databases** implement search, metadata lookup, optional staging, user
  federation, and finalization behavior for a participating system.
* **Endpoints** implement the actual file movement mechanism, such as Globus,
  S3, or local filesystem transfers.
* **Configuration** binds named databases and endpoints to concrete provider
  implementations, credentials, and deployment-specific settings.

This model is what allows DTS to adapt to unrelated systems with different
identifiers, metadata schemas, staging requirements, authentication needs, and
transfer technologies.

The configuration layer is especially important. DTS does not hardcode a single
instance of a database or endpoint; it binds deployment-specific names to
provider implementations and credentials. That makes the same codebase usable in
environments where the same provider type may appear with different identities,
roots, permissions, or organizational rules.

The repository also supports a limited notion of custom destinations. That is a
useful design clue: DTS is optimized for configured named systems, but it still
allows controlled escape hatches when a transfer target cannot be modeled as a
first-class database integration.

## Authentication and trust model

Authentication is separated from transfer orchestration. Requests arrive with a
bearer token, and DTS resolves that token to a user identity with an ORCID. The
service can do that through its local encrypted token file or by falling back to
KBase authentication.

That identity is then propagated into transfer logic so source and destination
integrations can apply their own local authorization or federation rules. This
matches the broader architectural pattern of the repository: DTS normalizes just
enough identity information to coordinate work, but it does not try to replace
each participating system's local trust model.

## Persistence and recovery

Long-running orchestration only works if state survives interruption. DTS
persists transfer-related state under its configured data directory and restores
that state when the service starts again. Database save/load hooks, store
records, and the transfer journal all contribute to that recovery model.

Architecturally, this means DTS is not just a thin wrapper over live provider
calls. It is a durable coordinator with memory of in-flight and completed work.

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

The documentation structure mirrors the repository's main audiences:

* **administrators** need deployment and configuration guidance
* **integrators** need to understand the contracts their systems must satisfy
* **developers** need to understand the internal orchestration and extension
  model

## Why the architecture looks this way

The repository favors orchestration over deep normalization. DTS does not try to
force every external system into the same internal storage or execution model.
Instead, it defines a narrow common contract for the capabilities it needs and
lets each integration satisfy that contract in its own way.

That approach keeps the service practical for environments where participating
systems are independently managed, evolve at different speeds, and may only
share a small set of interoperable behaviors.

The tradeoff is that some complexity is intentionally pushed to the edges of the
system. Database and endpoint adapters absorb the messiness of real-world
heterogeneity so that the client API and transfer lifecycle can stay coherent.
That is the central architectural idea to keep in mind when working anywhere in
this repository.
