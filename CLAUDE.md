# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`uvalib/easystore` — a Go object-storage abstraction (namespace/id keyed objects made of *fields*, opaque *metadata*, and binary *files*) plus a set of CLI tools that operate on a store. The library is consumed both in-process (direct datastore access) and over HTTP (proxy to a deployed easystore service).

## Repository layout

- `uvaeasystore/` — the library. **This is its own Go module** (`github.com/uvalib/easystore/uvaeasystore`); the only test suite lives here.
- `tools/easystore-*/` — CLI tools. **Each tool is a separate Go module** with its own `go.mod`/`go.sum` and `replace github.com/uvalib/easystore/uvaeasystore => ../../uvaeasystore`. There is no workspace file, so `go mod tidy` must be run per-module.
- `db/postgres/`, `db/s3/`, `db/sqlite/` — DDL for the `objects`, `fields`, `blobs` tables. `db/s3/` is the schema used by the S3 datastore (only `objects` + `fields`; blobs live in S3).
- `package/` — Dockerfile that cross-compiles every tool into an `easystore-tools` container, plus `scripts/build.ksh` and `scripts/shell.ksh` for local image build/shell.
- `pipeline/buildspec.yml` — AWS CodeBuild: builds the container, pushes to ECR, records the build tag in SSM.

## The `service` build tag — read this first

Everything that talks to a datastore directly is behind `//go:build service`: `datastore.go`, `s3-datastore*.go`, `pg-datastore.go`, `db-datastore-*.go`, `easystore-events.go`, `uva-easystore-impl.go`, `uva-easystore-readonly-impl.go`, `uva-easystore-object-set-impl.go`, and `uva-easystore-factory.go` (which is where `NewEasyStore` / `NewEasyStoreReadonly` live).

Consequences:
- Without `-tags service` the package compiles to the proxy client only (`NewEasyStoreProxy`, `NewEasyStoreProxyReadonly`) — that is the intended surface for ordinary API consumers, which is why they don't pull in AWS/postgres behaviour.
- Every build and test command in this repo passes `-tags service`. If a build mysteriously reports an undefined `NewEasyStore` or `DatastoreS3Config`, the tag is missing.
- When adding a file that references `DataStore` or a `Datastore*Config`, it needs the tag too.

## Commands

Library (from repo root; the `Makefile` `cd`s into `uvaeasystore` for you):

```sh
make test                          # go test -tags service -v
make TEST=TestObjectCreate test    # single test, or an alternation: TEST='A|B'
make fmt vet
make check                         # staticcheck + the shadow vet tool
make dep                           # go get -u && go mod tidy
```

`make test` (and `make build`, which just runs `test`) talks to live infrastructure — see below. `make vet` is clean; `make check` reports a standing backlog of ~39 staticcheck findings and so exits non-zero, which also means the shadow step after it does not run. Untagged `go build ./...` works and simply excludes the service files, but untagged `go test`/`go vet` cannot build the test files at all.

Tools (from a `tools/easystore-*/` directory):

```sh
make            # == make darwin -> bin/<name>.darwin
make all        # darwin + linux
make linux      # what the Dockerfile invokes
make clean fmt vet check dep
```

The per-tool `check` targets still use a bare `go install` with no `@version` and a hardcoded `$(HOME)/go/bin`, so they fail outside a module / when `GOBIN` is set. The root `Makefile` has been fixed; these have not.

Container: `package/scripts/build.ksh` then `package/scripts/shell.ksh` (tools land in `/easystore-tools/bin`).

## Running against a store

There is **no local/offline store**. The sqlite and plain-postgres paths are commented out and marked NOT SUPPORTED throughout; only `s3` and `proxy` modes work. So tests and tools both need live credentials/endpoints, supplied by environment (gitignored `env.*` files are the convention here):

- proxy mode: `ESENDPOINT`
- s3 mode: `BUCKET`, `SIGNER_ACCESS_KEY`, `SIGNER_SECRET_KEY`, `SIGNEXPIRE` (tools) / `SIGN_EXPIRE_MINUTES` (tests), `DBHOST`, `DBPORT`, `DBNAME`, `DBUSER`, `DBPASS`, `DBTIMEOUT`. Also needs ambient AWS role credentials — the S3 client uses the default AWS config chain for reads/writes and the static signer keys only for presigning.

Tools select the backend with `-mode s3|proxy`. **The test suite has no such flag**: it is switched by editing the `datastore` package var (and `debug` / `enableBus`) at the top of `uvaeasystore/uva-easystore-helpers_test.go`; it currently reads `proxy`. Tests write real objects into namespace `test-namespace` — chosen deliberately so downstream lambdas ignore them. Don't change that namespace, and note that `s3` mode tests mutate the configured bucket.

## Architecture

Two independent implementations of the same `EasyStore` / `EasyStoreReadonly` interfaces (`uva-easystore.go`):

1. **Direct** (`service` tag): `easyStoreImpl` → `DataStore` interface → `S3Storage` or `dbStorage`. This layer does the validation, the vtag check, and the event publishing.
2. **Proxy** (`uva-easystore-proxy.go`): `easyStoreProxyImpl` marshals the same calls to REST endpoints — `{endpoint}/{ns}`, `{endpoint}/{ns}/{oid}`, `.../file/{name}`, `.../file/{name}/content`, `{endpoint}/{ns}/search`, `{endpoint}/healthcheck`. Because the wire format contains interface-typed members, `easystore-proxy-api.go` hand-rolls `UnmarshalJSON` for objects/blobs/metadata; add new object fields there as well as in the serializer.

`uva-easystore-preflight.go` holds argument validation shared by both, so the proxy rejects bad input before a round trip and the service rejects it again.

### S3 datastore: S3 is truth, postgres is a cache

`S3Storage` (`s3-datastore*.go`) writes each object as keys under `{namespace}/{oid}/`: `object.json`, `fields.json`, `metadata.json`, and one key per file/blob. The postgres `objects` and `fields` tables are a *queryable mirror* — they exist because you cannot search S3. Hence:

- `DataStore` getters take a `useCache bool` (`FROMCACHE` / `NOCACHE`); cache reads hit postgres, `NOCACHE` reads hit S3. Metadata is never cached.
- Field search (`GetKeysByFields`) is postgres-only and builds its `WHERE` clause dynamically, ANDing the requested fields via `GROUP BY ... HAVING count(*) = n`.
- The mirror can drift, which is what `easystore-s3-check` (compare/verify) and `easystore-s3-rebuild` (repopulate the DB from bucket contents) are for.

`dbStorage` (`db-datastore-impl.go`, postgres-only-with-blobs-in-`BYTEA`) is legacy: several `Update*` methods return `ErrNotImplemented`, and it cannot stream payloads.

### Concurrency control via vtag

Every object carries a `vtag` (`vtag-<xid>`). `ObjectUpdate` / `ObjectDelete` re-read the stored object and return `ErrStaleObject` if the caller's vtag differs; any successful update mints a new one. Callers must therefore round-trip the object they read, and `ProxyEasyStoreObject(ns, id, vtag)` exists to build a minimal object for an update without fetching it first.

### Components bitmask

`BaseComponent 0x00`, `Fields 0x01`, `Files 0x10`, `Metadata 0x100`, `AllComponents 0x111` — note these are spaced hex nibbles, not sequential bits. Reads, updates, and deletes all take this mask to select which parts of an object to touch; preflight rejects anything `> AllComponents`.

### Blobs stream by default

`EasyStoreBlob` payloads may be buffered (`NewEasyStoreBlobFromBuffer`) or streamed (`NewEasyStoreBlobFromReader` / `FromFile`). Calling `Payload()` on a streaming blob returns `ErrPayloadNotBuffered` — use `PayloadReader()`. Prefer the streaming constructors for large files; the store consumes and closes the reader.

### Events

With `MessageBus()` configured, the direct implementation publishes create/update/delete events for objects, metadata, and files to librabus (`easystore-events.go`). Publish failures are logged, never fatal, and `ErrBusNotConfigured` is treated as "telemetry disabled" rather than an error.

## Conventions

- Errors are the package-level sentinels in `uva-easystore.go`, wrapped as `fmt.Errorf("%q: %w", context, ErrX)`; compare with `errors.Is`.
- Exported interfaces are in `uva-easystore.go`; each has a lowercase `*Impl` struct in its own `*-impl.go`, constructed only through the factory functions. Serialized struct fields use a trailing underscore (`Namespace_`, `Vtag_`).
- `make check` intentionally suppresses several staticcheck/stylecheck rules (`-ST1000,-S1002,-ST1003,...`) — the codebase uses `== true` / `== false` comparisons and non-idiomatic names throughout; match the surrounding style rather than "fixing" it.
- Files end with a `// end of file` comment block.
