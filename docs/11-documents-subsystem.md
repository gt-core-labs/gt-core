# 11 — Documents subsystem (`.md`-first content + binary attachments)

Epic spec for **`hq-docs`** — a per-tenant document store that lets operators and
agents attach supporting material to a bead (epic, skill, spec, …) so a model
resolving that bead reads the material as context.

Two content classes, one subsystem:

- **`.md` content** (skills, epic-docs, specs) — the *differentiator*. Plain
  text, versioned, model-readable **without extraction**. Lives as a row
  (`body_md`) in Postgres, partitioned per workspace.
- **Binary attachments** (PDF, image, Office) — bytes live in an S3-compatible
  object store (MinIO); only a pointer + extracted text live in Postgres.

This doc fixes the data model, the storage split, and the NN-16 bead breakdown
across the two delivery phases. It binds [docs/09](09-mt-storage-layout.md)
(storage layout) and [docs/04 §15](04-non-negotiables.md) (isolation invariant).

## Why not files-in-repo

The first instinct — store `.md` as files in a git repo (reusing the `main`
tree the readiness check already walks) — breaks under multiple users:

- **Merge conflicts** on concurrent edits of the same `.md`.
- **Write serialization** — commits race for the branch.
- **No tenant isolation** — one repo means tenant A sees tenant B's files; a
  repo-per-tenant is unmanageable.
- **No per-row RBAC** — git has no concept of the gateway's workspace scope.

Git stays the home of *code* (few authors, PR-gated). Mutable multi-user content
goes to the **per-tenant Postgres** that already realizes the isolation
invariant. `.md` becomes a row, not a file — versioned by a `version` token, not
a branch merge.

## Storage split

| Content | `kind` | Storage | Read path for a model |
|---------|--------|---------|-----------------------|
| `.md` (skills, epic-docs, specs) | `md` | Postgres `documents.body_md` (per-tenant `ws_<slug>`) | `body_md` inlined — raw `.md`, no extractor |
| Binary (PDF/image/Office) | `blob` | MinIO blob + Postgres metadata row | `extracted_text` inlined; original via presigned URL |

MinIO is the **fourth store** alongside Postgres / Dolt / event-log
([docs/09](09-mt-storage-layout.md)); it holds only binary bytes, keyed
`ws_<slug>/<owner_type>/<owner_id>/<uuid>-<filename>`. The MinIO service itself
is wired in the **`gt-app`** repo (compose + config), not here in the core.

## Multi-tenant mechanics

The `documents` table is created once in the **`ws_default` template schema**.
`gt_create_workspace_schema(ws)` ([docs/09](09-mt-storage-layout.md),
`hq-mt-data.2`) clones it into every tenant's `ws_<slug>` via
`CREATE TABLE … LIKE … INCLUDING ALL`, so a new tenant gets its own `documents`
table with zero per-tenant code. A `WorkspacePool` sets `search_path = ws_<slug>`
per connection, so every query reads `FROM documents` unqualified and resolves to
the caller's tenant — physical isolation, no `workspace_id` predicate.

Concurrency is an optimistic `version` token (mirrors `issues.version`): a stale
edit fails loud instead of clobbering a concurrent write. No file-merge step.

## Data model

Per-tenant projection data follows the `gt-rig` rule (docs/04 §15): the table is
defined **once in the `ws_default` template**, schema-qualified, with **no
`workspace_id` column and no FK** — isolation is structural (separate schema),
not a `WHERE` predicate. `gt_create_workspace_schema` clones it per tenant; a
`WorkspacePool` resolves the unqualified `documents` to the caller's schema.

```sql
-- migrations/gt-docs/0001_documents.sql  (defined in ws_default; cloned per tenant)
CREATE SCHEMA IF NOT EXISTS ws_default;

CREATE TABLE IF NOT EXISTS ws_default.documents (
  id            TEXT PRIMARY KEY,
  owner_type    TEXT NOT NULL,                 -- 'epic' | 'skill' | 'spec'
  owner_id      TEXT NOT NULL,
  kind          TEXT NOT NULL DEFAULT 'md'     -- 'md' | 'blob'
                CHECK (kind IN ('md', 'blob')),
  filename      TEXT NOT NULL,
  content_type  TEXT,
  size          BIGINT,
  sha256        CHAR(64),                       -- dedup key
  body_md       TEXT,                           -- kind='md': the .md itself
  bucket        TEXT,                           -- kind='blob': MinIO pointer
  key           TEXT,
  extracted_text TEXT,                          -- kind='blob': text for model+search
  tsv           tsvector,                       -- phase 1 full-text
  -- embedding  vector(1024),                   -- phase 2 (pgvector; added by .search.2)
  version       BIGINT NOT NULL DEFAULT 0,      -- optimistic concurrency
  deleted_at    TIMESTAMPTZ,                    -- soft-delete
  uploaded_by   TEXT,
  uploaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS documents_owner_idx ON ws_default.documents (owner_type, owner_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS documents_sha_idx   ON ws_default.documents (sha256);
CREATE INDEX IF NOT EXISTS documents_tsv_idx   ON ws_default.documents USING gin (tsv);
```

> No `workspace_id` / FK: a row in `ws_acme.documents` *is* acme's, by schema.
> Note `documents_tsv_idx` is the phase-1 full-text index; the phase-2
> `embedding` column + its ivfflat/hnsw index are added by a later migration
> (`hq-docs-search.2`), never by editing this applied file (sqlx checksum-validates).

Design decisions (locked):

- **Versioning: yes** — `version` token + a `document_versions` history table for
  full diff/restore.
- **Delete: soft** — `deleted_at`; a reconciler purges blobs of long-dead rows.
- **Dedup: yes** — by `sha256`; one blob, many rows (cross-owner reuse).
- **Allowed types + extractors: yes** — a closed allow-list (`md`, `pdf`,
  `png/jpg`, `docx/xlsx/pptx`); upload rejects an out-of-list MIME.

## Read path (how a model uses it)

```
model resolves epic E (tenant acme)
  → WorkspacePool(acme): search_path = ws_acme
  → SELECT body_md, extracted_text, kind, filename
       FROM documents
      WHERE owner_type='epic' AND owner_id=E AND deleted_at IS NULL
  → kind='md'   : body_md            → raw .md into context
    kind='blob' : extracted_text     → extracted text into context;
                  original fetched by human via short-TTL presigned URL
```

The `gt://issue/{id}` resource ([gt-issues `resources.rs`]) grows a `documents`
array so the existing per-issue read returns attachments inline — no extra round
trip for the model.

## NN-16 breakdown

Epic **`hq-docs`** (`issue_type = epic`, exempt from the bead rule). Sub-epics
carry `external_ref = hq-docs`; beads read `<sub-epic>.<n>`. A new closed-set
domain `platform.documents` is required (a conscious taxonomy add — see
`hq-docs-api.4`).

### `hq-docs-store` — storage layer

| bead | title | depends_on |
|------|-------|------------|
| `hq-docs-store.1` | `documents` migration in `ws_default` template (PG, per-tenant clone) + `document_versions` history | — |
| `hq-docs-store.2` | `gt-store-blob` kernel crate: `opendal` (feature `services-s3`) `put`/`get`/`presign`/`delete`; `services-fs` for local dev | — |
| `hq-docs-store.3` | `DocumentsRepository` port + PG adapter: insert/get/list/soft-delete, `version` guard, `sha256` dedup | `.1` |
| `hq-docs-store.4` | extraction pipeline (`kind='blob'`): PDF/Office → text (pure-Rust), image → text via the **`OcrEngine` port** → `extracted_text` | `.2`, `.3` |

> **OCR is decoupled** (`hq-docs-store.4`): the extraction crate depends on an
> `OcrEngine` *trait*, never on a concrete engine. Tesseract is one impl behind a
> non-default `ocr-tesseract` feature (its system libs — `libtesseract`/`libleptonica`
> — are installed in CI + the Docker build, not required for a default local build);
> a future engine (cloud OCR, PaddleOCR, …) is a new impl behind its own feature with
> no change to callers. PDF/Office extractors are pure-Rust and always compiled.

### `hq-docs-api` — domain + MCP surface

| bead | title | depends_on |
|------|-------|------------|
| `hq-docs-api.1` | `gt-documents` domain crate: `GtModule` facade + commands `AttachDoc`/`UpdateDoc`/`RemoveDoc` (+ shape `validate`) | `hq-docs-store.3` |
| `hq-docs-api.2` | MCP tools `documents.{attach,update,remove,list}.{validate,execute}` | `.1` |
| `hq-docs-api.3` | resources: `gt://doc/{id}`, `gt://issue/{id}/docs`; inline `documents[]` into `gt://issue/{id}`; presigned URL for `blob` | `.1`, `hq-docs-store.2` |
| `hq-docs-api.4` | taxonomy: add `platform.documents` to the closed `Domain` enum (`gt-issues` + `gt-module-mcp`) | — |

### `hq-docs-search` — retrieval

| bead | title | phase | depends_on |
|------|-------|-------|------------|
| `hq-docs-search.1` | **Phase 1** full-text: `tsv` populate trigger + `gin` index + query path over `body_md`/`extracted_text` | 1 | `hq-docs-store.3` |
| `hq-docs-search.2` | **Phase 2** semantic: `pgvector` `vector(384)` column (migration 0003) + decoupled `Embedder` port (local `fastembed`, feature `embeddings-fastembed`) + `search_hybrid` (text-rank + cosine) | 2 | `.1` |

> **Embedding is decoupled** (`hq-docs-search.2`, mirrors the OCR seam): the handler depends on
> the `Embedder` *trait* (gt-docs-embed), never a concrete model. fastembed (local ONNX,
> AllMiniLM-L6-v2, 384-dim) is one impl behind the non-default `embeddings-fastembed` feature; a
> future engine (hosted API, candle) is a new impl with no caller change. Enabled at runtime via
> `GT_EMBEDDINGS` on a build carrying the feature; otherwise `documents.search` is phase-1
> full-text only. Requires a pgvector Postgres image (CI + gt-app compose use `pgvector/pgvector`).

### `hq-docs-deploy` — infra (in `gt-app`, not core)

| bead | title | depends_on |
|------|-------|------------|
| `hq-docs-deploy.1` | MinIO service in `gt-app` compose + bucket/creds config + per-tenant key prefix policy | `hq-docs-store.2` |

## Phasing

- **Phase 1 (MVP)** — `documents` table, `.md` via `body_md`, blob via MinIO +
  extraction, full-text (`tsvector`). No embeddings. Schema reserves the
  `embedding` column slot but does not populate it.
- **Phase 2** — `pgvector` + embedding engine + hybrid search, switched on once
  keyword recall proves insufficient.

## Open decisions

- **`.md` source of truth**: Postgres `body_md` (chosen, multi-tenant-coherent)
  vs Dolt (`hq_<ws>`, row-level branch/merge). PG wins for tenant isolation +
  built-in `tsvector`/`pgvector`; Dolt remains the home of issues/beads.
- **Embedding engine** (phase 2): Voyage AI (hosted, Anthropic-recommended) vs
  local `fastembed`/`candle` (no external API). Deferred to `hq-docs-search.2`.
