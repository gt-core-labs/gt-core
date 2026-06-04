# 12 — Documents testing epic (`hq-docs-test`)

Test plan for the document subsystem ([docs/11](11-documents-subsystem.md)): prove the
**objects** — `.md` content and binary attachments — are stored, retrieved, searched, and
used-as-context correctly, end to end. Where docs/11 is the build spec, this is the
verification spec: the NN-16 bead breakdown of what must be tested and at which layer.

Each layer already ships some tests (noted as "have"); this epic fills the gaps ("add") and
adds the cross-cutting integration + multi-tenant + end-to-end coverage the unit/contract
tests can't reach.

## Test layers

| Layer | Engine needed | Gate |
|-------|---------------|------|
| Pure unit (key layout, validation, extraction dispatch) | none | always (CI build-test) |
| Repository contract | Postgres + pgvector | `GT_PG_URL` (CI contract job) |
| Blob roundtrip | object store | `fs` always; MinIO via `GT_BLOB_*` |
| MCP dispatch e2e | Dolt + PG + blob | the running compose stack |
| Hybrid search | + embedder | `embeddings-fastembed` build + `GT_EMBEDDINGS` |

## NN-16 breakdown

Epic **`hq-docs-test`** (`issue_type = epic`). Sub-epics carry `external_ref = hq-docs-test`;
beads read `<sub-epic>.<n>`.

### `hq-docs-test-store` — repository behaviour

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-store.1` | CRUD + version snapshot + soft-delete round trip (`documents_contract`) | have (extend) |
| `hq-docs-test-store.2` | dedup by `sha256` across owners: one blob hash, many rows; soft-deleted row drops out of `find_by_sha` | add |
| `hq-docs-test-store.3` | optimistic `version` guard: stale `update`/`soft_delete` → `VersionConflict`, fresh wins + appends `document_versions` | have |
| `hq-docs-test-store.4` | `list_by_owner_id` returns every owner_type for an id; excludes soft-deleted (backs `gt://issue` inline) | have (extend) |

### `hq-docs-test-blob` — object store

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-blob.1` | `fs` backend put/get/exists/delete + content-addressed key layout | have |
| `hq-docs-test-blob.2` | MinIO/S3 backend roundtrip (gated `GT_BLOB_*`): put → object present → get bytes match → presign returns a URL | add |
| `hq-docs-test-blob.3` | dedup: identical content at the same key is not re-uploaded (write-once) | add |

### `hq-docs-test-extract` — text extraction

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-extract.1` | unknown/`image` type without OCR → `Unsupported`; injected `OcrEngine` used for images | have |
| `hq-docs-test-extract.2` | PDF fixture → expected text; DOCX/XLSX/PPTX fixtures → text nodes extracted | add |
| `hq-docs-test-extract.3` | tesseract `OcrEngine` (feature `ocr-tesseract`): image fixture → OCR'd text | add |

### `hq-docs-test-search` — retrieval

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-search.1` | full-text: a body term hits via `tsv`; unrelated query misses; owner narrowing | have |
| `hq-docs-test-search.2` | hybrid (feature `embeddings-fastembed`): semantically-close-but-lexically-distinct query ranks the right doc above a keyword-only match | add |
| `hq-docs-test-search.3` | a row with no embedding still surfaces on the text side of `search_hybrid` (vector term coalesces to 0) | add |

### `hq-docs-test-api` — MCP dispatch + resources

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-api.1` | every `documents.*` tool: `validate` accepts/rejects shapes; `execute` performs the write | add |
| `hq-docs-test-api.2` | `attach` kind=md persists `body_md`; kind=blob decodes base64 → blob store + `extracted_text` | add |
| `hq-docs-test-api.3` | resources: `gt://doc/{id}` resolves one doc; `gt://issue/{id}` inlines `documents[]`; absent → not-found | add |
| `hq-docs-test-api.4` | scope: a `documents.read`-only actor is denied `documents.attach.execute`; a `closed`/`readonly` profile blocks execute | add |

### `hq-docs-test-mt` — multi-tenant isolation

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-mt.1` | a doc attached in workspace A is invisible to `list`/`search`/`gt://doc` in workspace B (schema isolation, blob key prefix) | add |

### `hq-docs-test-e2e` — full stack (running compose)

| bead | title | state |
|------|-------|-------|
| `hq-docs-test-e2e.1` | attach a `.md` to an epic via MCP → `gt://issue/{id}` returns it inline; `documents.search` finds it | add |
| `hq-docs-test-e2e.2` | attach a PDF via MCP → object lands in MinIO bucket, `extracted_text` is searchable, original fetchable via presigned URL | add |
| `hq-docs-test-e2e.3` | the model-context loop: resolving a bead surfaces its attached docs' text in one read | add |

## The objects, proven used

The epic's intent (docs/11): a model resolving a bead **uses the attached objects as
context**. `hq-docs-test-e2e.3` is the headline assertion — `gt://issue/{id}` (or
`documents.list`/`search`) returns the `.md` body / extracted text inline, so the model reads
supporting material in the same call it reads the bead. The lower layers exist to make that
one behaviour trustworthy.

## What to wire where

- Unit + repository/blob/extract contract beads → `#[cfg(test)]` / `tests/` in the owning
  crate, gated on the relevant engine env (mirrors `documents_contract.rs`).
- `api` + `mt` beads → an integration test in `gt-composition` driving the `DocumentsHandler`
  + resource reads over a test `WsPools`/store (mirrors the existing dispatch tests).
- `e2e` beads → a script/test against the running compose stack (the same one docs/11
  deploys), exercising the live MCP endpoint.
