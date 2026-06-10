//! `memory.*` domain dispatch (hq-memory-mcp.3).
//!
//! Routes the semantic-memory tools — `memory.{save,recall,list,forget}` — onto the
//! per-workspace [`MemoryRepository`] and (when wired) the [`Embedder`]. Mirrors the
//! `documents.*` handler ([`DocumentsHandler`](super::documents::DocumentsHandler)): a
//! durable, named note an agent writes once and later retrieves BY MEANING over the same
//! hybrid keyword (`tsv`) + vector (`embedding`) path documents use.
//!
//! **Embedding is best-effort, exactly like `documents`' `embed_doc`.** A `save` embeds
//! `name + description + body` and stores the vector; if no embedder is wired, or the
//! embed call fails, the row still persists — semantic recall just misses it until it is
//! re-saved. A write never fails on the embedding.
//!
//! **`recall` always surfaces every `feedback` memory.** Feedback memories are *hard
//! operating rules* the autonomous loop must obey ([`MemoryKind::is_operating_rule`]) — a
//! rule you do not recall is a rule you violate. So `recall` returns the full `feedback`
//! set UNCONDITIONALLY (via `by_kind("feedback")`), fused with the relevance-ranked top-k
//! of everything else, deduplicated by `name`. This is the same principle the planner's
//! `planning_brief` applies: the binding constraints are not subject to a relevance cutoff.
//! When `kind="feedback"` is requested explicitly the relevance path already is the
//! feedback set, so the union is a no-op; when a *different* `kind` is requested the
//! feedback rules are still prepended (they bind regardless of the query's topic).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_docs_embed::Embedder;
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_memory::MemoryKind;
use gt_module::McpTool;
use gt_store_dolt::AppError;
use gt_store_pg::{MemoryError, MemoryRepository, MemoryRow, NewMemory, PgMemory};

use super::pools::WsPools;
use super::util::{descriptor, opt, req, str_arg};

/// The `feedback` recall class: the hard operating rules `recall` always returns in full.
const FEEDBACK: &str = "feedback";

/// Default `recall` window for the relevance-ranked (non-feedback) tail.
const DEFAULT_LIMIT: i64 = 10;

/// PG-backed handler for the `memory.*` tool namespace.
///
/// Mirror of [`DocumentsHandler`](super::documents::DocumentsHandler): it HOLDS the
/// per-workspace pool cache and resolves the tenant's [`PgMemory`] repository
/// **per-request** from `ctx.workspace`, rather than binding a single pre-resolved repo.
/// Without this, every `memory.*` call would hit `ws_default.memories` regardless of the
/// caller's tenant (hq-memory-admin.4).
pub struct MemoryHandler {
    pools: Arc<WsPools>,
    /// Optional semantic-search engine. `Some` ⇒ memories are embedded on `save` and
    /// `recall` uses the hybrid (text + vector) path; `None` ⇒ full-text only. An embed
    /// failure degrades to full-text rather than erroring (mirror of `documents`).
    embedder: Option<Arc<dyn Embedder>>,
}

impl MemoryHandler {
    /// Wire the per-workspace pool cache and the optional embedding engine. The repository
    /// is resolved per-request in [`dispatch`](Self::dispatch) from the caller's workspace.
    pub fn new(pools: Arc<WsPools>, embedder: Option<Arc<dyn Embedder>>) -> Self {
        Self { pools, embedder }
    }

    /// Resolve the tenant-scoped memory repository for the caller's workspace (mirror of
    /// `DocumentsHandler::repo`). The pool's `search_path` targets `ws_<slug>.memories`.
    async fn repo(&self, ws: Option<&str>) -> Result<PgMemory, AppError> {
        Ok(PgMemory::new(self.pools.get(ws).await?))
    }

    /// `memory.save`: upsert by name, then best-effort embed. The embed never gates the
    /// write — a missing/failing embedder just leaves this row out of the vector index.
    async fn save(&self, ws: Option<&str>, cmd: SaveArgs) -> Result<Value, AppError> {
        let repo = self.repo(ws).await?;
        let new = NewMemory {
            name: cmd.name.clone(),
            description: cmd.description.clone(),
            kind: cmd.kind.as_str().to_string(),
            body: cmd.body.clone(),
            created_by: cmd.created_by,
        };
        let row = repo.upsert(new).await.map_err(mem_err)?;
        // Best-effort embedding over name + description + body (mirror of
        // `DocumentsHandler::embed_doc`): a missing embedder or an embed/store failure is
        // silently skipped; the save already succeeded.
        if let Some(emb) = &self.embedder {
            let text = format!("{} {} {}", cmd.name, cmd.description, cmd.body);
            if !text.trim().is_empty() {
                if let Ok(vec) = emb.embed(&text).await {
                    let _ = repo.set_embedding(&cmd.name, vec).await;
                }
            }
        }
        Ok(mem_json(&row))
    }

    /// `memory.recall`: the full `feedback` rule set fused with the relevance-ranked top-k
    /// of everything else (or of the requested `kind`), deduplicated by `name`. See the
    /// module docs for why feedback is unconditional.
    async fn recall(&self, ws: Option<&str>, cmd: RecallArgs) -> Result<Value, AppError> {
        let repo = self.repo(ws).await?;
        // The hard operating rules — always returned in full, regardless of the query.
        let mut out = repo.by_kind(FEEDBACK).await.map_err(mem_err)?;

        // The relevance-ranked tail: hybrid when an embedder is wired and the embed
        // succeeds, else full-text. An embed failure degrades to full-text, never errors.
        let kind = cmd.kind.as_deref();
        let ranked = match &self.embedder {
            Some(emb) => match emb.embed(&cmd.query).await {
                Ok(vec) => repo
                    .recall_hybrid(&cmd.query, &vec, kind, cmd.limit)
                    .await
                    .map_err(mem_err)?,
                Err(_) => repo
                    .recall(&cmd.query, kind, cmd.limit)
                    .await
                    .map_err(mem_err)?,
            },
            None => repo
                .recall(&cmd.query, kind, cmd.limit)
                .await
                .map_err(mem_err)?,
        };

        // Fuse without duplicating: a feedback row already in `out` is not re-added.
        for row in ranked {
            if !out.iter().any(|m| m.name == row.name) {
                out.push(row);
            }
        }
        Ok(json!({ "count": out.len(), "memories": out.iter().map(mem_json).collect::<Vec<_>>() }))
    }
}

#[async_trait]
impl DomainHandler for MemoryHandler {
    fn namespace(&self) -> &'static str {
        "memory"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        // Hand-built descriptors (mirror of `audit.tail`): the `descriptor`/`req`/`opt`
        // helpers fold an object input schema into `tools/list` + `meta.help`, so the four
        // memory tools are as discoverable as `documents.*`. The handler still validates on
        // dispatch (discovery, not enforcement).
        vec![
            descriptor(
                "memory.save",
                "Write (upsert-by-name) a durable, named memory. `kind` is one of \
                 feedback|project|reference|user (feedback = a hard operating rule the loop \
                 must obey). Best-effort embedded for semantic recall.",
                &[
                    req("name", "string"),
                    req("description", "string"),
                    req("kind", "string"),
                    req("body", "string"),
                    opt("created_by", "string"),
                ],
            ),
            descriptor(
                "memory.recall",
                "Retrieve memories BY MEANING. ALWAYS returns every `feedback` rule (in \
                 full) plus the top-k of the rest by relevance, deduplicated. Optional \
                 `kind` narrows the relevance tail; `limit` caps it (default 10).",
                &[
                    req("query", "string"),
                    opt("kind", "string"),
                    opt("limit", "integer"),
                ],
            ),
            descriptor(
                "memory.list",
                "List memories: all of one `kind` when given, else every memory in the \
                 tenant, ordered by name.",
                &[opt("kind", "string")],
            ),
            descriptor(
                "memory.get",
                "Inspect a single memory BY EXACT `name` (no semantic search). Returns the \
                 full row; errors if no memory with that name exists in the tenant.",
                &[req("name", "string")],
            ),
            descriptor(
                "memory.forget",
                "Hard-delete a memory by `name`. Idempotent — forgetting an absent memory \
                 is a successful no-op.",
                &[req("name", "string")],
            ),
            descriptor(
                "memory.clear",
                "Bulk hard-delete, returning the count removed. With `kind`, deletes exactly \
                 that class (including feedback when named explicitly); without `kind`, deletes \
                 everything EXCEPT feedback so the hard operating rules survive. Requires \
                 `confirm: true` — an unconfirmed call deletes nothing.",
                &[req("confirm", "boolean"), opt("kind", "string")],
            ),
            descriptor(
                "memory.conflicts",
                "Read-only redundancy scan: report pairs of memories whose embeddings are \
                 near-duplicates (cosine similarity >= `threshold`, default 0.85), most-similar \
                 first, capped at `limit` (default 50). Reports only — nothing is deleted; a human \
                 or agent decides whether to merge/forget. High similarity = redundant, not \
                 necessarily contradictory.",
                &[opt("threshold", "number"), opt("limit", "integer")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "memory.save" => {
                let cmd = SaveArgs::parse(&ctx)?;
                self.save(ctx.workspace, cmd).await
            }
            "memory.recall" => {
                let cmd = RecallArgs::parse(&ctx)?;
                self.recall(ctx.workspace, cmd).await
            }
            "memory.list" => {
                // Validate the optional kind BEFORE touching the pool so a bad `kind` is a
                // pure validation error (no tenant connection needed).
                let kind = parse_opt_kind(&ctx)?;
                let repo = self.repo(ctx.workspace).await?;
                // `by_kind` when a (validated) kind is given, else the whole tenant.
                let rows = match kind {
                    Some(kind) => repo.by_kind(kind.as_str()).await.map_err(mem_err)?,
                    None => repo.list().await.map_err(mem_err)?,
                };
                Ok(json!({
                    "count": rows.len(),
                    "memories": rows.iter().map(mem_json).collect::<Vec<_>>(),
                }))
            }
            "memory.get" => {
                // Exact-name lookup (no semantic search): resolve the tenant repo per-request
                // like list/forget, then fetch the one row. An absent name is reported as a
                // `NotFound` validation error — coherent with how `mem_err` maps the store's
                // own `MemoryError::NotFound`, rather than a silent `null` a caller might miss.
                let repo = self.repo(ctx.workspace).await?;
                let name = str_arg(&ctx.args, "name")?;
                let row = repo
                    .get(name)
                    .await
                    .map_err(mem_err)?
                    .ok_or_else(|| mem_err(MemoryError::NotFound(name.to_string())))?;
                Ok(mem_json(&row))
            }
            "memory.forget" => {
                let repo = self.repo(ctx.workspace).await?;
                let name = str_arg(&ctx.args, "name")?;
                repo.forget(name).await.map_err(mem_err)?;
                Ok(json!({ "ok": true, "name": name, "forgotten": true }))
            }
            "memory.clear" => {
                // Guard: a bulk delete must be explicitly confirmed. Validate confirm + kind
                // BEFORE touching the pool so an unconfirmed/bad call is a pure validation error
                // that deletes nothing.
                let confirm = ctx.args.get("confirm").and_then(Value::as_bool).unwrap_or(false);
                if !confirm {
                    return Err(AppError::Validation(
                        "memory.clear is a bulk delete — pass confirm:true to proceed".into(),
                    ));
                }
                let kind = parse_opt_kind(&ctx)?;
                let repo = self.repo(ctx.workspace).await?;
                // `Some(kind)` clears that class (feedback only when named); `None` clears all
                // EXCEPT feedback — the hard rules survive a blanket clear (store-side guard).
                let deleted = repo.clear(kind.map(|k| k.as_str())).await.map_err(mem_err)?;
                Ok(json!({ "ok": true, "kind": kind.map(|k| k.as_str()), "deleted": deleted }))
            }
            "memory.conflicts" => {
                // Validate threshold ∈ [0,1] BEFORE touching the pool (pure validation error).
                let threshold = match ctx.args.get("threshold") {
                    None | Some(Value::Null) => 0.85_f64,
                    Some(v) => v
                        .as_f64()
                        .filter(|t| (0.0..=1.0).contains(t))
                        .ok_or_else(|| {
                            AppError::Validation("`threshold` must be a number in [0, 1]".into())
                        })?,
                };
                let limit = ctx.args.get("limit").and_then(Value::as_i64).unwrap_or(50).clamp(1, 500);
                let repo = self.repo(ctx.workspace).await?;
                let pairs = repo.conflicts(threshold, limit).await.map_err(mem_err)?;
                Ok(json!({
                    "count": pairs.len(),
                    "threshold": threshold,
                    "conflicts": pairs.iter().map(|p| json!({
                        "a": p.a_name, "a_description": p.a_description, "a_kind": p.a_kind,
                        "b": p.b_name, "b_description": p.b_description, "b_kind": p.b_kind,
                        "similarity": p.similarity,
                    })).collect::<Vec<_>>(),
                }))
            }
            other => Err(AppError::Validation(format!("unknown memory tool `{other}`"))),
        }
    }
}

/// Parsed `memory.save` arguments, with `kind` already validated to the four-token set.
struct SaveArgs {
    name: String,
    description: String,
    kind: MemoryKind,
    body: String,
    created_by: String,
}

impl SaveArgs {
    fn parse(ctx: &DomainCtx<'_>) -> Result<Self, AppError> {
        let args = &ctx.args;
        Ok(Self {
            name: str_arg(args, "name")?.to_string(),
            description: str_arg(args, "description")?.to_string(),
            kind: parse_kind(str_arg(args, "kind")?)?,
            body: str_arg(args, "body")?.to_string(),
            // Default authorship to the dispatch actor when the caller omits it.
            created_by: args
                .get("created_by")
                .and_then(Value::as_str)
                .unwrap_or(ctx.actor)
                .to_string(),
        })
    }
}

/// Parsed `memory.recall` arguments.
struct RecallArgs {
    query: String,
    kind: Option<String>,
    limit: i64,
}

impl RecallArgs {
    fn parse(ctx: &DomainCtx<'_>) -> Result<Self, AppError> {
        let args = &ctx.args;
        Ok(Self {
            query: str_arg(args, "query")?.to_string(),
            kind: parse_opt_kind(ctx)?.map(|k| k.as_str().to_string()),
            limit: args
                .get("limit")
                .and_then(Value::as_i64)
                .unwrap_or(DEFAULT_LIMIT),
        })
    }
}

/// Validate a `kind` token against the four-member set, rejecting anything else.
fn parse_kind(s: &str) -> Result<MemoryKind, AppError> {
    MemoryKind::parse(s).ok_or_else(|| {
        AppError::Validation(format!(
            "invalid kind `{s}` — must be one of feedback|project|reference|user"
        ))
    })
}

/// Validate the optional `kind` argument: absent ⇒ `None`; present-but-invalid ⇒ error.
fn parse_opt_kind(ctx: &DomainCtx<'_>) -> Result<Option<MemoryKind>, AppError> {
    match ctx.args.get("kind") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let s = v
                .as_str()
                .ok_or_else(|| AppError::Validation("`kind` must be a string".into()))?;
            parse_kind(s).map(Some)
        }
    }
}

/// Map a [`MemoryError`] onto the MCP error space: a client-fixable condition (missing
/// name, stale version) as `Validation`, a backend failure as `Other`. Mirror of
/// `documents`' `doc_err`.
fn mem_err(e: MemoryError) -> AppError {
    match e {
        MemoryError::NotFound(_) | MemoryError::VersionConflict { .. } => {
            AppError::Validation(e.to_string())
        }
        MemoryError::Db(_) => AppError::Other(e.to_string()),
    }
}

/// Render a [`MemoryRow`] as the JSON payload returned to the client. The body is included
/// so a model reading a recall gets the fact inline (mirror of `documents`' `doc_json`).
fn mem_json(m: &MemoryRow) -> Value {
    json!({
        "name": m.name,
        "description": m.description,
        "kind": m.kind,
        "body": m.body,
        "version": m.version,
        "created_by": m.created_by,
        "updated_at": m.updated_at.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    //! Two test tiers, mirroring the multi-tenant `documents.*` shape:
    //!
    //! 1. **Pure-validation tests** (always run): namespace, the seven advertised tools in
    //!    `meta.help`, invalid-kind rejection, and unknown-tool routing. These short-circuit
    //!    *before* any pool is resolved (kind is parsed ahead of `self.repo(...)` on every
    //!    arm, and the unknown-tool arm never touches the pool), so a `WsPools` over a dummy
    //!    URL that is never dialed suffices.
    //!
    //! 2. **PG-backed behavioral tests** (gated on `GT_PG_URL`, skipped otherwise): the
    //!    end-to-end save→recall, feedback-union+dedup, list-by-kind, and idempotent-forget
    //!    semantics — the part that needs a real `ws_<slug>.memories` table. The handler now
    //!    HOLDS `WsPools` and resolves `PgMemory` per-request, so (exactly like
    //!    `DocumentsHandler`, which has no unit tests for the same reason) these can no longer
    //!    inject an in-memory fake repo; they run against the same Postgres the
    //!    `memory_contract.rs` adapter test uses. Each test namespaces its rows by a nonce so
    //!    parallel/repeat runs don't collide.
    use super::*;

    /// The default workspace slug the per-request pool resolves to in tests.
    const WS: Option<&str> = Some("default");

    /// A handler over a pool cache that is never dialed — for the validation tests that
    /// short-circuit before resolving a pool. The URL is intentionally bogus.
    fn handler() -> MemoryHandler {
        MemoryHandler::new(Arc::new(WsPools::new("postgres://unused")), None)
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: WS,
            actor: "tester",
            args,
        }
    }

    #[test]
    fn namespace_is_memory() {
        assert_eq!(handler().namespace(), "memory");
    }

    #[test]
    fn advertises_seven_tools_in_meta_help() {
        let names: Vec<String> = handler()
            .descriptors()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "memory.save",
                "memory.recall",
                "memory.list",
                "memory.get",
                "memory.forget",
                "memory.clear",
                "memory.conflicts"
            ]
        );
    }

    #[tokio::test]
    async fn conflicts_threshold_is_validated() {
        let h = handler();
        // Out-of-range threshold → pure validation error (no pool touched).
        for bad in [json!(1.5), json!(-0.1), json!("x")] {
            let err = h
                .dispatch("memory.conflicts", ctx(json!({ "threshold": bad })))
                .await;
            assert!(matches!(err, Err(AppError::Validation(_))), "threshold {bad} rejected");
        }
    }

    #[tokio::test]
    async fn clear_requires_confirmation() {
        let h = handler();
        // No confirm → pure validation error (no pool touched), deletes nothing.
        let err = h.dispatch("memory.clear", ctx(json!({}))).await;
        assert!(matches!(err, Err(AppError::Validation(_))), "unconfirmed clear is rejected");
        let err = h
            .dispatch("memory.clear", ctx(json!({ "confirm": false, "kind": "project" })))
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))), "confirm:false is rejected");
        // A bad kind is still a pure validation error even with confirm:true.
        let err = h
            .dispatch("memory.clear", ctx(json!({ "confirm": true, "kind": "bogus" })))
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))), "invalid kind rejected");
    }

    #[tokio::test]
    async fn invalid_kind_is_rejected() {
        let h = handler();
        // On save — `SaveArgs::parse` validates before any pool is touched.
        let err = h
            .dispatch(
                "memory.save",
                ctx(json!({
                    "name": "x", "description": "d", "kind": "bogus", "body": "b",
                })),
            )
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))));
        // On recall's optional kind — likewise validated in `RecallArgs::parse`.
        let err = h
            .dispatch("memory.recall", ctx(json!({ "query": "q", "kind": "nope" })))
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))));
        // On list's optional kind — `parse_opt_kind` runs ahead of `self.repo(...)`.
        let err = h
            .dispatch("memory.list", ctx(json!({ "kind": "nope" })))
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))));
    }

    #[tokio::test]
    async fn unknown_tool_is_validation_error() {
        let h = handler();
        assert!(matches!(
            h.dispatch("memory.bogus", ctx(json!({}))).await,
            Err(AppError::Validation(_))
        ));
    }

    // ----- PG-backed behavioral tier (gated on GT_PG_URL) ---------------------------------

    use std::time::{SystemTime, UNIX_EPOCH};

    use gt_store_pg::{memory_migrations, WorkspacePool};

    /// Unique suffix so parallel/repeat runs against the same ephemeral DB never collide.
    fn nonce() -> u128 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    }

    /// Provision the `ws_default` `memories` table (idempotent migrations, serialized by an
    /// advisory lock) and hand back a multi-tenant handler over a real `WsPools`, or `None`
    /// when `GT_PG_URL` is unset (skip the gated test). Mirrors `memory_contract.rs`'
    /// `repo_or_skip`.
    async fn pg_handler_or_skip(test: &str) -> Option<MemoryHandler> {
        let Ok(url) = std::env::var("GT_PG_URL") else {
            eprintln!("GT_PG_URL unset; skipping {test}");
            return None;
        };
        let admin = sqlx::PgPool::connect(&url).await.expect("connect admin pool");
        let mut conn = admin.acquire().await.expect("acquire admin conn");
        sqlx::query("SELECT pg_advisory_lock(4915623002)")
            .execute(&mut *conn)
            .await
            .expect("take migration lock");
        for m in memory_migrations() {
            sqlx::raw_sql(&m.sql)
                .execute(&mut *conn)
                .await
                .expect("apply memory migration");
        }
        sqlx::query("SELECT pg_advisory_unlock(4915623002)")
            .execute(&mut *conn)
            .await
            .expect("release migration lock");
        // Prove the per-request pool resolution path: connect once to confirm the slug is
        // reachable, then hand the handler the cache itself (it resolves per dispatch).
        WorkspacePool::connect(&url, "default")
            .await
            .expect("connect ws pool");
        Some(MemoryHandler::new(Arc::new(WsPools::new(url)), None))
    }

    /// Save a memory through the handler, returning the name actually written (nonce-tagged).
    async fn save(h: &MemoryHandler, name: &str, kind: &str, text: &str) {
        h.dispatch(
            "memory.save",
            ctx(json!({
                "name": name,
                "description": text,
                "kind": kind,
                "body": text,
            })),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn save_then_recall_returns_the_row() {
        let Some(h) = pg_handler_or_skip("save_then_recall_returns_the_row").await else {
            return;
        };
        let n = nonce();
        let name = format!("use-https-{n}");
        save(&h, &name, "project", &format!("always-prefer-https-{n} term")).await;
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": format!("https-{n}") })))
            .await
            .unwrap();
        let names: Vec<&str> = out["memories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&name.as_str()), "the saved row must recall");
        // forget it so the shared default schema doesn't accrete across runs.
        h.dispatch("memory.forget", ctx(json!({ "name": name })))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn recall_always_includes_feedback_rules_and_dedups() {
        let Some(h) =
            pg_handler_or_skip("recall_always_includes_feedback_rules_and_dedups").await
        else {
            return;
        };
        let n = nonce();
        let fb = format!("verify-build-{n}");
        let proj = format!("naming-{n}");
        save(&h, &fb, "feedback", &format!("run-cargo-test-{n} before commit")).await;
        save(&h, &proj, "project", &format!("use-kebab-{n} for slugs")).await;

        // A query that only matches the `project` note must STILL return the feedback rule.
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": format!("kebab-{n}") })))
            .await
            .unwrap();
        let names: Vec<&str> = out["memories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&fb.as_str()), "feedback rule must always surface");
        assert!(names.contains(&proj.as_str()));

        // And a query that ALSO matches the feedback rule must not duplicate it.
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": format!("cargo-test-{n}") })))
            .await
            .unwrap();
        let fb_hits = out["memories"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["name"].as_str() == Some(fb.as_str()))
            .count();
        assert_eq!(fb_hits, 1, "feedback row must appear exactly once");

        h.dispatch("memory.forget", ctx(json!({ "name": fb }))).await.unwrap();
        h.dispatch("memory.forget", ctx(json!({ "name": proj }))).await.unwrap();
    }

    #[tokio::test]
    async fn list_filters_by_kind() {
        let Some(h) = pg_handler_or_skip("list_filters_by_kind").await else {
            return;
        };
        let n = nonce();
        let a = format!("a-{n}");
        let b = format!("b-{n}");
        save(&h, &a, "feedback", "x").await;
        save(&h, &b, "project", "y").await;

        let out = h
            .dispatch("memory.list", ctx(json!({ "kind": "project" })))
            .await
            .unwrap();
        let proj_names: Vec<&str> = out["memories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert!(proj_names.contains(&b.as_str()));
        assert!(!proj_names.contains(&a.as_str()), "feedback row must not show under project");

        h.dispatch("memory.forget", ctx(json!({ "name": a }))).await.unwrap();
        h.dispatch("memory.forget", ctx(json!({ "name": b }))).await.unwrap();
    }

    #[tokio::test]
    async fn forget_is_idempotent() {
        let Some(h) = pg_handler_or_skip("forget_is_idempotent").await else {
            return;
        };
        let n = nonce();
        let gone = format!("gone-{n}");
        save(&h, &gone, "project", "x").await;
        let out = h
            .dispatch("memory.forget", ctx(json!({ "name": gone })))
            .await
            .unwrap();
        assert_eq!(out["forgotten"], true);
        // Deleting an absent memory still succeeds (idempotent no-op).
        let out = h
            .dispatch("memory.forget", ctx(json!({ "name": format!("missing-{n}") })))
            .await
            .unwrap();
        assert_eq!(out["forgotten"], true);
    }
}
