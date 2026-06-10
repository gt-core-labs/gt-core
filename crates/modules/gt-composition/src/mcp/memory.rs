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
use gt_store_pg::{MemoryError, MemoryRepository, MemoryRow, NewMemory};

use super::util::{descriptor, opt, req, str_arg};

/// The `feedback` recall class: the hard operating rules `recall` always returns in full.
const FEEDBACK: &str = "feedback";

/// Default `recall` window for the relevance-ranked (non-feedback) tail.
const DEFAULT_LIMIT: i64 = 10;

/// Repository- (+ optional embedder-) backed handler for the `memory.*` tool namespace.
pub struct MemoryHandler {
    repo: Arc<dyn MemoryRepository>,
    /// Optional semantic-search engine. `Some` ⇒ memories are embedded on `save` and
    /// `recall` uses the hybrid (text + vector) path; `None` ⇒ full-text only. An embed
    /// failure degrades to full-text rather than erroring (mirror of `documents`).
    embedder: Option<Arc<dyn Embedder>>,
}

impl MemoryHandler {
    /// Wire the per-workspace memory repository and the optional embedding engine.
    pub fn new(repo: Arc<dyn MemoryRepository>, embedder: Option<Arc<dyn Embedder>>) -> Self {
        Self { repo, embedder }
    }

    /// `memory.save`: upsert by name, then best-effort embed. The embed never gates the
    /// write — a missing/failing embedder just leaves this row out of the vector index.
    async fn save(&self, cmd: SaveArgs) -> Result<Value, AppError> {
        let new = NewMemory {
            name: cmd.name.clone(),
            description: cmd.description.clone(),
            kind: cmd.kind.as_str().to_string(),
            body: cmd.body.clone(),
            created_by: cmd.created_by,
        };
        let row = self.repo.upsert(new).await.map_err(mem_err)?;
        // Best-effort embedding over name + description + body (mirror of
        // `DocumentsHandler::embed_doc`): a missing embedder or an embed/store failure is
        // silently skipped; the save already succeeded.
        if let Some(emb) = &self.embedder {
            let text = format!("{} {} {}", cmd.name, cmd.description, cmd.body);
            if !text.trim().is_empty() {
                if let Ok(vec) = emb.embed(&text).await {
                    let _ = self.repo.set_embedding(&cmd.name, vec).await;
                }
            }
        }
        Ok(mem_json(&row))
    }

    /// `memory.recall`: the full `feedback` rule set fused with the relevance-ranked top-k
    /// of everything else (or of the requested `kind`), deduplicated by `name`. See the
    /// module docs for why feedback is unconditional.
    async fn recall(&self, cmd: RecallArgs) -> Result<Value, AppError> {
        // The hard operating rules — always returned in full, regardless of the query.
        let mut out = self.repo.by_kind(FEEDBACK).await.map_err(mem_err)?;

        // The relevance-ranked tail: hybrid when an embedder is wired and the embed
        // succeeds, else full-text. An embed failure degrades to full-text, never errors.
        let kind = cmd.kind.as_deref();
        let ranked = match &self.embedder {
            Some(emb) => match emb.embed(&cmd.query).await {
                Ok(vec) => self
                    .repo
                    .recall_hybrid(&cmd.query, &vec, kind, cmd.limit)
                    .await
                    .map_err(mem_err)?,
                Err(_) => self
                    .repo
                    .recall(&cmd.query, kind, cmd.limit)
                    .await
                    .map_err(mem_err)?,
            },
            None => self
                .repo
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
                "memory.forget",
                "Hard-delete a memory by `name`. Idempotent — forgetting an absent memory \
                 is a successful no-op.",
                &[req("name", "string")],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        match tool {
            "memory.save" => {
                let cmd = SaveArgs::parse(&ctx)?;
                self.save(cmd).await
            }
            "memory.recall" => {
                let cmd = RecallArgs::parse(&ctx)?;
                self.recall(cmd).await
            }
            "memory.list" => {
                // `by_kind` when a (validated) kind is given, else the whole tenant.
                let rows = match parse_opt_kind(&ctx)? {
                    Some(kind) => self.repo.by_kind(kind.as_str()).await.map_err(mem_err)?,
                    None => self.repo.list().await.map_err(mem_err)?,
                };
                Ok(json!({
                    "count": rows.len(),
                    "memories": rows.iter().map(mem_json).collect::<Vec<_>>(),
                }))
            }
            "memory.forget" => {
                let name = str_arg(&ctx.args, "name")?;
                self.repo.forget(name).await.map_err(mem_err)?;
                Ok(json!({ "ok": true, "name": name, "forgotten": true }))
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
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use sqlx::types::chrono::Utc;

    /// An in-memory `MemoryRepository` fake — no Postgres. Keyed by `name`; `recall`/
    /// `recall_hybrid` do a naive substring match over name+description+body so the
    /// dispatch logic (the feedback union, dedup, kind filter) is exercised end to end.
    #[derive(Default)]
    struct FakeMemory {
        rows: Mutex<HashMap<String, MemoryRow>>,
        embeddings: Mutex<HashMap<String, Vec<f32>>>,
    }

    impl FakeMemory {
        fn seed(&self, name: &str, kind: &str, text: &str) {
            self.rows.lock().unwrap().insert(
                name.to_string(),
                MemoryRow {
                    name: name.to_string(),
                    description: text.to_string(),
                    kind: kind.to_string(),
                    body: text.to_string(),
                    version: 0,
                    created_by: "seed".into(),
                    updated_at: Utc::now(),
                },
            );
        }

        fn matches(&self, query: &str, kind: Option<&str>, limit: i64) -> Vec<MemoryRow> {
            let rows = self.rows.lock().unwrap();
            let mut hits: Vec<MemoryRow> = rows
                .values()
                .filter(|r| kind.is_none_or(|k| r.kind == k))
                .filter(|r| {
                    let hay = format!("{} {} {}", r.name, r.description, r.body).to_lowercase();
                    hay.contains(&query.to_lowercase())
                })
                .cloned()
                .collect();
            hits.sort_by(|a, b| a.name.cmp(&b.name));
            hits.truncate(limit as usize);
            hits
        }
    }

    #[async_trait]
    impl MemoryRepository for FakeMemory {
        async fn upsert(&self, mem: NewMemory) -> Result<MemoryRow, MemoryError> {
            let mut rows = self.rows.lock().unwrap();
            let version = rows.get(&mem.name).map(|r| r.version + 1).unwrap_or(0);
            let row = MemoryRow {
                name: mem.name.clone(),
                description: mem.description,
                kind: mem.kind,
                body: mem.body,
                version,
                created_by: mem.created_by,
                updated_at: Utc::now(),
            };
            rows.insert(mem.name, row.clone());
            Ok(row)
        }
        async fn get(&self, name: &str) -> Result<Option<MemoryRow>, MemoryError> {
            Ok(self.rows.lock().unwrap().get(name).cloned())
        }
        async fn by_kind(&self, kind: &str) -> Result<Vec<MemoryRow>, MemoryError> {
            let mut v: Vec<MemoryRow> = self
                .rows
                .lock()
                .unwrap()
                .values()
                .filter(|r| r.kind == kind)
                .cloned()
                .collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(v)
        }
        async fn list(&self) -> Result<Vec<MemoryRow>, MemoryError> {
            let mut v: Vec<MemoryRow> =
                self.rows.lock().unwrap().values().cloned().collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(v)
        }
        async fn recall(
            &self,
            query: &str,
            kind: Option<&str>,
            limit: i64,
        ) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(self.matches(query, kind, limit))
        }
        async fn set_embedding(
            &self,
            name: &str,
            embedding: Vec<f32>,
        ) -> Result<(), MemoryError> {
            self.embeddings
                .lock()
                .unwrap()
                .insert(name.to_string(), embedding);
            Ok(())
        }
        async fn recall_hybrid(
            &self,
            query: &str,
            _query_embedding: &[f32],
            kind: Option<&str>,
            limit: i64,
        ) -> Result<Vec<MemoryRow>, MemoryError> {
            Ok(self.matches(query, kind, limit))
        }
        async fn forget(&self, name: &str) -> Result<(), MemoryError> {
            self.rows.lock().unwrap().remove(name);
            Ok(())
        }
    }

    fn handler() -> MemoryHandler {
        MemoryHandler::new(Arc::new(FakeMemory::default()), None)
    }

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: Some("default"),
            actor: "tester",
            args,
        }
    }

    #[test]
    fn namespace_is_memory() {
        assert_eq!(handler().namespace(), "memory");
    }

    #[test]
    fn advertises_four_tools_in_meta_help() {
        let names: Vec<String> = handler()
            .descriptors()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(
            names,
            vec!["memory.save", "memory.recall", "memory.list", "memory.forget"]
        );
    }

    #[tokio::test]
    async fn save_then_recall_returns_the_row() {
        let h = handler();
        h.dispatch(
            "memory.save",
            ctx(json!({
                "name": "use-https",
                "description": "always prefer https",
                "kind": "project",
                "body": "never call plain http endpoints",
            })),
        )
        .await
        .unwrap();
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": "https" })))
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["memories"][0]["name"], "use-https");
        assert_eq!(out["memories"][0]["version"], 0);
    }

    #[tokio::test]
    async fn recall_always_includes_feedback_rules_and_dedups() {
        let repo = Arc::new(FakeMemory::default());
        repo.seed("verify-build", "feedback", "run cargo test before commit");
        repo.seed("naming", "project", "use kebab-case for slugs");
        let h = MemoryHandler::new(repo, None);

        // A query that only matches the `project` note must STILL return the feedback rule.
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": "kebab" })))
            .await
            .unwrap();
        let names: Vec<&str> = out["memories"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"verify-build"), "feedback rule must always surface");
        assert!(names.contains(&"naming"));
        assert_eq!(out["count"], 2);

        // And a query that ALSO matches the feedback rule must not duplicate it.
        let out = h
            .dispatch("memory.recall", ctx(json!({ "query": "cargo" })))
            .await
            .unwrap();
        assert_eq!(out["count"], 1, "feedback row must appear exactly once");
    }

    #[tokio::test]
    async fn list_filters_by_kind() {
        let repo = Arc::new(FakeMemory::default());
        repo.seed("a", "feedback", "x");
        repo.seed("b", "project", "y");
        let h = MemoryHandler::new(repo, None);
        let out = h
            .dispatch("memory.list", ctx(json!({ "kind": "project" })))
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
        assert_eq!(out["memories"][0]["name"], "b");
        // No kind ⇒ everything.
        let out = h.dispatch("memory.list", ctx(json!({}))).await.unwrap();
        assert_eq!(out["count"], 2);
    }

    #[tokio::test]
    async fn forget_is_idempotent() {
        let repo = Arc::new(FakeMemory::default());
        repo.seed("gone", "project", "x");
        let h = MemoryHandler::new(repo, None);
        let out = h
            .dispatch("memory.forget", ctx(json!({ "name": "gone" })))
            .await
            .unwrap();
        assert_eq!(out["forgotten"], true);
        // Deleting an absent memory still succeeds.
        let out = h
            .dispatch("memory.forget", ctx(json!({ "name": "missing" })))
            .await
            .unwrap();
        assert_eq!(out["forgotten"], true);
    }

    #[tokio::test]
    async fn invalid_kind_is_rejected() {
        let h = handler();
        // On save.
        let err = h
            .dispatch(
                "memory.save",
                ctx(json!({
                    "name": "x", "description": "d", "kind": "bogus", "body": "b",
                })),
            )
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))));
        // On recall's optional kind.
        let err = h
            .dispatch("memory.recall", ctx(json!({ "query": "q", "kind": "nope" })))
            .await;
        assert!(matches!(err, Err(AppError::Validation(_))));
        // On list's optional kind.
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
}
