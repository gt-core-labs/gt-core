//! `graph.*` domain dispatch — read-only knowledge-graph queries (`hq-graphrig.10`).
//!
//! This is the surface other agents use to **consult** a rig's codebase graph for
//! context; only the warden writes (freshness state + the index itself). The
//! handler is tool-neutral: it talks to a [`GraphIndexer`] trait object, so the
//! backing tool (graphify today) is swapped by constructing a different adapter at
//! the composition edge — no change here.
//!
//! Per-rig routing: the repo a query runs against is resolved by replaying the
//! warden's events from the workspace log into a [`WardenState`] and reading the
//! rig's `repo_dir`. So a `graph.query` only works for a rig the warden has under
//! custody (`graphwarden.rig-registered.v1`) — exactly the rigs whose graphs exist.
//!
//! Server-side provisioning ([`RigProvisioner`], hq-vcs-connections.4): `graph.refresh`
//! no longer trusts a caller-supplied `repo_dir` (the footgun behind the inactivas-chain
//! incident — a caller passed a path from *their* host that did not exist in the container,
//! so the refresh "succeeded" over an empty/absent checkout with commit `unknown`). The
//! server now DERIVES the path (`<root>/<ws>/<rig>/`, root = `GT_GRAPH_ROOT` ||
//! `/var/lib/gt-graph`) and, when a rig is bound to a VCS connection, mints a JIT GitHub App
//! installation token, clones (or fetches+resets) the rig's `default_branch` into that path,
//! then indexes — discarding the token. Custody stays keyed by `rig` (default-branch-only; no
//! `(rig, branch)` dimension). For a rig already under custody whose stored path is dead/stale
//! (the inactivas-chain case), the handler Unregisters + re-registers it at the derived path so
//! custody and the build never diverge again.
//!
//! Tools (read-only): `graph.query`, `graph.explain`, `graph.status`, `graph.list`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use gt_graphindex::{ensure_ignored, GraphError, GraphIndexer, IndexStats};
use gt_graphwarden::{MarkRefreshed, RegisterRig, Unregister, WardenCommand, WardenState};
use gt_mcp_server::{DomainCtx, DomainHandler};
use gt_module::McpTool;
use gt_rig::{PgRigs, RigEntry, RigRepository};
use gt_store_dolt::AppError;
use gt_vcs::{ConnectionStatus, GithubAppClient, VcsConnectionRepo};

use super::eventlog::EventLog;
use super::pools::WsPools;
use super::util::{descriptor, now_secs, req, str_arg};

/// The warden event-log kind prefix the handler replays to resolve rigs.
const NS: &str = "graphwarden.";

/// The workspace a refresh resolves to for the derived path when the request carries no
/// workspace — the single-tenant default (mirrors `WsPools`' `DEFAULT_WORKSPACE`).
const DEFAULT_WORKSPACE: &str = "default";

/// Default root the server clones rig checkouts under. A managed Docker volume
/// (`gt-graph-rigs:/var/lib/gt-graph`) is mounted here on the gt-mcp-server — NEVER a host
/// bind-mount (epic decision). Overridable via `GT_GRAPH_ROOT` for tests / alternate deploys.
const GRAPH_ROOT_DEFAULT: &str = "/var/lib/gt-graph";

/// The configured graph-checkout root: `GT_GRAPH_ROOT` if set, else [`GRAPH_ROOT_DEFAULT`].
fn graph_root() -> PathBuf {
    std::env::var("GT_GRAPH_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(GRAPH_ROOT_DEFAULT))
}

/// Server-side rig checkout provisioner (hq-vcs-connections.4).
///
/// Resolves a rig to its VCS connection, mints a JIT installation token, and clones/fetches the
/// rig's default branch into the server-derived path — so the warden indexes a real checkout the
/// server controls, never a caller-named path. Optional on [`GraphHandler`]: without it (or
/// without a GitHub App configured / a rig bound to a connection) the handler falls back to
/// indexing whatever already exists at the derived path (the legacy operator-mounted rigs).
#[derive(Clone)]
pub struct RigProvisioner {
    /// Per-workspace pool cache — the rig catalog (`ws_<slug>.rigs`) is per-tenant.
    pools: Arc<WsPools>,
    /// The GLOBAL `public.vcs_connections` store (one repo serves every tenant; rows carry
    /// `workspace_id`).
    connections: Arc<dyn VcsConnectionRepo>,
    /// The platform GitHub App client used to mint installation tokens. `None` when no App is
    /// configured — provisioning then degrades to "index whatever is on disk".
    github: Option<GithubAppClient>,
}

impl RigProvisioner {
    /// Wire the provisioner from the same parts the binary already holds.
    pub fn new(
        pools: Arc<WsPools>,
        connections: Arc<dyn VcsConnectionRepo>,
        github: Option<GithubAppClient>,
    ) -> Self {
        Self {
            pools,
            connections,
            github,
        }
    }

    /// Fetch the rig catalog entry for `rig` in `ws`.
    async fn rig_entry(&self, ws: Option<&str>, rig: &str) -> Result<RigEntry, AppError> {
        let pool = self.pools.get(ws).await?;
        let repo = PgRigs::new(pool.pool().clone());
        repo.get(rig)
            .await
            .map_err(domain_err)?
            .ok_or_else(|| AppError::NotFound(format!("rig `{rig}` not in the catalog")))
    }

    /// Provision the on-disk checkout for `rig` at the derived `path`: if the rig is bound to an
    /// active GitHub App connection, mint a JIT token and clone (absent) or fetch+reset (present)
    /// its `default_branch`. The token lives only for the duration of the git command and is
    /// dropped. A rig WITHOUT a connection is treated as a public repo and cloned/fetched over
    /// HTTPS with no credentials (the host is normalized to github.com), so the refresh is
    /// self-sufficient instead of relying on a hand-mounted checkout.
    async fn provision(&self, ws: Option<&str>, rig: &str, path: &Path) -> Result<(), AppError> {
        let entry = self.rig_entry(ws, rig).await?;
        let Some(conn_ref) = entry.git_connection_ref.as_deref() else {
            // No VCS connection bound — public-repo path: clone/fetch the repo directly over HTTPS
            // (no token). Normalizing to HTTPS also rescues SSH-form git_urls, since the runtime
            // image ships no ssh client. This replaces the old "index whatever is on disk" branch
            // that forced an operator to hand-mount a checkout (and silently built an empty graph
            // when none existed).
            let (owner, repo_name) = split_git_url(&entry.git_url).ok_or_else(|| {
                AppError::Validation(format!(
                    "cannot derive owner/repo from rig `{rig}` git_url `{}`",
                    entry.git_url
                ))
            })?;
            let public_url = format!("https://github.com/{owner}/{repo_name}.git");
            let branch = &entry.default_branch;
            if path.join(".git").is_dir() {
                fetch_reset(path, &public_url, branch)?;
            } else {
                clone_branch(&public_url, branch, path)?;
            }
            return Ok(());
        };
        let Some(github) = &self.github else {
            return Err(AppError::Validation(format!(
                "rig `{rig}` is bound to a VCS connection but no GitHub App is configured \
                 (set GT_GITHUB_APP_* on the server)"
            )));
        };
        let conn = self
            .connections
            .get_for_workspace(ws.unwrap_or(DEFAULT_WORKSPACE), conn_ref)
            .await
            .map_err(domain_err)?
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "rig `{rig}` references VCS connection `{conn_ref}` not visible to this workspace"
                ))
            })?;
        if conn.status != ConnectionStatus::Active {
            return Err(AppError::Validation(format!(
                "VCS connection `{conn_ref}` for rig `{rig}` is not active ({})",
                conn.status.as_str()
            )));
        }
        let installation_id = conn.installation_id.clone().ok_or_else(|| {
            AppError::Validation(format!(
                "VCS connection `{conn_ref}` has no installation_id (PAT fallback not yet \
                 supported in the clone path)"
            ))
        })?;
        let (owner, repo_name) = split_git_url(&entry.git_url).ok_or_else(|| {
            AppError::Validation(format!(
                "cannot derive owner/repo from rig `{rig}` git_url `{}`",
                entry.git_url
            ))
        })?;

        // Mint the JIT token, run the git command with it embedded, then drop it — never stored.
        let token = github
            .installation_token(&installation_id)
            .await
            .map_err(domain_err)?;
        let clone_url = token.clone_url(&owner, &repo_name);
        let branch = &entry.default_branch;
        let has_checkout = path.join(".git").is_dir();
        if has_checkout {
            fetch_reset(path, &clone_url, branch)?;
        } else {
            clone_branch(&clone_url, branch, path)?;
        }
        drop(token);
        Ok(())
    }
}

/// Read-only handler for the `graph.*` tool namespace.
pub struct GraphHandler {
    log: Arc<EventLog>,
    indexer: Arc<dyn GraphIndexer>,
    /// Server-side checkout provisioner. `None` in tests / minimal deploys; the handler then
    /// indexes whatever sits at the derived path.
    provisioner: Option<RigProvisioner>,
}

impl GraphHandler {
    /// Wrap the per-workspace event log + the active graph indexer, with no provisioner (the
    /// derived path is indexed as-is). Used by tests and minimal deploys.
    pub fn new(log: Arc<EventLog>, indexer: Arc<dyn GraphIndexer>) -> Self {
        Self {
            log,
            indexer,
            provisioner: None,
        }
    }

    /// Attach the server-side [`RigProvisioner`] so `graph.refresh` clones/fetches from the rig's
    /// VCS connection before indexing.
    pub fn with_provisioner(mut self, provisioner: RigProvisioner) -> Self {
        self.provisioner = Some(provisioner);
        self
    }

    /// The server-DERIVED checkout path for `rig` in `ws`: `<GRAPH_ROOT>/<ws>/<rig>`. This is the
    /// single source of truth for both custody and the build — never a caller-supplied path.
    fn derived_path(ws: Option<&str>, rig: &str) -> PathBuf {
        graph_root().join(ws.unwrap_or(DEFAULT_WORKSPACE)).join(rig)
    }

    /// Replay the warden events for `ws` into a fresh [`WardenState`].
    fn warden(&self, ws: Option<&str>) -> Result<WardenState, AppError> {
        self.log
            .replay_domain(ws, NS, WardenState::default(), |s, e| {
                let _ = s.apply(e);
            })
    }

    /// The on-disk checkout for `rig`, or `NotFound` if the warden has no custody of it.
    fn repo_dir(&self, ws: Option<&str>, rig: &str) -> Result<PathBuf, AppError> {
        let state = self.warden(ws)?;
        state
            .rigs
            .get(rig)
            .map(|g| PathBuf::from(&g.repo_dir))
            .ok_or_else(|| AppError::NotFound(format!("no graph custody for rig `{rig}`")))
    }

    /// Run a warden command against the replayed state and append its events to the log,
    /// so the next replay (read or refresh) sees the new freshness state.
    fn warden_apply(&self, ws: Option<&str>, cmd: WardenCommand) -> Result<(), AppError> {
        let state = self.warden(ws)?;
        let events = cmd.execute(&state).map_err(ev_err)?;
        for ev in events {
            self.log.append(ws, ev)?;
        }
        Ok(())
    }

    /// Bring custody for `rig` into agreement with the server-derived `path` and register it if
    /// absent. The custody record (used by `graph.query`/`status`/`list`) and the build MUST point
    /// at the SAME derived path — the inactivas-chain incident left a rig under custody at a dead
    /// caller path while a later build wrote elsewhere. If the stored `repo_dir` diverges from
    /// `path`, Unregister + re-register cleanly at the derived path (custody stays keyed by `rig`).
    fn ensure_custody(&self, ws: Option<&str>, rig: &str, path: &Path) -> Result<(), AppError> {
        let derived = path.to_string_lossy().into_owned();
        let state = self.warden(ws)?;
        match state.rigs.get(rig) {
            Some(g) if g.repo_dir == derived => Ok(()),
            Some(_) => {
                // Poisoned custody (a dead/old path from the caller-repo_dir era): drop it and
                // re-register at the derived path so the next read resolves the real checkout.
                self.warden_apply(
                    ws,
                    WardenCommand::Unregister(Unregister {
                        rig: rig.to_string(),
                        now_secs: now_secs(),
                    }),
                )?;
                self.warden_apply(
                    ws,
                    WardenCommand::Register(RegisterRig {
                        rig: rig.to_string(),
                        repo_dir: derived,
                        now_secs: now_secs(),
                    }),
                )
            }
            None => self.warden_apply(
                ws,
                WardenCommand::Register(RegisterRig {
                    rig: rig.to_string(),
                    repo_dir: derived,
                    now_secs: now_secs(),
                }),
            ),
        }
    }

    /// Ensure-ignore, build-or-update `repo`'s graph, and record the refresh on the warden
    /// log. The rig must already be under custody (caller registers first). Shared by the
    /// single-rig `graph.refresh` and the batch `graph.refresh-stale`.
    async fn refresh_one(
        &self,
        ws: Option<&str>,
        rig: &str,
        repo: &std::path::Path,
    ) -> Result<(String, IndexStats), AppError> {
        let _ = ensure_ignored(repo, self.indexer.tool());
        let built = self.indexer.status(repo).await.map_err(graph_err)?.built;
        let stats = if built {
            self.indexer
                .update(repo, &[])
                .await
                .map_err(graph_err)?
                .after
        } else {
            self.indexer.build(repo).await.map_err(graph_err)?
        };
        let commit = head_commit(repo);
        self.warden_apply(
            ws,
            WardenCommand::MarkRefreshed(MarkRefreshed {
                rig: rig.to_string(),
                commit: commit.clone(),
                now_secs: now_secs(),
            }),
        )?;
        Ok((commit, stats))
    }
}

/// Map the warden custody's `(stale, ever_indexed)` onto the freshness state the FE chip renders.
/// Derived purely from custody so it is replay-deterministic (no network call on a status read):
///   - `built`  — not stale: the indexed commit is current.
///   - `behind` — stale AND a graph was indexed before: it has fallen behind origin (a known newer
///     tip exists — the merge queue, push webhook, or drift-reconcile flipped `stale`).
///   - `stale`  — stale AND never indexed: the initial build is still owed.
fn freshness_state(stale: bool, ever_indexed: bool) -> &'static str {
    match (stale, ever_indexed) {
        (false, _) => "built",
        (true, true) => "behind",
        (true, false) => "stale",
    }
}

/// `git -C <repo> rev-parse --short HEAD`, or `"unknown"` if it cannot be read — the warden
/// requires a non-empty commit and an unknown checkout still refreshes the graph.
fn head_commit(repo: &std::path::Path) -> String {
    std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Derive `(owner, repo)` from a rig's `git_url`, accepting both the SSH (`git@github.com:owner/
/// repo.git`) and HTTPS (`https://github.com/owner/repo.git`) spellings. `.git` and a trailing
/// slash are trimmed. `None` when no `owner/repo` pair can be read.
fn split_git_url(git_url: &str) -> Option<(String, String)> {
    let url = git_url.trim();
    // Strip the scheme/host prefix down to the `owner/repo[.git]` tail.
    let tail = if let Some(rest) = url.strip_prefix("git@") {
        // git@github.com:owner/repo.git -> owner/repo.git
        rest.split_once(':').map(|(_, p)| p)?
    } else if let Some(rest) = url.strip_prefix("https://") {
        // https://github.com/owner/repo.git -> owner/repo.git
        rest.split_once('/').map(|(_, p)| p)?
    } else if let Some(rest) = url.strip_prefix("http://") {
        rest.split_once('/').map(|(_, p)| p)?
    } else if let Some(rest) = url.strip_prefix("ssh://") {
        // ssh://git@github.com/owner/repo.git -> owner/repo.git
        let after_host = rest.split_once('/').map(|(_, p)| p)?;
        after_host
    } else {
        url
    };
    let tail = tail.trim_end_matches('/').trim_end_matches(".git");
    let (owner, repo) = tail.rsplit_once('/')?;
    // owner may itself carry a leading host segment for ssh:// (host/owner/repo) — keep the last.
    let owner = owner.rsplit('/').next().unwrap_or(owner);
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some((owner.to_string(), repo.to_string()))
}

/// `git clone --branch <branch> --single-branch <url> <path>` (the token is embedded in `url`).
/// The parent dir is created first so a fresh managed volume works on the first refresh.
fn clone_branch(url: &str, branch: &str, path: &Path) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| AppError::Other(format!("create graph checkout root {parent:?}: {e}")))?;
    }
    let out = std::process::Command::new("git")
        .args(["clone", "--branch", branch, "--single-branch"])
        .arg(url)
        .arg(path)
        .output()
        .map_err(|e| AppError::Other(format!("git clone spawn failed: {e}")))?;
    if !out.status.success() {
        // Never echo the URL (it carries the token); only git's stderr.
        return Err(AppError::Other(format!(
            "git clone of branch `{branch}` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// Refresh an existing checkout to `origin/<branch>`: set the remote (token-bearing URL), fetch,
/// then `reset --hard`. Idempotent — discards any local drift so the graph reflects the remote.
fn fetch_reset(path: &Path, url: &str, branch: &str) -> Result<(), AppError> {
    // Point `origin` at the freshly-minted token URL (a stale token from a prior clone would be
    // baked into the remote otherwise).
    run_git(path, &["remote", "set-url", "origin", url])?;
    run_git(path, &["fetch", "--prune", "origin", branch])?;
    run_git(path, &["reset", "--hard", &format!("origin/{branch}")])?;
    Ok(())
}

/// Run `git -C <path> <args>`; map a non-zero exit to [`AppError::Other`] (without echoing the
/// arguments, which may carry the token URL).
fn run_git(path: &Path, args: &[&str]) -> Result<(), AppError> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .map_err(|e| AppError::Other(format!("git spawn failed: {e}")))?;
    if !out.status.success() {
        let verb = args.first().copied().unwrap_or("git");
        return Err(AppError::Other(format!(
            "git {verb} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

/// Map a `gt_events::AppError` (warden command path) onto the server error type.
fn ev_err(e: gt_events::AppError) -> AppError {
    AppError::Validation(e.to_string())
}

/// Map a `gt_events::AppError` from a domain port (rig catalog / vcs connections / GitHub client)
/// onto the server error type, PRESERVING the variant so a missing rig stays `NotFound`, a bad
/// connection stays `Validation`, and a backend fault stays `Other`.
fn domain_err(e: gt_events::AppError) -> AppError {
    match e {
        gt_events::AppError::NotFound(m) => AppError::NotFound(m),
        gt_events::AppError::Validation(m) => AppError::Validation(m),
        gt_events::AppError::InvalidTransition(m) => AppError::InvalidTransition(m),
        other => AppError::Other(other.to_string()),
    }
}

/// Map an indexer failure onto the MCP-server error type.
fn graph_err(e: GraphError) -> AppError {
    match e {
        GraphError::NotBuilt(r) => AppError::NotFound(format!("graph not built: {r}")),
        other => AppError::Validation(other.to_string()),
    }
}

#[async_trait]
impl DomainHandler for GraphHandler {
    fn namespace(&self) -> &'static str {
        "graph"
    }

    fn descriptors(&self) -> Vec<McpTool> {
        vec![
            descriptor(
                "graph.query",
                "Ask a natural-language question against a rig's codebase knowledge graph.",
                &[req("rig", "string"), req("question", "string")],
            ),
            descriptor(
                "graph.explain",
                "Explain one node (crate/concept) in a rig's knowledge graph.",
                &[req("rig", "string"), req("node", "string")],
            ),
            descriptor(
                "graph.status",
                "Report a rig's graph freshness + index stats.",
                &[req("rig", "string")],
            ),
            descriptor(
                "graph.refresh",
                "Clone/fetch a rig from its VCS connection (server-derived path) and rebuild its \
                 knowledge graph.",
                &[req("rig", "string")],
            ),
            descriptor(
                "graph.refresh-stale",
                "Rebuild every rig whose graph the warden marked stale.",
                &[],
            ),
            descriptor(
                "graph.list",
                "List the rigs under warden custody with their freshness.",
                &[],
            ),
        ]
    }

    async fn dispatch(&self, tool: &str, ctx: DomainCtx<'_>) -> Result<Value, AppError> {
        let ws = ctx.workspace;
        match tool {
            "graph.query" => {
                let rig = str_arg(&ctx.args, "rig")?;
                let question = str_arg(&ctx.args, "question")?;
                let repo = self.repo_dir(ws, rig)?;
                let ans = self
                    .indexer
                    .query(&repo, question)
                    .await
                    .map_err(graph_err)?;
                Ok(json!({ "text": ans.text, "nodes": ans.nodes }))
            }
            "graph.explain" => {
                let rig = str_arg(&ctx.args, "rig")?;
                let node = str_arg(&ctx.args, "node")?;
                let repo = self.repo_dir(ws, rig)?;
                let ans = self.indexer.explain(&repo, node).await.map_err(graph_err)?;
                Ok(json!({ "text": ans.text, "nodes": ans.nodes }))
            }
            "graph.status" => {
                // Freshness is sourced from the WARDEN CUSTODY (the event-sourced record
                // `graph.list` reads), NOT from `indexer.status().built_at_commit` — the graphify
                // build never stamps the commit into its artifacts, so that field is always null
                // (live finding, hq-vcs-connections.9). The custody's `last_indexed_commit` is the
                // commit MarkRefreshed recorded; its `stale` flag is flipped by the three freshness
                // sources (merge queue, push webhook, drift-reconcile). Index size still comes from
                // the indexer (it owns the artifacts), but the commit + freshness are the warden's.
                let rig = str_arg(&ctx.args, "rig")?;
                let state = self.warden(ws)?;
                let g = state.rigs.get(rig).ok_or_else(|| {
                    AppError::NotFound(format!("no graph custody for rig `{rig}`"))
                })?;
                let repo = PathBuf::from(&g.repo_dir);
                let st = self.indexer.status(&repo).await.map_err(graph_err)?;
                Ok(json!({
                    "built": st.built,
                    "tool": st.tool,
                    "nodes": st.stats.map(|s| s.nodes),
                    "edges": st.stats.map(|s| s.edges),
                    "communities": st.stats.map(|s| s.communities),
                    // The indexed commit comes from the warden custody (`graph.list`'s source), not
                    // the always-null indexer metadata. `built_at_commit` kept as an alias for
                    // back-compat with any existing reader.
                    "last_indexed_commit": g.last_indexed_commit,
                    "built_at_commit": g.last_indexed_commit,
                    "stale": g.stale,
                    "pending_changes": g.pending_changes,
                    // Freshness state, derived purely from custody (replay-deterministic, no network):
                    //   built  — not stale: the indexed commit is current.
                    //   behind — stale AND a graph was indexed before: it has fallen behind origin
                    //            (a known newer tip exists; the merge/webhook/drift source flipped it).
                    //   stale  — stale AND never indexed: the initial build is owed.
                    "state": freshness_state(g.stale, g.last_indexed_commit.is_some()),
                }))
            }
            "graph.refresh" => {
                // The custodian's write trigger. The server DERIVES the checkout path (no caller
                // repo_dir — that footgun caused the inactivas-chain incident): provision the
                // checkout from the rig's VCS connection (clone/fetch+reset the default branch),
                // align custody with the derived path, build-or-update the graph, record the
                // refresh. Idempotent — re-running fetch+resets and updates in place.
                let rig = str_arg(&ctx.args, "rig")?;
                let repo = Self::derived_path(ws, rig);
                if let Some(provisioner) = &self.provisioner {
                    provisioner.provision(ws, rig, &repo).await?;
                } else {
                    // No provisioner (tests / minimal deploy): still create the dir so a build over
                    // an empty checkout does not fail spuriously, then index in place.
                    std::fs::create_dir_all(&repo).map_err(|e| {
                        AppError::Other(format!("create derived checkout {repo:?}: {e}"))
                    })?;
                }
                // Custody + build point at the SAME derived path (poisoned custody is re-pointed).
                self.ensure_custody(ws, rig, &repo)?;
                let (commit, stats) = self.refresh_one(ws, rig, &repo).await?;
                Ok(json!({
                    "ok": true,
                    "rig": rig,
                    "repo_dir": repo.to_string_lossy(),
                    "commit": commit,
                    "nodes": stats.nodes,
                    "edges": stats.edges,
                    "communities": stats.communities,
                }))
            }
            "graph.refresh-stale" => {
                // The custodian's batch tick: refresh every rig currently marked stale. A
                // loop/cron invokes this; `graph.agent.backend` is who runs the loop. Each stale
                // rig is re-provisioned from its connection at the server-derived path (the stored
                // repo_dir is authoritative — it already equals the derived path after a prior
                // refresh).
                let state = self.warden(ws)?;
                let stale: Vec<String> = state
                    .rigs
                    .values()
                    .filter(|g| g.stale)
                    .map(|g| g.rig.clone())
                    .collect();
                let mut refreshed = Vec::new();
                for rig in stale {
                    let repo = Self::derived_path(ws, &rig);
                    if let Some(provisioner) = &self.provisioner {
                        provisioner.provision(ws, &rig, &repo).await?;
                    }
                    self.ensure_custody(ws, &rig, &repo)?;
                    let (commit, stats) = self.refresh_one(ws, &rig, &repo).await?;
                    refreshed.push(json!({ "rig": rig, "commit": commit, "nodes": stats.nodes }));
                }
                Ok(json!({ "ok": true, "refreshed": refreshed }))
            }
            "graph.list" => {
                let state = self.warden(ws)?;
                let rigs: Vec<Value> = state
                    .rigs
                    .values()
                    .map(|g| {
                        json!({
                            "rig": g.rig,
                            "repo_dir": g.repo_dir,
                            "stale": g.stale,
                            "pending_changes": g.pending_changes,
                            "last_indexed_commit": g.last_indexed_commit,
                        })
                    })
                    .collect();
                Ok(json!({ "rigs": rigs }))
            }
            other => Err(AppError::Validation(format!("unknown tool `{other}`"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_graphindex::InMemoryGraphIndexer;
    use gt_graphwarden::WardenEvent;
    use tempfile::TempDir;

    fn ctx(args: Value) -> DomainCtx<'static> {
        DomainCtx {
            workspace: None,
            actor: "tester",
            args,
        }
    }

    /// Serializes the tests that mutate the process-global `GT_GRAPH_ROOT` env var, so two of them
    /// running concurrently (cargo runs tests in parallel) cannot clobber each other's pinned root.
    static GRAPH_ROOT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Pin `GT_GRAPH_ROOT` at a fresh temp dir so the derived path is writable in tests (the
    /// production default `/var/lib/gt-graph` is not). Returns `(dir, guard)`: the dir keeps the
    /// checkout alive and the [`GRAPH_ROOT_LOCK`] guard serializes against the other env-mutating
    /// tests — hold both for the test body. Recovers from a poisoned lock (a prior test panic) so a
    /// single failure does not cascade.
    fn pin_graph_root() -> (TempDir, std::sync::MutexGuard<'static, ()>) {
        let guard = GRAPH_ROOT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        std::env::set_var("GT_GRAPH_ROOT", dir.path());
        (dir, guard)
    }

    #[test]
    fn split_git_url_ssh_and_https() {
        assert_eq!(
            split_git_url("git@github.com:codecsrayo/inactivas-chain.git"),
            Some(("codecsrayo".into(), "inactivas-chain".into()))
        );
        assert_eq!(
            split_git_url("https://github.com/codecsrayo/inactivas-chain.git"),
            Some(("codecsrayo".into(), "inactivas-chain".into()))
        );
        assert_eq!(
            split_git_url("https://github.com/acme/repo"),
            Some(("acme".into(), "repo".into()))
        );
        assert_eq!(
            split_git_url("ssh://git@github.com/acme/repo.git"),
            Some(("acme".into(), "repo".into()))
        );
        assert_eq!(split_git_url("not-a-url"), None);
    }

    #[test]
    fn derived_path_is_root_ws_rig() {
        let _g = pin_graph_root();
        let p = GraphHandler::derived_path(Some("confiar"), "inactivas-chain");
        assert!(p.ends_with("confiar/inactivas-chain"));
        let d = GraphHandler::derived_path(None, "hq");
        assert!(d.ends_with("default/hq"));
    }

    /// Seed the workspace log with a rig under custody, then query/list/status it.
    #[tokio::test]
    async fn query_resolves_rig_and_answers() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        // The warden registered rig `alpha` pointing at a repo dir.
        log.append(
            None,
            WardenEvent::RigRegistered {
                rig: "alpha".into(),
                repo_dir: "/repo/alpha".into(),
                now_secs: 1,
            },
        )
        .unwrap();

        let indexer = Arc::new(InMemoryGraphIndexer::new());
        // InMemory needs a built graph for that repo to answer.
        indexer
            .build(std::path::Path::new("/repo/alpha"))
            .await
            .unwrap();

        let h = GraphHandler::new(log, indexer);

        let out = h
            .dispatch(
                "graph.query",
                ctx(json!({ "rig": "alpha", "question": "where is auth" })),
            )
            .await
            .unwrap();
        assert!(out["text"].as_str().unwrap().contains("where is auth"));

        let list = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["rigs"].as_array().unwrap().len(), 1);
        assert_eq!(list["rigs"][0]["rig"], "alpha");

        let st = h
            .dispatch("graph.status", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        assert_eq!(st["built"], true);
        assert_eq!(st["tool"], "inmemory");
        // Freshness is sourced from the warden custody, not the (null) indexer metadata. This rig
        // was registered but never MarkRefreshed, so custody is stale + never-indexed → `stale`,
        // and `last_indexed_commit` is null (NOT the indexer's always-null `built_at_commit`).
        assert_eq!(st["state"], "stale");
        assert_eq!(st["stale"], true);
        assert!(st["last_indexed_commit"].is_null());
    }

    /// `graph.status` reads the indexed commit + freshness from the WARDEN CUSTODY (the source
    /// `graph.list` uses), not from the always-null `indexer.status().built_at_commit`. After a
    /// refresh the state is `built` and the commit is the one MarkRefreshed recorded; a later
    /// MarkStale (webhook/merge/drift) flips it to `behind` while keeping the last indexed commit.
    #[tokio::test]
    async fn status_sources_commit_and_freshness_from_warden() {
        let _root = pin_graph_root();
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log.clone(), Arc::new(InMemoryGraphIndexer::new()));

        // Refresh registers + builds + MarkRefreshed → custody carries a real commit, not stale.
        h.dispatch("graph.refresh", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        let commit = h
            .dispatch("graph.list", ctx(json!({})))
            .await
            .unwrap()["rigs"][0]["last_indexed_commit"]
            .clone();
        assert!(commit.is_string(), "list must carry the indexed commit");

        let st = h
            .dispatch("graph.status", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        assert_eq!(st["state"], "built");
        assert_eq!(st["stale"], false);
        // The commit matches what graph.list reports — both from the warden custody.
        assert_eq!(st["last_indexed_commit"], commit);
        assert_eq!(st["built_at_commit"], commit, "back-compat alias");

        // A push webhook / merge / drift marks the rig stale: it has been indexed before, so the
        // freshness state is `behind` (not `stale`), and the last indexed commit is retained.
        log.append(
            None,
            WardenEvent::MarkedStale {
                rig: "alpha".into(),
                changed: 3,
                now_secs: 2,
            },
        )
        .unwrap();
        let behind = h
            .dispatch("graph.status", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        assert_eq!(behind["state"], "behind");
        assert_eq!(behind["stale"], true);
        assert_eq!(behind["pending_changes"], 3);
        assert_eq!(behind["last_indexed_commit"], commit, "commit retained while behind");
    }

    #[test]
    fn freshness_state_maps_custody_to_chip() {
        assert_eq!(freshness_state(false, false), "built");
        assert_eq!(freshness_state(false, true), "built");
        assert_eq!(freshness_state(true, false), "stale");
        assert_eq!(freshness_state(true, true), "behind");
    }

    /// `graph.status` for a rig the warden never registered is `NotFound` (custody is the source).
    #[tokio::test]
    async fn status_unknown_rig_is_not_found() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));
        let err = h
            .dispatch("graph.status", ctx(json!({ "rig": "ghost" })))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }

    /// `graph.refresh` (no provisioner, no repo_dir arg) derives the path, registers the rig at it,
    /// builds, marks it fresh — then it is queryable and listed as not-stale at the derived path.
    #[tokio::test]
    async fn refresh_derives_path_registers_builds_and_marks_fresh() {
        let (root, _guard) = pin_graph_root();
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));

        let out = h
            .dispatch("graph.refresh", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        assert_eq!(out["ok"], true);
        assert_eq!(out["rig"], "alpha");
        // The custody/build path is the server-derived one under the pinned root.
        let expected = root.path().join("default").join("alpha");
        assert_eq!(out["repo_dir"], expected.to_string_lossy().as_ref());

        // Now listed, not stale (just refreshed), at the derived path.
        let list = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["rigs"][0]["stale"], false);
        assert_eq!(
            list["rigs"][0]["repo_dir"],
            expected.to_string_lossy().as_ref()
        );

        // And queryable (the graph now exists).
        let q = h
            .dispatch(
                "graph.query",
                ctx(json!({ "rig": "alpha", "question": "x term" })),
            )
            .await
            .unwrap();
        assert!(q["text"].is_string());

        // Second refresh resolves the derived path again and updates in place.
        let again = h
            .dispatch("graph.refresh", ctx(json!({ "rig": "alpha" })))
            .await
            .unwrap();
        assert_eq!(again["ok"], true);
    }

    /// A rig already under custody at a DEAD path (the inactivas-chain case) is re-pointed cleanly
    /// to the server-derived path on the next refresh — custody and build never diverge again.
    #[tokio::test]
    async fn refresh_repoints_poisoned_custody_to_derived_path() {
        let (root, _guard) = pin_graph_root();
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        // Pre-existing poisoned custody: a path from someone's host that does not exist here.
        log.append(
            None,
            WardenEvent::RigRegistered {
                rig: "inactivas-chain".into(),
                repo_dir: "/home/brayan.rayo/compose/inactivas-chain".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));

        let out = h
            .dispatch("graph.refresh", ctx(json!({ "rig": "inactivas-chain" })))
            .await
            .unwrap();
        let expected = root.path().join("default").join("inactivas-chain");
        assert_eq!(out["repo_dir"], expected.to_string_lossy().as_ref());

        // Custody now points at the derived path, not the dead one.
        let list = h.dispatch("graph.list", ctx(json!({}))).await.unwrap();
        assert_eq!(list["rigs"].as_array().unwrap().len(), 1);
        assert_eq!(
            list["rigs"][0]["repo_dir"],
            expected.to_string_lossy().as_ref()
        );
    }

    /// A rig the warden never registered has no custody → NotFound on read.
    #[tokio::test]
    async fn query_unknown_rig_is_not_found() {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(EventLog::new(Some(dir.path().to_path_buf())));
        let h = GraphHandler::new(log, Arc::new(InMemoryGraphIndexer::new()));
        let err = h
            .dispatch(
                "graph.query",
                ctx(json!({ "rig": "ghost", "question": "x" })),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NotFound(_)));
    }
}
