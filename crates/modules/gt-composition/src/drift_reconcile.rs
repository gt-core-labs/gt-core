//! Scheduler drift-reconcile: `ls-remote` vs the indexed commit (hq-vcs-connections.8).
//!
//! The custodian (the graph warden) learns a rig's graph fell behind through three sources, ordered
//! by latency:
//!
//! 1. **Internal merge** — the merge queue marks the owning rig stale (`mcp/merge.rs`
//!    `mark_owning_rig_stale`).
//! 2. **External push webhook** — a signed GitHub `push` delivery marks the pushed rig stale
//!    (`crate::webhook`). The fast path for a push straight to origin.
//! 3. **Drift reconcile** — THIS module: a low-cadence backstop for the deliveries the webhook
//!    misses (App downtime, a missed/dropped delivery, the App uninstalled-then-reinstalled, a
//!    network blip). It polls the remote head directly and reconciles against the indexed commit.
//!
//! ## The tick
//!
//! On each interval, for every workspace partition under the event-log root, the daemon:
//! 1. replays that workspace's [`WardenState`] (`graphwarden.` stream) to find the rigs under graph
//!    custody — exactly the rigs whose graphs exist;
//! 2. for each such rig, reads its catalog entry ([`RigEntry`]: `git_url`, `default_branch`,
//!    `git_connection_ref`) from the tenant's `ws_<slug>` rig table;
//! 3. resolves the rig's VCS connection (when `git_connection_ref` is set) and mints a **JIT
//!    installation token** ([`gt_vcs::GithubAppClient::installation_token`], 1h, in-memory only —
//!    NEVER persisted) so a private `ls-remote` authenticates; a rig with no connection (a public
//!    repo) is `ls-remote`d with no token;
//! 4. runs `git ls-remote <git_url> refs/heads/<default_branch>` — one cheap call, no clone — to read
//!    the remote tip;
//! 5. compares that tip to the warden's `last_indexed_commit`. GitHub returns the FULL 40-char SHA;
//!    the warden records the SHORT `rev-parse --short` form, so the compare is by PREFIX
//!    ([`diverged`]) — the same shape as the webhook's head-moved filter;
//! 6. on divergence appends [`WardenCommand::MarkStale`] (replay → execute → append, the
//!    `mcp/merge.rs` / `crate::webhook` pattern). `graph.refresh-stale` then reindexes it.
//!
//! This module NEVER clones or indexes — like the webhook reactor it only flips the freshness flag.
//!
//! ## Opt-in / configurable
//!
//! The daemon is wired in `gt-mcp-server` only when `GT_GRAPH_DRIFT_TICK_SECS > 0` (default off in
//! the binary's call site is an hourly cadence; `0` disables it), so it never fires in tests or in a
//! deploy that has not opted in. The cadence is the env var's value in seconds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use gt_graphwarden::{MarkStale, WardenCommand, WardenState};
use gt_rig::{PgRigs, RigEntry, RigRepository};
use gt_vcs::{ConnectionKind, ConnectionStatus, GithubAppClient, VcsConnectionRepo};

use crate::mcp::{EventLog, WsPools};

/// The warden event-log kind prefix, replayed to read/append graph custody (mirrors
/// `crate::webhook::WARDEN_NS` and `mcp/merge.rs`).
const WARDEN_NS: &str = "graphwarden.";

/// Resolves the remote tip of a rig's default branch — abstracted so the tick decision is unit
/// testable without a network or a real git binary.
///
/// The production implementation ([`GitLsRemote`]) runs `git ls-remote`; a test substitutes a map.
#[async_trait]
pub trait RemoteTipResolver: Send + Sync {
    /// The current tip SHA of `git_url`'s `refs/heads/<branch>`, authenticating with `token` when
    /// `Some` (a private repo behind a JIT installation token) or anonymously when `None` (a public
    /// repo). `Ok(None)` when the branch does not exist remotely (deleted / never pushed) — not an
    /// error, just nothing to compare. `Err` is a transport/auth fault the caller logs and skips.
    async fn remote_tip(
        &self,
        git_url: &str,
        branch: &str,
        token: Option<&str>,
    ) -> Result<Option<String>, gt_events::AppError>;
}

/// Whether the remote tip diverged from the indexed commit — the core decision, kept pure for unit
/// tests.
///
/// `remote_tip` is GitHub's FULL 40-char SHA; `last_indexed_commit` is the warden's SHORT
/// `rev-parse --short` form, so a match is a PREFIX match (`full.starts_with(short)`), the same shape
/// as the webhook's head-moved filter.
///
/// Returns `true` (mark stale) when:
/// - the rig was never indexed (`last_indexed_commit` is `None` or empty) and the remote has a tip —
///   an initial build is owed and the backstop should not let it sit un-indexed; OR
/// - the remote tip does not start with the indexed short commit (the head moved).
///
/// Returns `false` (leave it) when the remote tip prefix-matches the indexed commit (already
/// current), or when the remote has no tip at all (`None` — nothing to reconcile against).
pub fn diverged(remote_tip: Option<&str>, last_indexed_commit: Option<&str>) -> bool {
    let Some(remote) = remote_tip.filter(|s| !s.is_empty()) else {
        // No remote tip (branch absent / unreadable) → nothing to compare, do not mark stale.
        return false;
    };
    match last_indexed_commit.filter(|s| !s.is_empty()) {
        // Never indexed but the remote has a head → an initial index is owed.
        None => true,
        // Indexed: stale iff the remote head no longer starts with the short commit we indexed.
        Some(short) => !remote.starts_with(short),
    }
}

/// A rig the tick should reconcile: its catalog entry plus the warden's last-indexed commit.
struct CustodyRig {
    entry: RigEntry,
    last_indexed_commit: Option<String>,
}

/// One workspace's reconcile pass. Replays the warden, reads the rig catalog, and for every rig
/// under custody compares the remote tip to the indexed commit, marking stale on divergence. Returns
/// the rig names it marked stale. Best-effort per rig: a connection-resolve / token-mint / ls-remote
/// fault on one rig is logged and skipped, never aborting the others.
///
/// Split out (and generic over the resolver) so it is unit-testable without standing up the binary.
async fn reconcile_workspace<R: RemoteTipResolver + ?Sized>(
    workspace: Option<&str>,
    log: &EventLog,
    rigs: &[CustodyRig],
    connections: &dyn VcsConnectionRepo,
    github: Option<&GithubAppClient>,
    resolver: &R,
) -> Vec<String> {
    let mut marked = Vec::new();
    for rig in rigs {
        match reconcile_one(workspace, log, rig, connections, github, resolver).await {
            Ok(true) => marked.push(rig.entry.name.clone()),
            Ok(false) => {}
            Err(e) => {
                eprintln!(
                    "[graph-drift] ws={} rig={} skipped: {e}",
                    workspace.unwrap_or("default"),
                    rig.entry.name
                );
            }
        }
    }
    marked
}

/// Reconcile a single rig: resolve its connection (private → JIT token; public → none), read the
/// remote tip, and `MarkStale` on divergence. `Ok(true)` when a stale-mark was appended.
async fn reconcile_one<R: RemoteTipResolver + ?Sized>(
    workspace: Option<&str>,
    log: &EventLog,
    rig: &CustodyRig,
    connections: &dyn VcsConnectionRepo,
    github: Option<&GithubAppClient>,
    resolver: &R,
) -> Result<bool, gt_events::AppError> {
    // Resolve a clone credential: a github_app connection mints a JIT installation token; a pat
    // connection unseals its token; no connection (or a non-active one) → anonymous (public repo).
    let token = resolve_token(workspace, &rig.entry, connections, github).await?;

    let tip = resolver
        .remote_tip(
            &rig.entry.git_url,
            &rig.entry.default_branch,
            token.as_deref(),
        )
        .await?;

    if !diverged(tip.as_deref(), rig.last_indexed_commit.as_deref()) {
        return Ok(false);
    }

    // Replay → execute → append, the merge.rs / webhook.rs pattern. Replay fresh so the append is
    // against the latest state (another source may have marked it stale since the snapshot above).
    let state = log
        .replay_domain(workspace, WARDEN_NS, WardenState::default(), |s, e| {
            let _ = s.apply(e);
        })
        .map_err(lift_dolt_err)?;
    // Idempotent: if it is already stale, do not append a duplicate mark.
    if state.rigs.get(&rig.entry.name).map(|g| g.stale) != Some(false) {
        return Ok(false);
    }
    let cmd = WardenCommand::MarkStale(MarkStale {
        rig: rig.entry.name.clone(),
        changed: 1,
        now_secs: now_secs(),
    });
    let events = cmd.execute(&state)?;
    for ev in events {
        log.append(workspace, ev).map_err(lift_dolt_err)?;
    }
    Ok(true)
}

/// Resolve the clone credential for `rig`: `Some(token)` to embed as `x-access-token` for a private
/// `ls-remote`, or `None` for an anonymous (public-repo / connectionless) one.
///
/// - No `git_connection_ref` → `None` (legacy operator-mounted / public-repo path).
/// - A `github_app` connection → mint a JIT installation token (1h, in-memory only, never persisted).
/// - A `pat` connection → the unsealed PAT (the fallback).
/// - A connection that is not `Active`, or has no installation id, or no resolvable client → `None`
///   (best-effort; an anonymous `ls-remote` of a private repo simply fails downstream and is skipped).
async fn resolve_token(
    workspace: Option<&str>,
    entry: &RigEntry,
    connections: &dyn VcsConnectionRepo,
    github: Option<&GithubAppClient>,
) -> Result<Option<String>, gt_events::AppError> {
    let Some(conn_ref) = entry.git_connection_ref.as_deref() else {
        return Ok(None);
    };
    let ws = workspace.unwrap_or("default");
    let Some(conn) = connections.get_for_workspace(ws, conn_ref).await? else {
        return Ok(None);
    };
    if conn.status != ConnectionStatus::Active {
        return Ok(None);
    }
    match conn.kind {
        ConnectionKind::GithubApp => {
            let (Some(client), Some(installation_id)) = (github, conn.installation_id.as_deref())
            else {
                return Ok(None);
            };
            let token = client.installation_token(installation_id).await?;
            Ok(Some(token.secret().to_string()))
        }
        ConnectionKind::Pat => conn.unseal_secret(),
    }
}

/// Run ONE reconcile pass over every workspace partition. Enumerates the workspace directories under
/// the event-log root (each is one tenant's append-only log), replays the warden per workspace, reads
/// the tenant's rig catalog from its `ws_<slug>` schema, and reconciles each rig under custody.
///
/// Returns the total number of rigs it marked stale across all workspaces. A per-workspace fault
/// (PG connect, replay) is logged and skipped so one bad tenant never stalls the sweep.
pub async fn reconcile_pass<R: RemoteTipResolver + ?Sized>(
    log: &EventLog,
    pools: &WsPools,
    connections: &dyn VcsConnectionRepo,
    github: Option<&GithubAppClient>,
    resolver: &R,
) -> usize {
    let mut total = 0usize;
    for ws in log.workspaces() {
        let workspace = Some(ws.as_str());
        // Replay the warden to find the rigs under custody (the ones whose graphs exist).
        let state = match log.replay_domain(workspace, WARDEN_NS, WardenState::default(), |s, e| {
            let _ = s.apply(e);
        }) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[graph-drift] ws={ws} warden replay failed: {e}");
                continue;
            }
        };
        if state.rigs.is_empty() {
            continue;
        }
        // Index the warden's last-indexed commits by rig.
        let indexed: BTreeMap<String, Option<String>> = state
            .rigs
            .values()
            .map(|g| (g.rig.clone(), g.last_indexed_commit.clone()))
            .collect();

        // Read the tenant's rig catalog.
        let pool = match pools.get(workspace).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[graph-drift] ws={ws} pool connect failed: {e}");
                continue;
            }
        };
        let catalog = match PgRigs::new(pool.pool().clone()).list().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[graph-drift] ws={ws} rig list failed: {e}");
                continue;
            }
        };

        // Reconcile only rigs that are BOTH under custody and in the catalog (we need git_url +
        // default_branch from the catalog and the indexed commit from the warden).
        let rigs: Vec<CustodyRig> = catalog
            .into_iter()
            .filter_map(|entry| {
                indexed
                    .get(&entry.name)
                    .cloned()
                    .map(|last_indexed_commit| CustodyRig {
                        entry,
                        last_indexed_commit,
                    })
            })
            .collect();
        if rigs.is_empty() {
            continue;
        }

        let marked =
            reconcile_workspace(workspace, log, &rigs, connections, github, resolver).await;
        if !marked.is_empty() {
            eprintln!(
                "[graph-drift] ws={ws} marked {} rig(s) stale: {}",
                marked.len(),
                marked.join(", ")
            );
        }
        total += marked.len();
    }
    total
}

/// The daemon loop: a `tokio::time::interval` of `tick` that runs [`reconcile_pass`] each cycle.
/// Awaitable (never returns) so it composes with `tokio::spawn` exactly like the account-dir GC and
/// quota-feed loops in `gt-mcp-server`. The production resolver is [`GitLsRemote`].
pub async fn run(
    tick: Duration,
    log: Arc<EventLog>,
    pools: Arc<WsPools>,
    connections: Arc<dyn VcsConnectionRepo>,
    github: Option<GithubAppClient>,
) {
    let resolver = GitLsRemote;
    let mut interval = tokio::time::interval(tick);
    loop {
        interval.tick().await;
        let _ = reconcile_pass(
            &log,
            &pools,
            connections.as_ref(),
            github.as_ref(),
            &resolver,
        )
        .await;
    }
}

/// Production [`RemoteTipResolver`]: `git ls-remote <git_url> refs/heads/<branch>`, embedding a JIT
/// `x-access-token` into the URL for a private repo. One cheap network call — no clone.
pub struct GitLsRemote;

#[async_trait]
impl RemoteTipResolver for GitLsRemote {
    async fn remote_tip(
        &self,
        git_url: &str,
        branch: &str,
        token: Option<&str>,
    ) -> Result<Option<String>, gt_events::AppError> {
        let url = authenticated_url(git_url, token);
        let git_ref = format!("refs/heads/{branch}");
        let url_for_blocking = url.clone();
        let ref_for_blocking = git_ref.clone();
        // `git ls-remote` is blocking; run it off the async runtime.
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new("git")
                .arg("ls-remote")
                .arg(&url_for_blocking)
                .arg(&ref_for_blocking)
                .output()
        })
        .await
        .map_err(|e| gt_events::AppError::Other(format!("ls-remote join: {e}")))?
        .map_err(|e| gt_events::AppError::Other(format!("ls-remote spawn: {e}")))?;
        if !output.status.success() {
            // Redact any embedded token from the error.
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(gt_events::AppError::Other(format!(
                "ls-remote {git_url} {git_ref} failed: {}",
                redact_token(&stderr, token)
            )));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(parse_ls_remote_tip(&stdout))
    }
}

/// Build the `ls-remote` URL: embed the token as `x-access-token` for an `https://github.com/...`
/// URL when `Some`, else return the URL unchanged. An SSH URL (`git@github.com:...`) is left as-is —
/// a token does not apply to it (those rigs authenticate via the SSH agent, the legacy host path).
fn authenticated_url(git_url: &str, token: Option<&str>) -> String {
    match token {
        Some(tok) if git_url.starts_with("https://github.com/") => git_url.replacen(
            "https://github.com/",
            &format!("https://x-access-token:{tok}@github.com/"),
            1,
        ),
        _ => git_url.to_string(),
    }
}

/// The SHA in the first line of `git ls-remote` output (`<sha>\t<ref>`), or `None` when the output is
/// empty (the ref does not exist remotely).
fn parse_ls_remote_tip(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().next())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

/// Replace an embedded token with `***` in an error string so a secret never reaches a log.
fn redact_token(s: &str, token: Option<&str>) -> String {
    match token {
        Some(t) if !t.is_empty() => s.replace(t, "***"),
        _ => s.to_string(),
    }
}

/// Lift a `gt_store_dolt::AppError` (the EventLog replay/append path) into the kernel
/// `gt_events::AppError` this module threads through (the connection/rig ports + warden command path
/// already return it), preserving the variant — the reverse of `crate::webhook::lift_err`.
fn lift_dolt_err(e: gt_store_dolt::AppError) -> gt_events::AppError {
    use gt_events::AppError as K;
    use gt_store_dolt::AppError as D;
    match e {
        D::InvalidTransition(s) => K::InvalidTransition(s),
        D::NotFound(s) => K::NotFound(s),
        D::Validation(s) => K::Validation(s),
        D::Handler(s) => K::Handler(s),
        other => K::Other(other.to_string()),
    }
}

/// Current unix time in seconds (mirrors `crate::webhook::now_secs`, kept local so the module is
/// self-contained).
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_graphwarden::WardenEvent;
    use gt_rig::RigEntry;
    use gt_vcs::{NewConnection, PatchConnection, VcsConnection};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // --- the pure divergence decision ----------------------------------------------------------

    #[test]
    fn diverged_when_never_indexed_but_remote_has_a_tip() {
        // An initial build is owed: the warden has the rig under custody with no last_indexed_commit.
        assert!(diverged(Some("abcdef0123"), None));
        assert!(diverged(Some("abcdef0123"), Some("")));
    }

    #[test]
    fn not_diverged_when_remote_tip_prefix_matches_indexed_short_commit() {
        // GitHub's full SHA starts with the warden's short rev-parse form → already current.
        let full = "abcdef0123456789abcdef0123456789abcdef01";
        assert!(!diverged(Some(full), Some("abcdef0")));
    }

    #[test]
    fn diverged_when_remote_head_moved() {
        let full = "fedcba9876543210fedcba9876543210fedcba98";
        assert!(diverged(Some(full), Some("abcdef0")));
    }

    #[test]
    fn not_diverged_when_remote_has_no_tip() {
        // Branch absent / unreadable → nothing to compare, never mark stale.
        assert!(!diverged(None, Some("abcdef0")));
        assert!(!diverged(Some(""), None));
    }

    // --- ls-remote parsing + URL auth ----------------------------------------------------------

    #[test]
    fn parse_ls_remote_takes_the_first_sha() {
        let out = "abcdef0123456789abcdef0123456789abcdef01\trefs/heads/main\n";
        assert_eq!(
            parse_ls_remote_tip(out).as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef01")
        );
        assert_eq!(parse_ls_remote_tip("").as_deref(), None);
        assert_eq!(parse_ls_remote_tip("\n").as_deref(), None);
    }

    #[test]
    fn authenticated_url_embeds_token_only_for_https_github() {
        let tok = "ghs_secret";
        assert_eq!(
            authenticated_url("https://github.com/o/r.git", Some(tok)),
            "https://x-access-token:ghs_secret@github.com/o/r.git"
        );
        // No token → unchanged.
        assert_eq!(
            authenticated_url("https://github.com/o/r.git", None),
            "https://github.com/o/r.git"
        );
        // SSH URL → token does not apply, left as-is.
        assert_eq!(
            authenticated_url("git@github.com:o/r.git", Some(tok)),
            "git@github.com:o/r.git"
        );
    }

    #[test]
    fn redact_token_scrubs_the_secret_from_an_error() {
        let err = "fatal: could not read https://x-access-token:ghs_secret@github.com/o/r.git";
        let scrubbed = redact_token(err, Some("ghs_secret"));
        assert!(!scrubbed.contains("ghs_secret"));
        assert!(scrubbed.contains("***"));
    }

    // --- the reconcile decision end-to-end against a tempdir warden log -------------------------

    /// A resolver returning a fixed tip per git_url, recording the token it was handed.
    struct FakeResolver {
        tips: HashMap<String, Option<String>>,
        seen_token: Mutex<Option<Option<String>>>,
    }

    impl FakeResolver {
        fn new(tips: impl IntoIterator<Item = (&'static str, Option<&'static str>)>) -> Self {
            Self {
                tips: tips
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
                    .collect(),
                seen_token: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl RemoteTipResolver for FakeResolver {
        async fn remote_tip(
            &self,
            git_url: &str,
            _branch: &str,
            token: Option<&str>,
        ) -> Result<Option<String>, gt_events::AppError> {
            *self.seen_token.lock().unwrap() = Some(token.map(str::to_string));
            Ok(self.tips.get(git_url).cloned().flatten())
        }
    }

    /// A connection store with zero connections — exercises the connectionless (public-repo) path.
    struct NoConns;

    #[async_trait]
    impl VcsConnectionRepo for NoConns {
        async fn list_for_workspace(
            &self,
            _: &str,
        ) -> Result<Vec<VcsConnection>, gt_events::AppError> {
            Ok(vec![])
        }
        async fn get_for_workspace(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok(None)
        }
        async fn create(&self, _: NewConnection) -> Result<VcsConnection, gt_events::AppError> {
            unreachable!()
        }
        async fn patch(
            &self,
            _: &str,
            _: &str,
            _: PatchConnection,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok(None)
        }
        async fn delete(&self, _: &str, _: &str) -> Result<bool, gt_events::AppError> {
            Ok(false)
        }
        async fn find_by_installation(
            &self,
            _: &str,
        ) -> Result<Option<VcsConnection>, gt_events::AppError> {
            Ok(None)
        }
    }

    fn entry(name: &str, git_url: &str) -> RigEntry {
        RigEntry::new(name, "px", git_url, "main", 1)
    }

    fn custody(name: &str, git_url: &str, indexed: Option<&str>) -> CustodyRig {
        CustodyRig {
            entry: entry(name, git_url),
            last_indexed_commit: indexed.map(str::to_string),
        }
    }

    /// A rig whose remote head moved past the indexed commit gets a MarkStale appended; a rig already
    /// current does not — both decided through `reconcile_workspace` against a real warden log.
    #[tokio::test]
    async fn moved_head_marks_stale_current_head_does_not() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let ws = Some("acme");
        // Two rigs under custody, both freshly refreshed (not stale) at their indexed commit.
        for (rig, commit) in [("moved", "aaaaaaa"), ("current", "bbbbbbb")] {
            log.append(
                ws,
                WardenEvent::RigRegistered {
                    rig: rig.into(),
                    repo_dir: format!("/g/{rig}"),
                    now_secs: 1,
                },
            )
            .unwrap();
            log.append(
                ws,
                WardenEvent::Refreshed {
                    rig: rig.into(),
                    commit: commit.into(),
                    now_secs: 2,
                },
            )
            .unwrap();
        }

        // Remote: `moved` advanced to a new full SHA; `current` is still at the indexed short commit.
        let resolver = FakeResolver::new([
            (
                "https://github.com/o/moved.git",
                Some("ccccccc1111111111111111111111111111ffff"),
            ),
            (
                "https://github.com/o/current.git",
                Some("bbbbbbb2222222222222222222222222222ffff"),
            ),
        ]);
        let rigs = vec![
            custody("moved", "https://github.com/o/moved.git", Some("aaaaaaa")),
            custody(
                "current",
                "https://github.com/o/current.git",
                Some("bbbbbbb"),
            ),
        ];

        let marked = reconcile_workspace(ws, &log, &rigs, &NoConns, None, &resolver).await;
        assert_eq!(marked, vec!["moved".to_string()]);
        // The connectionless rig was ls-remote'd anonymously.
        assert_eq!(*resolver.seen_token.lock().unwrap(), Some(None));

        // The warden now sees `moved` stale and `current` still fresh.
        let state = log
            .replay_domain(ws, WARDEN_NS, WardenState::default(), |s, e| {
                let _ = s.apply(e);
            })
            .unwrap();
        assert!(state.rigs["moved"].stale, "moved head → stale");
        assert!(!state.rigs["current"].stale, "current head → left fresh");
    }

    /// A rig under custody that was never indexed (no Refreshed event) is marked stale when the
    /// remote has any tip — the backstop owes it an initial build.
    #[tokio::test]
    async fn never_indexed_rig_with_a_remote_tip_marks_stale() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let ws = Some("acme");
        log.append(
            ws,
            WardenEvent::RigRegistered {
                rig: "fresh".into(),
                repo_dir: "/g/fresh".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        // Registration already sets stale=true, so re-marking must be a no-op (idempotent guard).
        let resolver = FakeResolver::new([(
            "https://github.com/o/fresh.git",
            Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
        )]);
        let rigs = vec![custody("fresh", "https://github.com/o/fresh.git", None)];

        let marked = reconcile_workspace(ws, &log, &rigs, &NoConns, None, &resolver).await;
        // Already stale on registration → the idempotent guard skips a duplicate mark.
        assert!(marked.is_empty(), "already-stale rig is not re-marked");
    }

    /// `reconcile_pass` skips a rig that is in the catalog but NOT under warden custody (no graph
    /// exists for it yet) — only custody rigs are reconciled.
    #[tokio::test]
    async fn already_stale_rig_is_not_re_marked() {
        let dir = TempDir::new().unwrap();
        let log = EventLog::new(Some(dir.path().to_path_buf()));
        let ws = Some("acme");
        // Registered (stale=true) then NOT refreshed → still stale.
        log.append(
            ws,
            WardenEvent::RigRegistered {
                rig: "r".into(),
                repo_dir: "/g/r".into(),
                now_secs: 1,
            },
        )
        .unwrap();
        let resolver =
            FakeResolver::new([("https://github.com/o/r.git", Some("0011223344556677"))]);
        let rigs = vec![custody("r", "https://github.com/o/r.git", Some("aaaaaaa"))];
        let marked = reconcile_workspace(ws, &log, &rigs, &NoConns, None, &resolver).await;
        assert!(marked.is_empty(), "an already-stale rig is not re-marked");
    }
}
