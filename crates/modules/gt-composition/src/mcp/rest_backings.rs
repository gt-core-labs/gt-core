//! REST-surface backings for the domains whose `register_routes` adapter (hq-fe-api)
//! needs a composition-tier provider (hq-fe-api-mount.1).
//!
//! The per-domain REST `ApiState`s (`gt_rig::RigApiState`, `gt_quota::QuotaApiState`)
//! own no store of their own — they delegate to a `WorkspaceRigs` / `WorkspaceQuota`
//! provider the binary supplies, exactly as the MCP handlers delegate to the shared
//! [`WsPools`] cache / [`EventLog`]. These providers are the REST mirrors of
//! [`RigHandler`](super::rig::RigHandler) and [`QuotaHandler`](super::quota::QuotaHandler):
//! they resolve the *same* per-workspace state, so a `rig.add` over MCP and a
//! `POST /api/v1/rig` see one Postgres schema, and a `quota.sample` over either edge
//! folds into one event log.
//!
//! Only `modules` may know both the composition-tier handles ([`WsPools`], [`EventLog`])
//! and the domain crates (docs/03 Rule 4), so the providers live here, not in the domain
//! crate (whose REST trait is deliberately store-agnostic).
//!
//! The two `AppError` types are distinct nominal enums with identical variants — the
//! domain-core [`gt_events::AppError`] the REST traits speak, and the
//! [`gt_store_dolt::AppError`] the [`WsPools`]/[`EventLog`] edges return — so [`lift`]
//! maps one to the other at the boundary.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use gt_events::AppError;

use gt_auth::MembershipDirectory;
use gt_feed::{FeedItem, FeedPage, WorkspaceFeed};
use gt_issues::{MeMembership, MeStatsSource};
use gt_mcp_server::WorkspaceStores;
use gt_store_dolt::{issues_max_limit, AppError as DoltError, IssueFilter, IssueRow};
use gt_merge::{
    DynMergeRepository, MergeBoard, MergeEvent, MergeRepository, MergeSlot, MergeSlotState,
    MergeState, WorkspaceMerges,
};
use gt_orchestration::{ConvoyBoard, OrchEvent, OrchState, WorkspaceConvoy};
use gt_quota::{AccountRegistry, QuotaEvent, QuotaState, WorkspaceQuota};
use gt_rig::{DynRigRepository, PgRigs, WorkspaceRigs};
use gt_skills::{SkillCatalog, SkillEvent, SkillState, SkillWriter, WorkspaceSkills};

use super::eventlog::EventLog;
use super::pools::WsPools;

/// The event-log kind prefix every quota event carries (`quota.*.v1`); matches
/// [`QuotaHandler`](super::quota::QuotaHandler)'s replay filter so both edges fold
/// the identical stream.
const QUOTA_NS: &str = "quota.";

/// The event-log kind prefix every merge event carries (`merge.*.v1`); matches
/// [`MergeHandler`](super::merge::MergeHandler)'s replay filter so the REST surface and the
/// MCP handler fold the identical per-workspace stream.
const MERGE_NS: &str = "merge.";

/// The event-log kind prefix every skill event carries (`skills.*.v1`); the read-only REST
/// surface replays exactly this stream into the catalog the dashboard hydrates from.
const SKILLS_NS: &str = "skills.";

/// The event-log kind prefix every convoy event carries (`convoy.*.v1`); matches
/// [`ConvoyHandler`](super::convoy::ConvoyHandler)'s replay filter so the REST surface and the
/// MCP handler fold the identical per-workspace stream.
const CONVOY_NS: &str = "convoy.";

/// Upper bound on how many records the feed REST surface scans per request. The feed paginates the
/// *whole* tenant log (every namespace, not one prefix), so a generous cap bounds the read without
/// truncating realistic workloads; `offset`/`limit` then window the matched set.
const FEED_SCAN_LIMIT: usize = 100_000;

/// Map the edge error ([`gt_store_dolt::AppError`]) onto the domain-core error
/// ([`gt_events::AppError`]) the REST ports return. The two enums are variant-for-variant
/// identical (the dolt one is a behaviour-preserving port of the core one), so this is a
/// total 1:1 relabel, never a lossy collapse.
fn lift(e: gt_store_dolt::AppError) -> AppError {
    use gt_store_dolt::AppError as D;
    match e {
        D::InvalidTransition(s) => AppError::InvalidTransition(s),
        D::NotFound(s) => AppError::NotFound(s),
        D::Validation(s) => AppError::Validation(s),
        D::Handler(s) => AppError::Handler(s),
        D::Other(s) => AppError::Other(s),
    }
}

/// REST backing for `rig.*` (`gt_rig::WorkspaceRigs`): resolves the caller's workspace
/// schema through the shared [`WsPools`] cache and hands back a [`PgRigs`] over it — the
/// same pool + adapter [`RigHandler`](super::rig::RigHandler) dispatches through.
pub struct WsPoolRigs {
    pools: Arc<WsPools>,
}

impl WsPoolRigs {
    /// Wrap the per-workspace pool cache (the binary's single shared instance).
    pub fn new(pools: Arc<WsPools>) -> Self {
        Self { pools }
    }
}

#[async_trait]
impl WorkspaceRigs for WsPoolRigs {
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynRigRepository>, AppError> {
        let pool = self.pools.get(Some(workspace)).await.map_err(lift)?;
        Ok(Box::new(PgRigs::new(pool.pool().clone())))
    }
}

/// REST backing for `quota.*` (`gt_quota::WorkspaceQuota`): rehydrates the account
/// registry from the caller's workspace event log and appends decided events back — the
/// same read-modify-append [`QuotaHandler`](super::quota::QuotaHandler) runs over the
/// shared [`EventLog`].
pub struct EventLogQuota {
    log: Arc<EventLog>,
}

impl EventLogQuota {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceQuota for EventLogQuota {
    async fn registry(&self, workspace: &str) -> Result<AccountRegistry, AppError> {
        let state = self
            .log
            .replay_domain(Some(workspace), QUOTA_NS, QuotaState::default(), QuotaState::apply)
            .map_err(lift)?;
        Ok(AccountRegistry::from_state(&state))
    }

    async fn append(&self, workspace: &str, event: QuotaEvent) -> Result<(), AppError> {
        self.log.append(Some(workspace), event).map_err(lift)
    }
}

/// REST backing for `merge.*` (`gt_merge::WorkspaceMerges`): the durable mirror of
/// [`MergeHandler`](super::merge::MergeHandler). The merge board is event-sourced — the
/// in-memory [`MergeRepository`](gt_merge::MergeRepository) the actor holds is a rebuilt-on-boot
/// projection, never durable — so this backing resolves a per-request repository that rehydrates
/// the board by replaying the caller's `merge.*` stream and appends transitions back to that same
/// authoritative log. A `merge.submit` over MCP and a `POST /api/v1/merge` therefore land in one
/// per-workspace board that survives restart.
pub struct EventLogMerges {
    log: Arc<EventLog>,
}

impl EventLogMerges {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceMerges for EventLogMerges {
    async fn repo(&self, workspace: &str) -> Result<Box<dyn DynMergeRepository>, AppError> {
        // The repo is a thin per-workspace handle over the shared log; building it cannot fail
        // (the first replay/append is where an I/O error would surface), so no store touch here.
        Ok(Box::new(EventLogMergeRepo {
            log: self.log.clone(),
            workspace: workspace.to_string(),
        }))
    }
}

/// A `merge.*` repository scoped to one workspace, over the shared event log.
///
/// Reads ([`list_slots`](MergeRepository::list_slots) / [`get_slot`](MergeRepository::get_slot))
/// replay the stream; a write ([`upsert_slot`](MergeRepository::upsert_slot)) appends the
/// [`MergeEvent`] the slot's resulting state implies. The board state machine derives a slot's
/// state from its event *variant*, so the slot's lifecycle round-trips faithfully. The event's
/// audit fields (`channel_msg_id` / `sha` / `reason`) are not part of the projection
/// [`MergeSlot`] (`{bead, branch, state}`) the REST adapter persists, so a REST-written event
/// carries them empty — the authoritative audit values are those the actor / refinery edge
/// append. Implementing [`MergeRepository`] (not the boxed [`DynMergeRepository`] directly) reuses
/// the crate's blanket adapter, so `Box<dyn DynMergeRepository>` falls out for free.
struct EventLogMergeRepo {
    log: Arc<EventLog>,
    workspace: String,
}

impl EventLogMergeRepo {
    /// Rehydrate the tenant's board from its `merge.*` events — the same replay the MCP
    /// [`MergeHandler`](super::merge::MergeHandler) runs.
    fn board(&self) -> Result<MergeBoard, AppError> {
        let state = self
            .log
            .replay_domain(Some(&self.workspace), MERGE_NS, MergeState::default(), MergeState::apply)
            .map_err(lift)?;
        Ok(MergeBoard::from_state(&state))
    }

    /// The `merge.*` event a slot's current state implies, for the projection write the REST
    /// adapter performs after a validated transition. Audit fields absent from the slot default
    /// to empty (see the type docs).
    fn slot_event(slot: &MergeSlot) -> MergeEvent {
        match slot.state {
            MergeSlotState::Ready => MergeEvent::Ready {
                bead: slot.bead.clone(),
                branch: slot.branch.clone(),
                channel_msg_id: String::new(),
            },
            MergeSlotState::Merging => MergeEvent::Started { bead: slot.bead.clone() },
            MergeSlotState::Merged => {
                MergeEvent::Merged { bead: slot.bead.clone(), sha: String::new() }
            }
            MergeSlotState::Failed => {
                MergeEvent::Failed { bead: slot.bead.clone(), reason: String::new() }
            }
        }
    }
}

// The log's replay/append are synchronous, so each method resolves its `Result` eagerly and
// moves it into a trivially-ready (and `Send`) future — no borrow of `self` crosses the await.
impl MergeRepository for EventLogMergeRepo {
    fn upsert_slot(&self, slot: &MergeSlot) -> impl Future<Output = Result<(), AppError>> + Send {
        let res = self.log.append(Some(&self.workspace), Self::slot_event(slot)).map_err(lift);
        async move { res }
    }

    fn get_slot(
        &self,
        bead: &str,
    ) -> impl Future<Output = Result<Option<MergeSlot>, AppError>> + Send {
        let res = self.board().map(|b| b.get(bead).cloned());
        async move { res }
    }

    fn list_slots(&self) -> impl Future<Output = Result<Vec<MergeSlot>, AppError>> + Send {
        let res = self.board().map(|b| b.snapshot());
        async move { res }
    }
}

/// REST backing for `convoy.*` (`gt_orchestration::WorkspaceConvoy`, hq-web-extras.16): the
/// durable mirror of [`ConvoyHandler`](super::convoy::ConvoyHandler). Convoys keep no projection
/// table — the board is replayed from the workspace's `convoy.*` event stream through the
/// [`OrchState`] reducer into a [`ConvoyBoard`] — so this backing resolves the board by replaying
/// that stream and appends decided events back to the same authoritative log. A `convoy.launch`
/// over MCP and a `POST /api/v1/convoy` therefore land in one per-workspace board that survives
/// restart, the exact read-modify-append the MCP handler runs.
pub struct EventLogConvoy {
    log: Arc<EventLog>,
}

impl EventLogConvoy {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceConvoy for EventLogConvoy {
    async fn board(&self, workspace: &str) -> Result<ConvoyBoard, AppError> {
        let state = self
            .log
            .replay_domain(Some(workspace), CONVOY_NS, OrchState::default(), OrchState::apply)
            .map_err(lift)?;
        Ok(ConvoyBoard::from_state(&state))
    }

    async fn append(&self, workspace: &str, events: Vec<OrchEvent>) -> Result<(), AppError> {
        // One convoy command can emit several events (a launch is created + launched + the first
        // member-dispatched); append the whole batch in order, exactly as the MCP handler does.
        for event in events {
            self.log.append(Some(workspace), event).map_err(lift)?;
        }
        Ok(())
    }
}

/// REST backing for the read-only `skills.read` surface (`gt_skills::WorkspaceSkills`,
/// hq-web-extras.13): rehydrates the catalog from the caller's workspace `skills.*` event log —
/// the same replay the skills domain folds, exposed read-only over HTTP.
pub struct EventLogSkills {
    log: Arc<EventLog>,
}

impl EventLogSkills {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceSkills for EventLogSkills {
    async fn catalog(&self, workspace: &str) -> Result<SkillCatalog, AppError> {
        let state = self
            .log
            .replay_domain(Some(workspace), SKILLS_NS, SkillState::default(), SkillState::apply)
            .map_err(lift)?;
        Ok(state.catalog)
    }
}

#[async_trait]
impl SkillWriter for EventLogSkills {
    /// Persist a register/retire event into the workspace's `skills.*` stream (`hq-agent-
    /// observability.7`) — the same log the read replay folds, so the next `catalog()` sees it and
    /// the SSE feed carries `skills.registered.v1` / `skills.retired.v1` to the dashboard.
    async fn append(&self, workspace: &str, event: SkillEvent) -> Result<(), AppError> {
        self.log.append(Some(workspace), event).map_err(lift)
    }
}

/// REST backing for the read-only `feed.read` surface (`gt_feed::WorkspaceFeed`,
/// hq-web-extras.14): paginates the caller's *whole* workspace log — the same per-workspace stream
/// the SSE `/stream` and MCP feed read, exposed read-only + paginated over HTTP. The historical
/// counterpart to the live push: `/stream` tails new events, this pages back through the log.
pub struct EventLogFeed {
    log: Arc<EventLog>,
}

impl EventLogFeed {
    /// Wrap the per-workspace event log (the binary's single shared instance).
    pub fn new(log: Arc<EventLog>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WorkspaceFeed for EventLogFeed {
    async fn page(
        &self,
        workspace: &str,
        channel: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<FeedPage, AppError> {
        // `read_since` returns the channel-filtered records in chronological order (oldest→newest),
        // newest-bounded by the scan cap. The feed surfaces newest-first, so reverse, then window
        // by offset/limit. The channel namespace filter is `read_since`'s own (`merge` matches
        // `merge.*`), so the REST surface and `/stream` partition the stream identically.
        let mut records = self
            .log
            .read_since(Some(workspace), channel, None, FEED_SCAN_LIMIT)
            .map_err(lift)?;
        records.reverse(); // newest-first
        let total = records.len();
        let end = offset.saturating_add(limit).min(total);
        let items: Vec<FeedItem> = if offset < total {
            records[offset..end]
                .iter()
                .map(|r| FeedItem {
                    event_id: r.event_id.clone(),
                    kind: r.kind.clone(),
                    correlation_id: r.correlation_id.clone(),
                    causation_id: r.causation_id.clone(),
                    ts: r.ts.clone(),
                })
                .collect()
        } else {
            Vec::new()
        };
        let has_more = end < total;
        Ok(FeedPage { items, offset, limit, has_more, next_offset: has_more.then_some(end) })
    }
}

/// REST backing for the cross-workspace `me.*` surface (`gt_issues::MeStatsSource`,
/// `hq-web-extras.15`): resolves the caller's workspace memberships through the global identity
/// directory ([`MembershipDirectory`], `public.user_workspaces`) and each membership's tracker rows
/// through the per-workspace Dolt store cache ([`WorkspaceStores`], `hq_<ws>`).
///
/// Only `modules` may know both the identity directory and the per-workspace issue stores
/// (docs/03 Rule 4), so this adapter lives here, not in `gt-issues` (whose port is store-agnostic).
/// It is the REST mirror of the per-workspace stats endpoint run once per membership: the same
/// snapshot rows `read_issues` folds, sourced from each tenant's own `hq_<ws>` database.
pub struct IdentityDoltMeStats {
    /// The global membership directory — lists the caller's `(workspace, role)` rows by `sub`.
    memberships: Arc<dyn MembershipDirectory>,
    /// The per-workspace Dolt store resolver — one `hq_<ws>` store per tenant, lazily pooled.
    stores: Arc<WorkspaceStores>,
}

impl IdentityDoltMeStats {
    /// Wire the cross-workspace stats source over the shared membership directory + per-workspace
    /// Dolt store cache (the binary's single shared instances).
    pub fn new(memberships: Arc<dyn MembershipDirectory>, stores: Arc<WorkspaceStores>) -> Self {
        Self { memberships, stores }
    }
}

#[async_trait]
impl MeStatsSource for IdentityDoltMeStats {
    async fn memberships(&self, sub: &str) -> Result<Vec<MeMembership>, DoltError> {
        // The directory speaks `gt_auth::AuthError`; relabel a backend fault onto the store error
        // the port returns (a directory outage is an internal error, never a silent empty list).
        let rows = self
            .memberships
            .list(sub)
            .await
            .map_err(|e| DoltError::Other(format!("membership directory: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|m| MeMembership { workspace: m.workspace, role: m.role })
            .collect())
    }

    async fn issue_rows(&self, workspace: &str) -> Result<Vec<IssueRow>, DoltError> {
        // The tenant's own `hq_<ws>` tracker. Request the whole snapshot (full=false, max limit) so
        // the aggregate covers the workspace in one pass — the same filter the per-workspace
        // `/issues/stats` uses.
        let store = self.stores.store_for(workspace).await?;
        let filter = IssueFilter {
            status: Vec::new(),
            priority_max: None,
            assignee: None,
            external_ref: None,
            issue_type: None,
            limit: Some(issues_max_limit()),
            offset: None,
            full: false,
            ready: false,
        };
        store.list(&filter).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn slot(bead: &str, branch: &str, state: MergeSlotState) -> MergeSlot {
        MergeSlot { bead: bead.into(), branch: branch.into(), state }
    }

    /// A slot upserted through the backing survives a fresh provider over the same on-disk log —
    /// the board is event-sourced, so a restart rehydrates it (the in-memory projection would not).
    #[tokio::test]
    async fn upsert_is_durable_across_provider_instances() {
        let dir = TempDir::new().unwrap();
        let log = || Arc::new(EventLog::new(Some(dir.path().to_path_buf())));

        // Write a Ready slot through one provider instance.
        EventLogMerges::new(log())
            .repo("ws")
            .await
            .unwrap()
            .upsert_slot(&slot("b1", "feat/b1", MergeSlotState::Ready))
            .await
            .unwrap();

        // A brand-new provider (simulating a restart) still sees it by replaying the log.
        let repo = EventLogMerges::new(log()).repo("ws").await.unwrap();
        let slots = repo.list_slots().await.unwrap();
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].bead, "b1");
        assert_eq!(slots[0].state, MergeSlotState::Ready);
        assert_eq!(repo.get_slot("b1").await.unwrap().unwrap().state, MergeSlotState::Ready);
    }

    /// The full lifecycle round-trips through the event stream: each upsert appends the event its
    /// state implies, and a later read derives the current state from the replayed variants.
    #[tokio::test]
    async fn lifecycle_round_trips_through_the_stream() {
        let dir = TempDir::new().unwrap();
        let merges = EventLogMerges::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))));
        let repo = merges.repo("ws").await.unwrap();

        for state in [MergeSlotState::Ready, MergeSlotState::Merging, MergeSlotState::Merged] {
            repo.upsert_slot(&slot("b1", "feat/b1", state)).await.unwrap();
        }
        assert_eq!(repo.get_slot("b1").await.unwrap().unwrap().state, MergeSlotState::Merged);

        // An unknown bead is absent, not an error.
        assert!(repo.get_slot("nope").await.unwrap().is_none());
    }

    /// Per-workspace isolation is structural: a slot written under one tenant is invisible to
    /// another (the log is path-partitioned per workspace).
    #[tokio::test]
    async fn workspaces_are_isolated() {
        let dir = TempDir::new().unwrap();
        let merges = EventLogMerges::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))));

        merges
            .repo("alpha")
            .await
            .unwrap()
            .upsert_slot(&slot("b1", "x", MergeSlotState::Ready))
            .await
            .unwrap();

        assert!(merges.repo("beta").await.unwrap().list_slots().await.unwrap().is_empty());
        assert_eq!(merges.repo("alpha").await.unwrap().list_slots().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn skills_write_then_read_round_trips_per_workspace() {
        // hq-agent-observability.7: a SkillWriter append lands in the workspace's skills stream and
        // the WorkspaceSkills replay sees it; retire removes it; another tenant never sees either.
        let dir = TempDir::new().unwrap();
        let skills = EventLogSkills::new(Arc::new(EventLog::new(Some(dir.path().to_path_buf()))));

        skills
            .append(
                "acme",
                SkillEvent::Registered {
                    skill: "graphify".into(),
                    label: "Graphify".into(),
                    description: "knowledge graph".into(),
                    default_scopes: vec!["graph.read".into()],
                    now_secs: 1,
                },
            )
            .await
            .unwrap();

        let cat = skills.catalog("acme").await.unwrap();
        assert!(cat.get("graphify").is_some(), "registered skill is read back");
        // Tenant isolation: another workspace's catalog is empty.
        assert!(skills.catalog("beta").await.unwrap().get("graphify").is_none());

        skills
            .append("acme", SkillEvent::Retired { skill: "graphify".into(), now_secs: 2 })
            .await
            .unwrap();
        assert!(
            skills.catalog("acme").await.unwrap().get("graphify").is_none(),
            "retired skill is gone after replay"
        );
    }
}
