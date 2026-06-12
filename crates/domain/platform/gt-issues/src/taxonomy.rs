//! Closed-set bead taxonomy values (hq-core-mcp.3).
//!
//! The issues tracker's `domain[]` is a CLOSED set, not free strings: every
//! variant is a deliberate part of the taxonomy (doc 14 §3), so a typo or an
//! out-of-set value is rejected at deserialization instead of silently entering
//! the dependency graph. Adding a variant is a conscious change — the
//! `meta.report-gap` path is how an unknown domain gets surfaced as a
//! `hq-gap-domain-<slug>` bead rather than relaxing this enum.
//!
//! The wire form keeps the dotted-namespace shape (`kernel.events`,
//! `orch.merge`, …) so JSON payloads read like the spec; each variant pins it
//! with `#[serde(rename = …)]`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A semantic domain a bead affects. Replaces the previous free-form
/// `Vec<String>` so `issues.create` / `issues.update` reject an unknown domain.
///
/// Each variant's meaning is its dotted wire name (the `#[serde(rename)]`); the
/// per-variant doc is intentionally omitted as redundant.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub enum Domain {
    #[serde(rename = "kernel.events")]
    KernelEvents,
    #[serde(rename = "kernel.bus")]
    KernelBus,
    #[serde(rename = "kernel.audit")]
    KernelAudit,
    #[serde(rename = "kernel.telemetry")]
    KernelTelemetry,
    #[serde(rename = "kernel.plugin")]
    KernelPlugin,
    #[serde(rename = "kernel.channel")]
    KernelChannel,
    #[serde(rename = "kernel.root")]
    KernelRoot,
    /// The semantic graph index (gt-graphindex) — the kernel-tier indexer the
    /// per-rig knowledge graph is built over (hq-graphrig); added by
    /// hq-gap-domain-taxonomy-add-discriminators-kernel-graphindex-role-graphwarden.
    #[serde(rename = "kernel.graphindex")]
    KernelGraphindex,
    #[serde(rename = "lifecycle.agent")]
    LifecycleAgent,
    #[serde(rename = "lifecycle.polecat")]
    LifecyclePolecat,
    #[serde(rename = "orch.scheduling")]
    OrchScheduling,
    #[serde(rename = "orch.patrol")]
    OrchPatrol,
    #[serde(rename = "orch.merge")]
    OrchMerge,
    #[serde(rename = "orch.quota")]
    OrchQuota,
    #[serde(rename = "orch.convoy")]
    OrchConvoy,
    /// The login / PTY session driver (gt-login) — added for the gt-web port wave
    /// (hq-gap-domain-taxonomy-…-doc-14); gt-login sits in the orchestration tier.
    #[serde(rename = "orch.login")]
    OrchLogin,
    #[serde(rename = "platform.feed")]
    PlatformFeed,
    #[serde(rename = "platform.notify")]
    PlatformNotify,
    #[serde(rename = "platform.rig")]
    PlatformRig,
    #[serde(rename = "platform.wisp")]
    PlatformWisp,
    /// The skills capability registry (gt-skills) — added for the gap the
    /// closed set was missing (hq-gap-domain-platform-skills, folded here).
    #[serde(rename = "platform.skills")]
    PlatformSkills,
    /// The dock-terminal / PTY-attach surface (gt-terminal) — added for the gt-web
    /// port wave (hq-gap-domain-taxonomy-…-doc-14); gt-terminal sits in the platform tier.
    #[serde(rename = "platform.terminal")]
    PlatformTerminal,
    /// The per-workspace document store (gt-documents) — supporting material (.md
    /// content + binary attachments) a model reads as context when resolving a bead
    /// (docs/11, hq-docs). Added by hq-docs-api.4.
    #[serde(rename = "platform.documents")]
    PlatformDocuments,
    #[serde(rename = "role.sheriff")]
    RoleSheriff,
    #[serde(rename = "role.deacon")]
    RoleDeacon,
    #[serde(rename = "role.refinery")]
    RoleRefinery,
    #[serde(rename = "role.witness")]
    RoleWitness,
    #[serde(rename = "role.mayor")]
    RoleMayor,
    /// The per-rig graph warden (gt-graphwarden) — the role that owns rebuilding
    /// and serving a rig's knowledge graph (hq-graphrig); added by
    /// hq-gap-domain-taxonomy-add-discriminators-kernel-graphindex-role-graphwarden.
    #[serde(rename = "role.graphwarden")]
    RoleGraphwarden,
    #[serde(rename = "bin.gt")]
    BinGt,
    #[serde(rename = "bin.gt-web")]
    BinGtWeb,
    #[serde(rename = "bin.gt-mcp")]
    BinGtMcp,
    #[serde(rename = "bin.gt-mcp-cli")]
    BinGtMcpCli,
    /// The gt-core MCP server bin (hq-core-host.3) — a real crate now.
    #[serde(rename = "bin.gt-mcp-server")]
    BinGtMcpServer,
    #[serde(rename = "store.dolt")]
    StoreDolt,
    #[serde(rename = "store.pg")]
    StorePg,
    #[serde(rename = "store.beads")]
    StoreBeads,
    /// The S3-compatible object store (gt-store-blob, MinIO) holding binary
    /// attachment bytes for the document store (docs/11, hq-docs-store.2). Added by
    /// hq-docs-api.4.
    #[serde(rename = "store.blob")]
    StoreBlob,
    #[serde(rename = "fe.web")]
    FeWeb,
    #[serde(rename = "fe.docs")]
    FeDocs,
    #[serde(rename = "deploy.compose")]
    DeployCompose,
    #[serde(rename = "deploy.dolt")]
    DeployDolt,
    #[serde(rename = "deploy.k8s")]
    DeployK8s,
    #[serde(rename = "deploy.talos")]
    DeployTalos,
    #[serde(rename = "docs.spec")]
    DocsSpec,
    #[serde(rename = "meta.gap")]
    MetaGap,
}

/// A bead's `issue_type` — a CLOSED set (hq-48ca5c), same rationale as
/// [`Domain`]: a free-form string let any value (`"banana"`) pass shape
/// validation, while `domain` and `phase` were already closed. Typing the wire
/// field also surfaces the allowed variants as a JSON-schema `enum`, so an MCP
/// client can offer a picker instead of a blank text box.
///
/// Existing rows keep whatever legacy string they carry — the read path
/// (`IssueRow`/`IssueDetail`) stays `String`; only `issues.create` /
/// `issues.update` narrow.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum IssueType {
    Epic,
    Task,
    Bug,
    Spike,
    Chore,
    Feature,
}

impl IssueType {
    /// The lowercase wire/store form (the `hq.issues.issue_type` column value).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Epic => "epic",
            Self::Task => "task",
            Self::Bug => "bug",
            Self::Spike => "spike",
            Self::Chore => "chore",
            Self::Feature => "feature",
        }
    }

    /// Whether this is the epic type — the NN-16 `external_ref` exemption.
    pub fn is_epic(self) -> bool {
        matches!(self, Self::Epic)
    }
}

/// A bead's dispatch policy — a CLOSED set (gtcore-1acbcf C1), same rationale as
/// [`IssueType`]: whether an agent may claim the bead is an explicit operator
/// decision, so an out-of-set value is rejected at deserialization rather than
/// silently landing in the column.
///
/// `Manual` is the default ([`Default`]): nothing becomes agent-dispatchable by
/// accident — `auto` is opt-in. On the wire the value rides per-bead as an
/// OPTIONAL field: an omitted/`NULL` value means "inherit from the `child_of`
/// parent chain", resolved in read by
/// [`resolve_dispatch`](crate::dispatch::resolve_dispatch); a chain that yields
/// no value resolves to `Manual`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Dispatch {
    Auto,
    #[default]
    Manual,
}

impl Dispatch {
    /// Parse a wire/SQL token (`"auto"`/`"manual"`) into the typed policy.
    /// `None` for any other string so an out-of-set value is rejected at the
    /// frontier instead of silently entering the column (mirrors
    /// [`IssuePhase::parse`](gt_store_dolt::IssuePhase::parse)).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(Self::Auto),
            "manual" => Some(Self::Manual),
            _ => None,
        }
    }

    /// The lowercase wire/store form (the `hq.issues.dispatch` column value).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Manual => "manual",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Dispatch, Domain, IssueType};

    #[test]
    fn round_trips_dotted_wire_form() {
        let d: Domain = serde_json::from_str("\"orch.merge\"").unwrap();
        assert_eq!(d, Domain::OrchMerge);
        assert_eq!(serde_json::to_string(&d).unwrap(), "\"orch.merge\"");
    }

    #[test]
    fn rejects_unknown_domain() {
        assert!(serde_json::from_str::<Domain>("\"orch.bogus\"").is_err());
    }

    #[test]
    fn issue_type_round_trips_lowercase_wire_form() {
        let t: IssueType = serde_json::from_str("\"task\"").unwrap();
        assert_eq!(t, IssueType::Task);
        assert_eq!(serde_json::to_string(&t).unwrap(), "\"task\"");
        assert_eq!(IssueType::Epic.as_str(), "epic");
        assert!(IssueType::Epic.is_epic());
        assert!(!IssueType::Bug.is_epic());
    }

    #[test]
    fn issue_type_rejects_out_of_set_value() {
        assert!(serde_json::from_str::<IssueType>("\"banana\"").is_err());
        // Wire form is lowercase only — capitalized rejected too.
        assert!(serde_json::from_str::<IssueType>("\"Epic\"").is_err());
    }

    #[test]
    fn dispatch_round_trips_lowercase_wire_form() {
        let d: Dispatch = serde_json::from_str("\"auto\"").unwrap();
        assert_eq!(d, Dispatch::Auto);
        assert_eq!(serde_json::to_string(&d).unwrap(), "\"auto\"");
        assert_eq!(Dispatch::Manual.as_str(), "manual");
        assert_eq!(Dispatch::parse("auto"), Some(Dispatch::Auto));
        assert_eq!(Dispatch::parse("manual"), Some(Dispatch::Manual));
        // Default is the opt-in-safe Manual.
        assert_eq!(Dispatch::default(), Dispatch::Manual);
    }

    #[test]
    fn dispatch_rejects_out_of_set_value() {
        assert!(serde_json::from_str::<Dispatch>("\"always\"").is_err());
        // Wire form is lowercase only — capitalized rejected too.
        assert!(serde_json::from_str::<Dispatch>("\"Auto\"").is_err());
        assert_eq!(Dispatch::parse("Auto"), None);
        assert_eq!(Dispatch::parse(""), None);
    }

    #[test]
    fn skills_variant_present() {
        let d: Domain = serde_json::from_str("\"platform.skills\"").unwrap();
        assert_eq!(d, Domain::PlatformSkills);
    }

    #[test]
    fn documents_variants_present() {
        let docs: Domain = serde_json::from_str("\"platform.documents\"").unwrap();
        assert_eq!(docs, Domain::PlatformDocuments);
        let blob: Domain = serde_json::from_str("\"store.blob\"").unwrap();
        assert_eq!(blob, Domain::StoreBlob);
    }

    #[test]
    fn login_and_terminal_variants_present() {
        let login: Domain = serde_json::from_str("\"orch.login\"").unwrap();
        assert_eq!(login, Domain::OrchLogin);
        let terminal: Domain = serde_json::from_str("\"platform.terminal\"").unwrap();
        assert_eq!(terminal, Domain::PlatformTerminal);
    }

    #[test]
    fn graph_variants_present() {
        let index: Domain = serde_json::from_str("\"kernel.graphindex\"").unwrap();
        assert_eq!(index, Domain::KernelGraphindex);
        let warden: Domain = serde_json::from_str("\"role.graphwarden\"").unwrap();
        assert_eq!(warden, Domain::RoleGraphwarden);
    }
}
