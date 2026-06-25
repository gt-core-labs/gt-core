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

impl Domain {
    /// Every variant, in declaration order. The single source of truth for the
    /// technical domain set — the per-workspace `domain_catalog` seed for the
    /// `default`/gtcore workspace is built from this slice (gtcore-55d5fb H1) so
    /// the catalog can never silently diverge from the enum. Adding a `Domain`
    /// variant extends the seed automatically.
    pub const ALL: &'static [Domain] = &[
        Self::KernelEvents,
        Self::KernelBus,
        Self::KernelAudit,
        Self::KernelTelemetry,
        Self::KernelPlugin,
        Self::KernelChannel,
        Self::KernelRoot,
        Self::KernelGraphindex,
        Self::LifecycleAgent,
        Self::LifecyclePolecat,
        Self::OrchScheduling,
        Self::OrchPatrol,
        Self::OrchMerge,
        Self::OrchQuota,
        Self::OrchConvoy,
        Self::OrchLogin,
        Self::PlatformFeed,
        Self::PlatformNotify,
        Self::PlatformRig,
        Self::PlatformWisp,
        Self::PlatformSkills,
        Self::PlatformTerminal,
        Self::PlatformDocuments,
        Self::RoleSheriff,
        Self::RoleDeacon,
        Self::RoleRefinery,
        Self::RoleWitness,
        Self::RoleMayor,
        Self::RoleGraphwarden,
        Self::BinGt,
        Self::BinGtWeb,
        Self::BinGtMcp,
        Self::BinGtMcpCli,
        Self::BinGtMcpServer,
        Self::StoreDolt,
        Self::StorePg,
        Self::StoreBeads,
        Self::StoreBlob,
        Self::FeWeb,
        Self::FeDocs,
        Self::DeployCompose,
        Self::DeployDolt,
        Self::DeployK8s,
        Self::DeployTalos,
        Self::DocsSpec,
        Self::MetaGap,
    ];

    /// The dotted wire/store form (the `#[serde(rename)]`), e.g.
    /// `Domain::OrchMerge` → `"orch.merge"`. Matches the JSON form
    /// [`Serialize`] produces, but without going through serde — the catalog
    /// seed and any non-JSON consumer (SQL keys) read the canonical key here.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KernelEvents => "kernel.events",
            Self::KernelBus => "kernel.bus",
            Self::KernelAudit => "kernel.audit",
            Self::KernelTelemetry => "kernel.telemetry",
            Self::KernelPlugin => "kernel.plugin",
            Self::KernelChannel => "kernel.channel",
            Self::KernelRoot => "kernel.root",
            Self::KernelGraphindex => "kernel.graphindex",
            Self::LifecycleAgent => "lifecycle.agent",
            Self::LifecyclePolecat => "lifecycle.polecat",
            Self::OrchScheduling => "orch.scheduling",
            Self::OrchPatrol => "orch.patrol",
            Self::OrchMerge => "orch.merge",
            Self::OrchQuota => "orch.quota",
            Self::OrchConvoy => "orch.convoy",
            Self::OrchLogin => "orch.login",
            Self::PlatformFeed => "platform.feed",
            Self::PlatformNotify => "platform.notify",
            Self::PlatformRig => "platform.rig",
            Self::PlatformWisp => "platform.wisp",
            Self::PlatformSkills => "platform.skills",
            Self::PlatformTerminal => "platform.terminal",
            Self::PlatformDocuments => "platform.documents",
            Self::RoleSheriff => "role.sheriff",
            Self::RoleDeacon => "role.deacon",
            Self::RoleRefinery => "role.refinery",
            Self::RoleWitness => "role.witness",
            Self::RoleMayor => "role.mayor",
            Self::RoleGraphwarden => "role.graphwarden",
            Self::BinGt => "bin.gt",
            Self::BinGtWeb => "bin.gt-web",
            Self::BinGtMcp => "bin.gt-mcp",
            Self::BinGtMcpCli => "bin.gt-mcp-cli",
            Self::BinGtMcpServer => "bin.gt-mcp-server",
            Self::StoreDolt => "store.dolt",
            Self::StorePg => "store.pg",
            Self::StoreBeads => "store.beads",
            Self::StoreBlob => "store.blob",
            Self::FeWeb => "fe.web",
            Self::FeDocs => "fe.docs",
            Self::DeployCompose => "deploy.compose",
            Self::DeployDolt => "deploy.dolt",
            Self::DeployK8s => "deploy.k8s",
            Self::DeployTalos => "deploy.talos",
            Self::DocsSpec => "docs.spec",
            Self::MetaGap => "meta.gap",
        }
    }

    /// The coarse tier prefix of the dotted key (`kernel`, `orch`, `platform`,
    /// `role`, `bin`, `store`, `fe`, `deploy`, `docs`, `meta`) — the segment
    /// before the first `.`. The catalog stores this as the optional `tier`
    /// column so a UI can group domains without re-parsing the key.
    pub fn tier(self) -> &'static str {
        self.as_str().split('.').next().unwrap_or("")
    }

    /// Whether this domain is RESERVED — present in every catalog and not an
    /// operator-editable business domain. `meta.gap` is the reserved domain the
    /// auto-created gap beads carry (gtcore-55d5fb H1); a reserved row is seeded
    /// into every workspace catalog (technical and template alike).
    pub fn is_reserved(self) -> bool {
        matches!(self, Self::MetaGap)
    }
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

/// A bead's `role_scope` — the responsible-role discriminator, a CLOSED set
/// (gtcore-b45a2d), same rationale as [`IssueType`]/[`Dispatch`]: the field was
/// free-form (`"Responsible role discriminator. Free-form; persisted verbatim."`),
/// so any value (`"banana"`) passed shape validation. Typing it rejects an
/// out-of-set value at deserialization and surfaces the allowed variants as a
/// JSON-schema `enum`, so an MCP client offers a role picker instead of a blank
/// text box.
///
/// The variants are the system's agent-role catalog, the union of two sources:
/// the `role.*` discriminators of [`Domain`] (sheriff/deacon/refinery/witness/
/// mayor/graphwarden) and the session roles `SessionRole`/`DogKind` enumerate in
/// gt-agent (which add `polecat`, `dog`, `overseer`). The wire/store form is the
/// bare lowercase role name (e.g. `sheriff`) — NOT the dotted `role.sheriff`
/// `Domain` form — matching `SessionRole::as_str` and the legacy column values.
///
/// `role_scope` is OPTIONAL (not every bead pins a role): the wire field stays
/// `Option<RoleScope>`, an omitted/`None` value storing SQL `NULL`. Existing rows
/// keep whatever legacy string they carry — the read path (`IssueRow`/
/// `IssueDetail`) stays `String`; only `issues.create` / `issues.update` narrow.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum RoleScope {
    Mayor,
    Polecat,
    Sheriff,
    Refinery,
    Witness,
    Deacon,
    Overseer,
    Dog,
    Graphwarden,
}

impl RoleScope {
    /// The lowercase wire/store form (the `hq.issues.role_scope` column value).
    /// Matches `gt_agent::SessionRole::as_str` so the tracker and the agent
    /// runtime agree on a role's canonical name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mayor => "mayor",
            Self::Polecat => "polecat",
            Self::Sheriff => "sheriff",
            Self::Refinery => "refinery",
            Self::Witness => "witness",
            Self::Deacon => "deacon",
            Self::Overseer => "overseer",
            Self::Dog => "dog",
            Self::Graphwarden => "graphwarden",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Dispatch, Domain, IssueType, RoleScope};

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
    fn role_scope_round_trips_lowercase_wire_form() {
        let r: RoleScope = serde_json::from_str("\"sheriff\"").unwrap();
        assert_eq!(r, RoleScope::Sheriff);
        assert_eq!(serde_json::to_string(&r).unwrap(), "\"sheriff\"");
        // The catalog covers both Domain's role.* set and gt-agent's session roles.
        assert_eq!(RoleScope::Graphwarden.as_str(), "graphwarden");
        assert_eq!(RoleScope::Polecat.as_str(), "polecat");
        assert_eq!(RoleScope::Overseer.as_str(), "overseer");
        assert_eq!(RoleScope::Dog.as_str(), "dog");
        for token in [
            "mayor",
            "polecat",
            "sheriff",
            "refinery",
            "witness",
            "deacon",
            "overseer",
            "dog",
            "graphwarden",
        ] {
            let parsed: RoleScope = serde_json::from_str(&format!("\"{token}\"")).unwrap();
            assert_eq!(parsed.as_str(), token);
        }
    }

    #[test]
    fn role_scope_rejects_out_of_set_value() {
        assert!(serde_json::from_str::<RoleScope>("\"banana\"").is_err());
        // Wire form is lowercase only — capitalized rejected too.
        assert!(serde_json::from_str::<RoleScope>("\"Sheriff\"").is_err());
        // The dotted Domain form is NOT the role_scope wire form.
        assert!(serde_json::from_str::<RoleScope>("\"role.sheriff\"").is_err());
    }

    #[test]
    fn all_matches_serde_wire_form_and_has_no_dupes() {
        use std::collections::HashSet;
        // `as_str` must agree with serde's `#[serde(rename)]` for every variant
        // in `ALL` — they are the two faces of the same wire name, and the
        // catalog seed relies on `as_str` matching what the write path validates.
        let mut seen = HashSet::new();
        for &d in Domain::ALL {
            let via_serde = serde_json::to_string(&d).unwrap();
            let via_as_str = format!("\"{}\"", d.as_str());
            assert_eq!(via_serde, via_as_str, "as_str diverges from serde for {d:?}");
            assert!(seen.insert(d.as_str()), "duplicate key {} in Domain::ALL", d.as_str());
            // Round-trip back from the wire form to catch a missing ALL entry's twin.
            let back: Domain = serde_json::from_str(&via_as_str).unwrap();
            assert_eq!(back, d);
        }
        // The technical set the bead documents (~45 values) — pin the count so a
        // dropped variant trips the test rather than silently shrinking the seed.
        assert_eq!(Domain::ALL.len(), 46);
    }

    #[test]
    fn meta_gap_is_the_reserved_domain() {
        assert!(Domain::MetaGap.is_reserved());
        assert_eq!(Domain::MetaGap.as_str(), "meta.gap");
        assert_eq!(Domain::MetaGap.tier(), "meta");
        // Exactly one reserved domain in the technical set.
        assert_eq!(Domain::ALL.iter().filter(|d| d.is_reserved()).count(), 1);
        // A non-reserved sample, and tier extraction.
        assert!(!Domain::OrchMerge.is_reserved());
        assert_eq!(Domain::OrchMerge.tier(), "orch");
        assert_eq!(Domain::KernelGraphindex.tier(), "kernel");
    }

    #[test]
    fn graph_variants_present() {
        let index: Domain = serde_json::from_str("\"kernel.graphindex\"").unwrap();
        assert_eq!(index, Domain::KernelGraphindex);
        let warden: Domain = serde_json::from_str("\"role.graphwarden\"").unwrap();
        assert_eq!(warden, Domain::RoleGraphwarden);
    }
}
