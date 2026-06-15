//! Cross-workspace delegation grants (B4, gtcore-3f3c57).
//!
//! `a2a.delegate`/`a2a.discover` historically only saw rigs in the **caller's own**
//! workspace — the [cross-workspace leak gate](gt_mcp_server) makes a token for
//! workspace A unable to touch workspace B, full stop. For a multi-tenant deploy
//! that wants one tenant to hand work to another (a shared platform team, a
//! sub-tenant farming work out to a central rig pool), that confinement is too
//! coarse: there is no way to say "tenant `acme` MAY delegate into `platform`."
//!
//! This module is the **explicit, deny-by-default grant** that opens exactly that
//! seam and nothing more. It is a pure, config-driven allow-list — the same shape
//! as [`Scope`](gt_rbac::Scope): an origin/destination pair the operator writes
//! into one env var (`GT_A2A_CROSS_WS_GRANTS`), parsed once at the composition
//! root, consulted by the `a2a` handler before it mints a bead in another tenant's
//! tracker. Unset ⇒ empty ⇒ no cross-workspace delegation at all (the legacy
//! single-tenant behaviour, byte-for-byte).
//!
//! ## Grant syntax
//!
//! Comma-separated `origin->dest` pairs; whitespace around tokens is trimmed and
//! empty/malformed entries are dropped (a typo can never *widen* the grant):
//!
//! ```text
//! GT_A2A_CROSS_WS_GRANTS="acme->platform, beta->platform, ops->*"
//! ```
//!
//! Either side may be the wildcard `*`: `ops->*` lets `ops` delegate into any
//! tenant, `*->platform` lets any tenant delegate into `platform`, and `*->*` is
//! the (explicit) open grant. A workspace may always delegate to **itself**
//! ([`allows`](CrossWsGrants::allows) returns `true` for `origin == dest`) without
//! a grant — the grant governs only the cross-tenant hop.

/// A parsed `origin -> dest` rule. Each side is either a concrete workspace slug
/// or the `*` wildcard.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Grant {
    origin: String,
    dest: String,
}

impl Grant {
    /// Does this rule authorise `origin -> dest`? A `*` on a side matches any value
    /// on that side.
    fn matches(&self, origin: &str, dest: &str) -> bool {
        (self.origin == "*" || self.origin == origin) && (self.dest == "*" || self.dest == dest)
    }
}

/// The cross-workspace delegation allow-list. Deny-by-default: an empty registry
/// authorises only same-workspace delegation.
#[derive(Debug, Clone, Default)]
pub struct CrossWsGrants {
    grants: Vec<Grant>,
}

impl CrossWsGrants {
    /// An empty grant set — no cross-workspace delegation is permitted (every
    /// `allows(a, b)` with `a != b` is `false`). The default.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Parse the `GT_A2A_CROSS_WS_GRANTS` spec: comma-separated `origin->dest`
    /// pairs. Whitespace is trimmed; an entry without a single `->`, or with an
    /// empty side, is silently dropped so a malformed clause can never *widen* the
    /// grant (deny-by-default is preserved bit-for-bit on a typo).
    pub fn parse(spec: &str) -> Self {
        let grants = spec
            .split(',')
            .filter_map(|entry| {
                let (origin, dest) = entry.split_once("->")?;
                let origin = origin.trim();
                let dest = dest.trim();
                if origin.is_empty() || dest.is_empty() {
                    return None;
                }
                Some(Grant {
                    origin: origin.to_string(),
                    dest: dest.to_string(),
                })
            })
            .collect();
        Self { grants }
    }

    /// `true` when delegation from `origin` into `dest` is authorised. A
    /// same-workspace hop (`origin == dest`) is always allowed; a cross-workspace
    /// hop requires a matching grant.
    pub fn allows(&self, origin: &str, dest: &str) -> bool {
        if origin == dest {
            return true;
        }
        self.grants.iter().any(|g| g.matches(origin, dest))
    }

    /// `true` when no cross-workspace grants are configured (the legacy
    /// single-tenant posture). Used at the composition root for the startup log.
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty()
    }

    /// Number of parsed grant rules — for diagnostics / the startup log line.
    pub fn len(&self) -> usize {
        self.grants.len()
    }

    /// Like [`allows`](Self::allows) but **without** the same-name auto-allow (A5, gtcore-f3a016).
    /// Used for rig-level RBAC, where `origin` is a workspace/actor and `target` is a *rig* name:
    /// an agent must hold an explicit `origin->rig` grant to delegate work onto that rig — there
    /// is no implicit "you may target the rig that happens to share your name". Deny-by-default
    /// among the configured rules (wildcards on either side still apply).
    pub fn allows_target(&self, origin: &str, target: &str) -> bool {
        self.grants.iter().any(|g| g.matches(origin, target))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_target_gates_rigs_with_no_self_shortcut() {
        // A5, gtcore-f3a016: rig-level RBAC reuses the grant syntax but DROPS the same-name
        // auto-allow — an agent needs an explicit `workspace->rig` grant to delegate onto a rig.
        let g = CrossWsGrants::parse("default->gtcore, ops->*");
        assert!(g.allows_target("default", "gtcore"));
        assert!(g.allows_target("ops", "anything"), "wildcard target still applies");
        // A rig sharing the origin's name is NOT implicitly allowed (unlike `allows`).
        assert!(!g.allows_target("gtcore", "gtcore"));
        assert!(g.allows("gtcore", "gtcore"), "the workspace variant still self-allows");
        // Ungranted pairs stay denied; an empty grant set denies everything.
        assert!(!g.allows_target("default", "gtweb"));
        assert!(!CrossWsGrants::empty().allows_target("a", "a"));
    }

    #[test]
    fn empty_grants_deny_cross_ws_but_allow_self() {
        let g = CrossWsGrants::empty();
        assert!(g.is_empty());
        // Same-workspace is always fine, even with no grants.
        assert!(g.allows("acme", "acme"));
        // Any cross-workspace hop is denied by default.
        assert!(!g.allows("acme", "platform"));
    }

    #[test]
    fn parse_reads_concrete_pairs() {
        let g = CrossWsGrants::parse("acme->platform, beta->platform");
        assert_eq!(g.len(), 2);
        assert!(g.allows("acme", "platform"));
        assert!(g.allows("beta", "platform"));
        // A pair that was never granted stays denied.
        assert!(!g.allows("acme", "beta"));
        assert!(!g.allows("platform", "acme"));
    }

    #[test]
    fn wildcard_origin_and_dest() {
        let any_origin = CrossWsGrants::parse("*->platform");
        assert!(any_origin.allows("acme", "platform"));
        assert!(any_origin.allows("zeta", "platform"));
        assert!(!any_origin.allows("acme", "other"));

        let any_dest = CrossWsGrants::parse("ops->*");
        assert!(any_dest.allows("ops", "platform"));
        assert!(any_dest.allows("ops", "acme"));
        assert!(!any_dest.allows("acme", "platform"));

        let open = CrossWsGrants::parse("*->*");
        assert!(open.allows("a", "b"));
        assert!(open.allows("x", "y"));
    }

    #[test]
    fn malformed_entries_are_dropped_never_widen() {
        // No arrow, empty sides, stray whitespace, a blank clause — all dropped.
        let g = CrossWsGrants::parse("acme platform, ->platform, acme->, , acme->platform");
        assert_eq!(g.len(), 1, "only the one well-formed pair survives");
        assert!(g.allows("acme", "platform"));
        assert!(!g.allows("acme", "beta"));
    }

    #[test]
    fn whitespace_is_trimmed_around_tokens() {
        let g = CrossWsGrants::parse("  acme  ->  platform  ");
        assert!(g.allows("acme", "platform"));
    }

    #[test]
    fn empty_spec_parses_to_empty() {
        assert!(CrossWsGrants::parse("").is_empty());
        assert!(CrossWsGrants::parse("   ").is_empty());
    }
}
