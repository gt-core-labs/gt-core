//! The **closed scope vocabulary** (hq-rbac.1): the grammar every authorization scope
//! string must satisfy before it can ride a JWT, be granted by a role, or gate a route.
//!
//! A scope is a dotted `<resource>.<verb>` label (e.g. `beads.write`) — the same wire shape
//! [`gt_module::Scope`](../../gt-module/src/scope.rs) enforces for module capabilities and the
//! REST guard checks. This crate owns the *authorization* half of that contract: the set of
//! verbs is **closed** (an admin defining a role cannot invent `beads.frobnicate` and silently
//! grant nothing), while the resource segment stays open so a new module can declare its own
//! namespace without editing this list.
//!
//! Two grants live outside the `<resource>.<verb>` grammar and are accepted verbatim:
//!
//! - [`SCOPE_WILDCARD`] (`"*"`) — the superuser grant the seeded admin carries; it authorizes
//!   every REST route ([`gt_module::CallerScopes::all`]) and every MCP tool
//!   ([`Scope::from_workspace_claim`](crate::Scope::from_workspace_claim)).
//!
//! [`validate_scope`] is the single gate: role CRUD (hq-rbac.4) calls it before persisting a
//! role's scopes, so a typo is rejected at write time rather than discovered as a silent
//! permission hole at request time.

use crate::error::AppError;

/// The wildcard superuser grant — every scope, every tool. Accepted by [`validate_scope`]
/// verbatim (it is not a `<resource>.<verb>` label).
pub const SCOPE_WILDCARD: &str = "*";

/// The **closed** set of verbs a scope's second segment may take. Resources stay open (any
/// kebab slug), but the verb must be one of these — the closure that makes the vocabulary
/// finite and a misspelled verb a load-time error.
///
/// `admin`/`member` are the two workspace-role verbs (`workspace.admin`, `workspace.member`);
/// the rest are the CRUD/operational verbs the modules' capabilities and the REST guard use
/// (`read`/`write` are what [`gt_module`]'s route guard derives from the HTTP method). `exec`
/// gates capabilities that *run* something rather than read/mutate state — the interactive
/// terminal (`terminal.exec`, hq-terminal) is the first. `attach` gates the dock-terminal
/// WebSocket attach surface (`terminal.attach`, hq-term-dock).
pub const SCOPE_VERBS: &[&str] = &[
    "read", "write", "create", "update", "delete", "submit", "list", "info", "admin", "member",
    "exec", "attach",
];

/// True when `verb` is in the closed [`SCOPE_VERBS`] set.
pub fn is_known_verb(verb: &str) -> bool {
    SCOPE_VERBS.contains(&verb)
}

/// A non-empty lowercase kebab-case slug: `[a-z0-9]+` groups joined by single `-`, with no
/// leading, trailing, or doubled hyphen. The same shape `gt_module`'s scope/`ModuleId` use,
/// reimplemented here so this kernel crate stays free of a `gt-module` dependency.
fn is_kebab_segment(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return false;
    }
    let mut prev_dash = false;
    for c in s.chars() {
        match c {
            'a'..='z' | '0'..='9' => prev_dash = false,
            '-' => {
                if prev_dash {
                    return false;
                }
                prev_dash = true;
            }
            _ => return false,
        }
    }
    true
}

/// Validate one scope string against the closed vocabulary.
///
/// Accepts:
/// - [`SCOPE_WILDCARD`] (`"*"`) — the superuser grant, verbatim.
/// - exactly one `<resource>.<verb>` split where `resource` is a lowercase kebab slug and
///   `verb` is in the closed [`SCOPE_VERBS`] set.
///
/// Everything else — wrong arity, a non-kebab resource, or an unknown verb — is an
/// [`AppError::Validation`] naming the offending input, so role CRUD can surface a precise
/// rejection. Deny-by-default's write-time counterpart: an unrecognised scope never reaches
/// persistence.
pub fn validate_scope(scope: &str) -> Result<(), AppError> {
    if scope == SCOPE_WILDCARD {
        return Ok(());
    }
    let mut parts = scope.split('.');
    let (Some(resource), Some(verb), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(AppError::Validation(format!(
            "scope {scope:?} is not `<resource>.<verb>` (or the `*` wildcard)"
        )));
    };
    if !is_kebab_segment(resource) {
        return Err(AppError::Validation(format!(
            "scope {scope:?}: resource {resource:?} is not lowercase kebab-case"
        )));
    }
    if !is_known_verb(verb) {
        return Err(AppError::Validation(format!(
            "scope {scope:?}: verb {verb:?} is not one of {SCOPE_VERBS:?}"
        )));
    }
    Ok(())
}

/// Validate every scope in `scopes`, short-circuiting on the first rejection. The bulk gate role
/// CRUD (hq-rbac.4) runs over a role's full scope list before persisting it.
pub fn validate_scopes<I, S>(scopes: I) -> Result<(), AppError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    for scope in scopes {
        validate_scope(scope.as_ref())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_resource_verb_for_every_known_verb() {
        for verb in SCOPE_VERBS {
            let scope = format!("beads.{verb}");
            assert!(validate_scope(&scope).is_ok(), "expected {scope:?} valid");
        }
    }

    #[test]
    fn accepts_the_wildcard() {
        assert!(validate_scope(SCOPE_WILDCARD).is_ok());
    }

    #[test]
    fn accepts_the_workspace_role_scopes() {
        // The two workspace-role grants ride the same grammar (verb ∈ {admin, member}).
        validate_scope("workspace.admin").unwrap();
        validate_scope("workspace.member").unwrap();
    }

    #[test]
    fn accepts_a_kebab_resource() {
        validate_scope("merge-queue.read").unwrap();
    }

    #[test]
    fn accepts_the_terminal_exec_scope() {
        // hq-terminal: the interactive PTY terminal gates on `terminal.exec`.
        validate_scope("terminal.exec").unwrap();
    }

    #[test]
    fn rejects_an_unknown_verb() {
        let err = validate_scope("beads.frobnicate").unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn rejects_wrong_arity() {
        assert!(validate_scope("beads").is_err());
        assert!(validate_scope("a.b.c").is_err());
        assert!(validate_scope("").is_err());
    }

    #[test]
    fn rejects_a_non_kebab_resource() {
        assert!(validate_scope("Beads.read").is_err());
        assert!(validate_scope("be_ads.read").is_err());
        assert!(validate_scope("-beads.read").is_err());
    }

    #[test]
    fn bulk_validation_short_circuits_on_the_first_bad_scope() {
        validate_scopes(["beads.read", "merge.submit", "*"]).unwrap();
        assert!(validate_scopes(["beads.read", "beads.nope"]).is_err());
    }
}
