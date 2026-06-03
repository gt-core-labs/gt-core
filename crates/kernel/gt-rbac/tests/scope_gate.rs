//! RBAC / scope gate (`hq-mcp-test.4`): deny-by-default + per-actor resolution.
//!
//! The MCP server resolves a [`Scope`] per connection from the `X-Actor` header
//! against the boot [`RbacConfig`] and runs [`Scope::check`] before every tool
//! (see `IssuesServer::authorize`). This gate pins that contract at the seam the
//! server depends on:
//!
//! - an actor absent from the config folds to a **closed** scope — every tool
//!   denied (deny-by-default, never admin),
//! - two distinct `X-Actor`s resolve to their own allow-lists off one config,
//! - `validate_only` permits `.validate` but rejects `.execute`,
//! - trailing-`*` globs scope a whole namespace without leaking a sibling,
//! - a JWT workspace claim derives admin / member / denied by its scope strings.
//!
//! Pure in-process — no transport, no store.

use gt_rbac::{RbacConfig, Scope, WORKSPACE_ADMIN, WORKSPACE_MEMBER};

/// Two actors off one config: a full-authority host and a validate-only watcher
/// scoped to a single namespace.
fn config() -> RbacConfig {
    RbacConfig::from_toml(
        r#"
        [actors.claude-host]
        allow = ["*"]
        validate_only = false

        [actors.watcher]
        allow = ["issues.*"]
        validate_only = true
        "#,
    )
    .expect("valid rbac toml")
}

#[test]
fn unknown_actor_is_denied_every_tool() {
    // The server resolves an actor missing from the config to `Scope::denied`.
    let scope = Scope::from_rbac(&config(), "ghost");
    for tool in [
        "issues.create.validate",
        "issues.create.execute",
        "meta.help.execute",
        "workspace.list",
    ] {
        assert!(
            scope.check(tool).is_err(),
            "deny-by-default: unknown actor must be denied `{tool}`"
        );
    }
    // `Scope::denied` directly carries the same posture.
    assert!(Scope::denied("nobody").check("issues.create.validate").is_err());
}

#[test]
fn per_actor_allow_lists_resolve_independently() {
    let cfg = config();

    // The host actor: full authority — both variants of every namespace.
    let host = Scope::from_rbac(&cfg, "claude-host");
    assert_eq!(host.actor, "claude-host");
    host.check("issues.create.execute").expect("host may execute issues");
    host.check("workspace.create.execute").expect("host may execute workspace");

    // The watcher actor: a different allow-list off the same config.
    let watcher = Scope::from_rbac(&cfg, "watcher");
    assert_eq!(watcher.actor, "watcher");
    watcher
        .check("issues.create.validate")
        .expect("watcher may validate in-scope");
    assert!(
        watcher.check("workspace.list").is_err(),
        "watcher's `issues.*` allow-list must not reach `workspace.*`"
    );
}

#[test]
fn validate_only_blocks_execute_but_allows_validate() {
    let watcher = Scope::from_rbac(&config(), "watcher");
    watcher
        .check("issues.update.validate")
        .expect("validate is permitted");
    assert!(
        watcher.check("issues.update.execute").is_err(),
        "validate_only scope must reject the .execute variant"
    );
}

#[test]
fn namespace_glob_does_not_leak_a_sibling_namespace() {
    let cfg = RbacConfig::from_toml(
        r#"
        [actors.rigger]
        allow = ["rig.*"]
        "#,
    )
    .expect("valid toml");
    let rigger = Scope::from_rbac(&cfg, "rigger");
    rigger.check("rig.add.execute").expect("rig.* covers rig.add");
    rigger.check("rig.list").expect("rig.* covers rig.list");
    assert!(
        rigger.check("rigging.add").is_err(),
        "`rig.*` must not match the `rigging` namespace by prefix"
    );
    assert!(rigger.check("issues.create.execute").is_err());
}

#[test]
fn workspace_claim_scopes_derive_admin_member_or_denied() {
    // admin claim → full tool reach.
    let admin = Scope::from_workspace_claim("alice", &[WORKSPACE_ADMIN.to_string()]);
    admin.check("workspace.create.execute").expect("admin provisions tenants");

    // member claim → operational tools, but not the tenant-catalog mutations.
    let member = Scope::from_workspace_claim("bob", &[WORKSPACE_MEMBER.to_string()]);
    member.check("issues.create.execute").expect("member operates issues");
    member.check("workspace.list").expect("member reads the catalog");
    assert!(
        member.check("workspace.create.execute").is_err(),
        "member must not provision a tenant"
    );

    // any other scope string → denied (never admin by default).
    let other = Scope::from_workspace_claim("eve", &["some.unknown.scope".to_string()]);
    assert!(other.check("issues.create.validate").is_err());
}
