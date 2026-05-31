//! MCP claim tools — `dog.claim.{validate,execute}`, `dog.release.execute`,
//! `dog.status.read` (`hq-mod-dogs.7`).
//!
//! The seam by which the Dog worker surfaces its lifecycle over MCP. A host
//! module registers these tools through the [`McpRegistry`] contribution API
//! (`hq-mod-mcp.1`) by calling [`register`]; clients then drive a Dog with the
//! `validate`/`execute` pair that mirrors the orchestrator's other tools
//! (`issues.*`, `merge.*`): `validate` judges an argument payload without
//! touching state, `execute` turns a validated payload into the
//! [`DogEvent`](crate::DogEvent) the [`DogDispatcher`](crate::DogDispatcher)
//! folds into the pool.
//!
//! ## Why this stays in the sync core
//!
//! This module performs **no I/O and holds no handles**. It does two pure
//! things: declare the tool descriptors (data) and parse a JSON argument payload
//! into a [`DogEvent`] (a total function). The actual application of the event —
//! and any persistence or async execution behind it — is the caller's job via
//! [`DogDispatcher::apply`](crate::DogDispatcher::apply) and
//! [`PluginExecutor`](crate::PluginExecutor), so the tool layer replays
//! byte-for-byte (non-negotiable #4) and carries nothing across an `await`.
//!
//! ## Mapping to the state machine
//!
//! - `dog.claim.execute` → [`DogEvent::Claimed`] (`Idle` → `Claimed`).
//! - `dog.release.execute` → [`DogEvent::Failed`] — releasing returns the held
//!   claim and moves the Dog back to `Idle` from `Claimed`/`Running`. There is no
//!   separate "release" reducer primitive; abandoning the claim *is* the release.
//! - `dog.status.read` is a query: it carries an optional Dog filter and never
//!   produces an event.
//!
//! The host binds the parsed event to its dispatcher; what a Dog is *allowed* to
//! claim (capacity, gate readiness) is enforced there, not here — this layer only
//! guarantees the payload is well-formed.

use serde::Deserialize;

use crate::dispatcher::DogEvent;
use crate::state::{DogId, DogIdError};
use gt_module::McpRegistry;

/// The module-id namespace every Dog MCP tool is published under. The
/// [`RootBuilder`](gt_module::RootBuilder) binds tool names to the contributing
/// module's id, so this must match the id the host registers the Dog module as.
pub const DOG_TOOL_NAMESPACE: &str = "dog";

/// Arguments for `dog.claim.{validate,execute}`: which Dog takes which claim.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ClaimArgs {
    /// The worker that should take the claim.
    pub dog: String,
    /// The claim reference being taken (e.g. a bead or merge id). Non-empty.
    pub claim: String,
}

/// Arguments for `dog.release.execute`: which Dog releases its held claim.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ReleaseArgs {
    /// The worker releasing its claim.
    pub dog: String,
}

/// Arguments for `dog.status.read`: an optional single-Dog filter.
///
/// `dog: None` requests the whole pool; `Some(id)` narrows to one worker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct StatusArgs {
    /// Restrict the read to this worker; omit for every Dog.
    #[serde(default)]
    pub dog: Option<String>,
}

/// Why a Dog tool payload was rejected before it could become a [`DogEvent`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DogToolError {
    /// The `dog` field was not a valid [`DogId`]; carries the reason verbatim.
    BadDog(DogIdError),
    /// The `claim` reference was empty or whitespace-only.
    EmptyClaim,
}

impl std::fmt::Display for DogToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DogToolError::BadDog(e) => write!(f, "invalid dog id: {e}"),
            DogToolError::EmptyClaim => write!(f, "claim reference must not be empty"),
        }
    }
}

impl std::error::Error for DogToolError {}

impl From<DogIdError> for DogToolError {
    fn from(e: DogIdError) -> Self {
        DogToolError::BadDog(e)
    }
}

/// Register the Dog claim tools into a module's [`McpRegistry`] (`hq-mod-dogs.7`).
///
/// Declares the four tools in a stable order with their JSON input schemas.
/// Chainable through `&mut McpRegistry` like every other contributor; call it
/// from the host module's `register_mcp_tools`.
pub fn register(reg: &mut McpRegistry) {
    reg.tool_with_schema(
        "dog.claim.validate",
        "Check that a Dog claim payload is well-formed without taking the claim",
        claim_schema(),
    )
    .tool_with_schema(
        "dog.claim.execute",
        "Take a claim on behalf of a Dog (Idle -> Claimed)",
        claim_schema(),
    )
    .tool_with_schema(
        "dog.release.execute",
        "Release a Dog's held claim, returning it to Idle",
        release_schema(),
    )
    .tool_with_schema(
        "dog.status.read",
        "Read the lifecycle status of one Dog, or the whole pool",
        status_schema(),
    );
}

/// Validate a `dog.claim.validate` payload: confirm it would yield a legal
/// [`DogEvent::Claimed`] without producing one. Pure; mutates nothing.
pub fn validate_claim(args: &ClaimArgs) -> Result<(), DogToolError> {
    claim_event(args).map(|_| ())
}

/// Turn a `dog.claim.execute` payload into the [`DogEvent::Claimed`] the
/// dispatcher applies. Rejects an empty dog id or claim reference.
pub fn claim_event(args: &ClaimArgs) -> Result<DogEvent, DogToolError> {
    let dog = DogId::new(args.dog.clone())?;
    if args.claim.trim().is_empty() {
        return Err(DogToolError::EmptyClaim);
    }
    Ok(DogEvent::Claimed { dog, claim: args.claim.clone() })
}

/// Turn a `dog.release.execute` payload into the [`DogEvent::Failed`] that
/// abandons the held claim and returns the Dog to `Idle`.
pub fn release_event(args: &ReleaseArgs) -> Result<DogEvent, DogToolError> {
    let dog = DogId::new(args.dog.clone())?;
    Ok(DogEvent::Failed { dog })
}

/// Resolve a `dog.status.read` payload into the optional [`DogId`] filter the
/// host applies to its pool. `None` means "every Dog".
pub fn status_filter(args: &StatusArgs) -> Result<Option<DogId>, DogToolError> {
    match &args.dog {
        None => Ok(None),
        Some(id) => Ok(Some(DogId::new(id.clone())?)),
    }
}

fn claim_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "dog": { "type": "string", "description": "Worker id taking the claim" },
            "claim": { "type": "string", "description": "Claim reference to take" }
        },
        "required": ["dog", "claim"],
        "additionalProperties": false
    })
}

fn release_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "dog": { "type": "string", "description": "Worker id releasing its claim" }
        },
        "required": ["dog"],
        "additionalProperties": false
    })
}

fn status_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "dog": { "type": "string", "description": "Restrict to one worker; omit for the whole pool" }
        },
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use gt_module::McpTool;

    #[test]
    fn register_declares_the_four_tools_in_order() {
        let mut reg = McpRegistry::new();
        register(&mut reg);
        let names: Vec<&str> = reg.tools().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "dog.claim.validate",
                "dog.claim.execute",
                "dog.release.execute",
                "dog.status.read",
            ]
        );
    }

    #[test]
    fn every_tool_name_obeys_the_namespacing_rule() {
        let mut reg = McpRegistry::new();
        register(&mut reg);
        for t in reg.tools() {
            let named = McpTool::new(t.name.clone(), "");
            let (module, _, _) = named
                .parse_name()
                .unwrap_or_else(|e| panic!("{}: {e}", t.name));
            assert_eq!(module, DOG_TOOL_NAMESPACE, "tool {} not under namespace", t.name);
        }
    }

    #[test]
    fn claim_event_maps_to_claimed() {
        let args = ClaimArgs { dog: "sheriff".into(), claim: "premerge-42".into() };
        assert_eq!(
            claim_event(&args).unwrap(),
            DogEvent::Claimed { dog: DogId::new("sheriff").unwrap(), claim: "premerge-42".into() }
        );
        // validate agrees and stays side-effect-free.
        assert!(validate_claim(&args).is_ok());
    }

    #[test]
    fn claim_rejects_empty_dog_and_claim() {
        assert_eq!(
            claim_event(&ClaimArgs { dog: "  ".into(), claim: "x".into() }),
            Err(DogToolError::BadDog(DogIdError::Empty))
        );
        assert_eq!(
            claim_event(&ClaimArgs { dog: "sheriff".into(), claim: "  ".into() }),
            Err(DogToolError::EmptyClaim)
        );
    }

    #[test]
    fn release_event_maps_to_failed() {
        let args = ReleaseArgs { dog: "sheriff".into() };
        assert_eq!(
            release_event(&args).unwrap(),
            DogEvent::Failed { dog: DogId::new("sheriff").unwrap() }
        );
    }

    #[test]
    fn release_rejects_empty_dog() {
        assert_eq!(
            release_event(&ReleaseArgs { dog: "".into() }),
            Err(DogToolError::BadDog(DogIdError::Empty))
        );
    }

    #[test]
    fn status_filter_handles_present_and_absent_dog() {
        assert_eq!(status_filter(&StatusArgs::default()).unwrap(), None);
        assert_eq!(
            status_filter(&StatusArgs { dog: Some("digest".into()) }).unwrap(),
            Some(DogId::new("digest").unwrap())
        );
        assert_eq!(
            status_filter(&StatusArgs { dog: Some("".into()) }),
            Err(DogToolError::BadDog(DogIdError::Empty))
        );
    }

    #[test]
    fn args_deserialize_from_mcp_payload() {
        let claim: ClaimArgs =
            serde_json::from_str(r#"{"dog":"sheriff","claim":"bead-9"}"#).unwrap();
        assert_eq!(claim, ClaimArgs { dog: "sheriff".into(), claim: "bead-9".into() });

        let status: StatusArgs = serde_json::from_str("{}").unwrap();
        assert_eq!(status, StatusArgs { dog: None });
    }

    #[test]
    fn claim_schema_requires_both_fields() {
        let schema = claim_schema();
        assert_eq!(schema["required"], serde_json::json!(["dog", "claim"]));
    }
}
