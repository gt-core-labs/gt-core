//! [`ExecutionType`] + [`PluginExecutor`] — turning a claim into real execution
//! (`hq-mod-dogs.4`).
//!
//! `hq-mod-dogs.1` defined the [`Dog`](crate::Dog) worker and `.2` the
//! [`DogDispatcher`](crate::DogDispatcher) that matches a claim to an idle Dog.
//! This bead supplies the *execution* step the lifecycle's
//! [`Running`](crate::DogStatus::Running) state stands for: an
//! [`ExecutionType`] describing **how** a plugin's claim runs, and a
//! [`PluginExecutor`] that validates a request and dispatches it to the matching
//! backend.
//!
//! ## The backend seam (NN#1 / NN#2)
//!
//! Actually spawning an agent, running a script, or invoking a wrapped binary is
//! side-effecting I/O — it belongs at the edge, never in the sync replay core
//! (non-negotiable #2). So the executor performs none of it directly: it owns an
//! [`ExecBackend`] port and dispatches to it. `ExecBackend` is `async` + `dyn`-
//! friendly — the observer/plugin exception non-negotiable #1 carves out — and
//! the real process-spawning adapter lands with the runtime wiring (the
//! per-workspace pool, `hq-mod-dogs.8`, and the end-to-end test, `.9`). Tests
//! here drive a fake backend, so the dispatch logic and the synchronous
//! validation are exercised without touching the OS.

use async_trait::async_trait;

use crate::dog::DogReport;

/// Which execution strategy a plugin's claim uses.
///
/// Mirrors the `ExecutionType` of `gt-plugin`'s descriptor (the loop this crate
/// closes, per the crate docs); `gt-plugin` migrates up in Phase 4, so this is
/// the in-repo model until then. `#[non_exhaustive]` so a new strategy can be
/// added without breaking matches.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionType {
    /// Spawn an agent session, identified by the persona that should handle it.
    Agent {
        /// The agent persona to run (e.g. `sheriff`, `refinery`).
        persona: String,
    },
    /// Run a shell script.
    Script {
        /// The script body / command line to execute.
        command: String,
    },
    /// Invoke an existing executable, wrapping it with arguments.
    ExecWrapper {
        /// The program to invoke.
        program: String,
        /// Arguments passed to the program, in order.
        args: Vec<String>,
    },
}

/// The discriminant of an [`ExecutionType`], free of its payload.
///
/// Lets diagnostics and a [`PluginExecutor`] name the strategy without cloning
/// the (potentially large) command or arg list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionKind {
    /// An [`ExecutionType::Agent`].
    Agent,
    /// An [`ExecutionType::Script`].
    Script,
    /// An [`ExecutionType::ExecWrapper`].
    ExecWrapper,
}

impl std::fmt::Display for ExecutionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ExecutionKind::Agent => "agent",
            ExecutionKind::Script => "script",
            ExecutionKind::ExecWrapper => "exec-wrapper",
        };
        f.write_str(s)
    }
}

impl ExecutionType {
    /// The strategy discriminant, without the payload.
    pub fn kind(&self) -> ExecutionKind {
        match self {
            ExecutionType::Agent { .. } => ExecutionKind::Agent,
            ExecutionType::Script { .. } => ExecutionKind::Script,
            ExecutionType::ExecWrapper { .. } => ExecutionKind::ExecWrapper,
        }
    }

    /// Reject a structurally invalid request before any backend runs it.
    ///
    /// A strategy with an empty selector — blank persona, blank command, blank
    /// program — is a wiring mistake, not a runnable task; catching it here keeps
    /// the (side-effecting) backend off the hook for input validation.
    fn validate(&self) -> Result<(), ExecError> {
        let blank = match self {
            ExecutionType::Agent { persona } => persona.trim().is_empty(),
            ExecutionType::Script { command } => command.trim().is_empty(),
            ExecutionType::ExecWrapper { program, .. } => program.trim().is_empty(),
        };
        if blank {
            return Err(ExecError::EmptySpec { kind: self.kind() });
        }
        Ok(())
    }
}

/// Why a [`PluginExecutor`] refused a request before dispatching it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    /// The claim reference was empty — there is nothing to attribute the run to.
    EmptyClaim,
    /// The [`ExecutionType`]'s selector (persona/command/program) was blank.
    EmptySpec {
        /// Which strategy carried the blank selector.
        kind: ExecutionKind,
    },
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::EmptyClaim => write!(f, "claim reference must not be empty"),
            ExecError::EmptySpec { kind } => write!(f, "{kind} execution has an empty spec"),
        }
    }
}

impl std::error::Error for ExecError {}

/// The I/O port a [`PluginExecutor`] dispatches a validated request to.
///
/// One method per call so an adapter can run the work however the strategy
/// demands (spawn a session, shell out, exec a binary) and report a coarse
/// [`DogReport`]. `async` + `dyn`-friendly under the non-negotiable #1 plugin
/// exception; the real adapter lands with the runtime wiring (`hq-mod-dogs.8`).
#[async_trait]
pub trait ExecBackend: Send + Sync {
    /// Run `claim` under `execution` and report the outcome.
    ///
    /// Called only after [`PluginExecutor::execute`] has validated both, so an
    /// implementation may assume a non-empty claim and a non-blank spec.
    async fn run(&self, claim: &str, execution: &ExecutionType) -> DogReport;
}

/// Validates an execution request and dispatches it to an [`ExecBackend`].
///
/// Construct with a backend, then call [`execute`](PluginExecutor::execute) per
/// claim. The synchronous validation (non-empty claim, non-blank spec) runs
/// before the backend is touched, so a malformed request never reaches the I/O
/// edge.
pub struct PluginExecutor<B> {
    backend: B,
}

impl<B: ExecBackend> PluginExecutor<B> {
    /// Build an executor over `backend`.
    pub fn new(backend: B) -> Self {
        PluginExecutor { backend }
    }

    /// Borrow the backend (diagnostics / wiring).
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Validate `claim` + `execution`, then run it on the backend.
    ///
    /// Returns [`ExecError`] without touching the backend if the claim is empty
    /// or the strategy's selector is blank; otherwise yields the backend's
    /// [`DogReport`].
    pub async fn execute(
        &self,
        claim: &str,
        execution: &ExecutionType,
    ) -> Result<DogReport, ExecError> {
        if claim.trim().is_empty() {
            return Err(ExecError::EmptyClaim);
        }
        execution.validate()?;
        Ok(self.backend.run(claim, execution).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Backend that records what it was asked to run and returns a canned report.
    #[derive(Default)]
    struct SpyBackend {
        report: Option<DogReport>,
        seen: Mutex<Vec<(String, ExecutionKind)>>,
    }

    impl SpyBackend {
        fn completing() -> Self {
            SpyBackend {
                report: Some(DogReport::Completed),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExecBackend for SpyBackend {
        async fn run(&self, claim: &str, execution: &ExecutionType) -> DogReport {
            self.seen
                .lock()
                .unwrap()
                .push((claim.to_string(), execution.kind()));
            self.report.clone().unwrap_or(DogReport::Completed)
        }
    }

    fn agent() -> ExecutionType {
        ExecutionType::Agent { persona: "sheriff".into() }
    }

    #[test]
    fn kind_reflects_the_variant() {
        assert_eq!(agent().kind(), ExecutionKind::Agent);
        assert_eq!(
            ExecutionType::Script { command: "ls".into() }.kind(),
            ExecutionKind::Script
        );
        assert_eq!(
            ExecutionType::ExecWrapper { program: "git".into(), args: vec![] }.kind(),
            ExecutionKind::ExecWrapper
        );
    }

    #[tokio::test]
    async fn execute_dispatches_a_valid_request_to_the_backend() {
        let exec = PluginExecutor::new(SpyBackend::completing());
        let report = exec.execute("premerge-42", &agent()).await.unwrap();
        assert_eq!(report, DogReport::Completed);
        let seen = exec.backend().seen.lock().unwrap();
        assert_eq!(seen.as_slice(), &[("premerge-42".to_string(), ExecutionKind::Agent)]);
    }

    #[tokio::test]
    async fn execute_routes_each_strategy_to_the_backend() {
        let exec = PluginExecutor::new(SpyBackend::completing());
        exec.execute("a", &ExecutionType::Script { command: "echo hi".into() })
            .await
            .unwrap();
        exec.execute(
            "b",
            &ExecutionType::ExecWrapper { program: "git".into(), args: vec!["status".into()] },
        )
        .await
        .unwrap();
        let seen = exec.backend().seen.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[
                ("a".to_string(), ExecutionKind::Script),
                ("b".to_string(), ExecutionKind::ExecWrapper),
            ]
        );
    }

    #[tokio::test]
    async fn execute_surfaces_a_backend_failure_report() {
        let backend = SpyBackend {
            report: Some(DogReport::Failed("boom".into())),
            ..Default::default()
        };
        let exec = PluginExecutor::new(backend);
        let report = exec.execute("x", &agent()).await.unwrap();
        assert_eq!(report, DogReport::Failed("boom".into()));
    }

    #[tokio::test]
    async fn empty_claim_is_rejected_before_the_backend() {
        let exec = PluginExecutor::new(SpyBackend::completing());
        assert_eq!(exec.execute("   ", &agent()).await, Err(ExecError::EmptyClaim));
        // Backend never ran.
        assert!(exec.backend().seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn blank_spec_is_rejected_per_strategy() {
        let exec = PluginExecutor::new(SpyBackend::completing());
        assert_eq!(
            exec.execute("x", &ExecutionType::Agent { persona: " ".into() }).await,
            Err(ExecError::EmptySpec { kind: ExecutionKind::Agent })
        );
        assert_eq!(
            exec.execute("x", &ExecutionType::Script { command: "".into() }).await,
            Err(ExecError::EmptySpec { kind: ExecutionKind::Script })
        );
        assert_eq!(
            exec.execute(
                "x",
                &ExecutionType::ExecWrapper { program: "".into(), args: vec!["a".into()] }
            )
            .await,
            Err(ExecError::EmptySpec { kind: ExecutionKind::ExecWrapper })
        );
        assert!(exec.backend().seen.lock().unwrap().is_empty());
    }
}
