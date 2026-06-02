//! `gt-planner` — the autonomous **Plan** tier of the MAPE-K loop (hq-auto.2).
//!
//! Where the dispatcher (hq-auto.1) is *Execute* and the reconciler is *Monitor +
//! Analyze*, the planner is the expert that turns a goal into work. It reads a
//! goal/epic intent and decomposes it into **beads + `depends_on` edges + phase**,
//! emitting that decision as **graph structure, never prose** ([`decompose`] →
//! [`Plan`] of [`CreateBead`]). Authorship is recorded (`created_by`) so the graph
//! audits who generated each bead.
//!
//! Three guardrails distinguish it from a worker:
//!
//! - **RBAC** — the planner carries a [`planner_scope`] (create / edit edges /
//!   advance phase), deliberately **disjoint** from the [`worker_scope`] (claim /
//!   close). Generation and execution are separated so neither tier holds the
//!   whole pipeline. See [`scope`].
//! - **Park irreversible forks** — a grave intent is routed to the park queue
//!   (hq-auto.3), not auto-scheduled, when no human is on-loop.
//! - **Intent edges only** — the planner supplies the semantic edges an expert
//!   knows; the mechanical Cargo-graph edges are auto-derived by the reconciler
//!   (§A), so the planner never hand-encodes the build graph.
//!
//! Before planning, the planner consults the memory corpus (hq-auto.8) for the
//! operating rules and goal-relevant lessons it must honor: [`planning_brief`].
//!
//! Pure and dependency-light: the whole crate is total functions over plain data,
//! unit-tested without a store, a model, or async. Applying a [`Plan`] (issuing
//! `issues.create` under the planner scope) lives a tier up.

mod epoch;
mod meta;
mod plan;
mod scope;

pub use epoch::{
    decide_epoch, DiscardReason, EpochFitness, EpochGates, EpochVerdict, Evolution, HaltReason,
    LoopControl,
};
pub use meta::{learn, reflect, BeadOutcome, Cycle, Outcome, Reflection};
pub use plan::{
    decompose, planning_brief, CreateBead, Goal, Intent, ParkedFork, Plan, PlanningBrief,
};
pub use scope::{planner_scope, worker_scope, PLANNER_ALLOW, WORKER_ALLOW};
