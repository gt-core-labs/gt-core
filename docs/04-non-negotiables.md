# Non-negotiables (ported from gastown)

Hard invariants. Every rule here has a battle scar behind it — they exist because past violations broke replay, leaked tenants, or fragmented the kernel. **Breaking one of these is a stop-the-line event**, not a tradeoff.

Cross-link with [03-architecture-guardrails.md](03-architecture-guardrails.md). This doc is the *source of truth* for the principles; guardrails is the *how-to*.

---

## 1. Dependency direction (single-source)

**Domain depends only on kernel. A domain never imports another domain.**

- A → B direct call is forbidden. Cross-domain communication is event-driven, full stop.
- Adapter (`gt-store-*`) depends on domain, never the inverse. Domains define ports; adapters implement them.
- `gt-feed` is special: depends **only on `gt-audit`**, knows zero domains, reads the type-erased log.
- `dyn` + `#[async_trait]` allowed ONLY in `gt-plugin` (heterogeneous observer plugins). Everywhere else: static dispatch via generics.

**Provenance:** [gastown apps/api/docs/01-architecture.md §"Regla de dependencias"](file:///home/nixos/gastown/apps/api/docs/01-architecture.md), [README.md §"Principios no negociables"](file:///home/nixos/gastown/apps/api/docs/README.md).

---

## 2. Sync core, async at the edges

**The pure logic is never async. The compiler enforces this; `async fn` colors all callers.**

| async (I/O, waits) | sync (pure) |
|---|---|
| `gt-store-*`, `gt-web`, transports | every `state.rs` / `model.rs` |
| supervisor / probe (processes, net) | plan calculation, state derivation |
| bus relay to I/O tasks | serde, state machines, matching, `Command::{validate,execute}` |

**Why:** async contagion poisons replay determinism. A sync core can replay byte-for-byte from a recorded log without runtime coupling.

**Corollary:** the `Command` trait is sync and does NOT use `#[async_trait]`. If a command needs I/O, it returns an event the bus routes to an async edge effect.

**Provenance:** [01-architecture.md §"Async en los bordes"](file:///home/nixos/gastown/apps/api/docs/01-architecture.md).

---

## 3. Determinism — core reads no clock, no random

**The pure core never calls `Instant::now()`, `OffsetDateTime::now_utc()`, or `rand`.** Time and identity (`event_id`, `correlation_id`, `ts`) enter through the `Envelope`, generated at the edge by async producers.

- Replay re-feeds recorded envelopes verbatim. A core that decides "expired" by looking at the wall clock during replay would diverge from the original run.
- Timeouts and expirations are **real events** emitted by edge producers (`expectations.rs`, `witness.rs`), persisted to the log, and consumed by the core. They are never recomputed.

**If the Paso 3 / byte-for-byte replay gate fails: suspect #1 is a clock or random call leaking into the core, or a timeout computed instead of read from the log.**

**Provenance:** [06-observability.md §"Regla de determinismo"](file:///home/nixos/gastown/apps/api/docs/06-observability.md). This is the rule that makes everything else work.

---

## 4. Mutable state lives in an actor

**No `Arc<Mutex<T>>` shared across tasks.** Each domain with mutable state owns ONE task; everyone else sends messages via `mpsc`.

```rust
// gt-agent: the SessionRegistry lives in one task. Nobody else touches it.
enum AgentMsg {
    Add(Session),
    Remove(String),
    Snapshot(oneshot::Sender<Vec<Session>>),
}
```

This kills three bug classes:

- Holding a `MutexGuard` across `.await` (not `Send`).
- Send/Sync bound propagation through the stack.
- Most borrow-checker complaints — data MOVES through channels, isn't SHARED by reference.

**Exception:** read-heavy quasi-immutable data (config, rotation plans) uses `arc-swap` — lockless readers, writer atomic-replaces.

**Provenance:** [01-architecture.md §"Modelo de actores"](file:///home/nixos/gastown/apps/api/docs/01-architecture.md).

---

## 5. Events are owned enums, not trait objects

**`enum DomainEvent { … }` with `#[derive(Serialize, Deserialize)]`.** No `Box<dyn Event>`, no lifetimes, no trait objects in the bus or the audit log.

- Exhaustive `match` is the compile-time check that no variant is forgotten by reducers / replay paths.
- Owned (no borrowed `&str`, no `Cow`) so envelopes serialize/deserialize round-trip cleanly into the log.

**Provenance:** [README.md §"Principios no negociables"](file:///home/nixos/gastown/apps/api/docs/README.md).

---

## 6. Storage rules (two-engine consistency)

1. **Dolt is the only source of truth for beads.** Nothing else writes beads.
2. **Zero cross-store transactions.** No transaction spans Dolt + Postgres. Each engine writes its own; integration via events on the bus, never cross-write.
3. **Outbox per writing store.** Entity + outbox row in one transaction; relay publishes to bus → audit JSONB.
4. **Feed is read-only over the stream.** Projects events; never writes back.
5. **Bus + audit log are the integration spine** between engines that don't speak SQL to each other.

**Provenance:** [04-persistence.md §"Reglas de consistencia"](file:///home/nixos/gastown/apps/api/docs/04-persistence.md).

---

## 7. Idempotency

**Every durable queue / outbox can redeliver after a crash (at-least-once; "exactly-once" does not exist).** Consumers deduplicate by `event_id` from the envelope.

Without this rule: a retry of a dispatch spawns the agent twice — classic semantic bug.

**Provenance:** [04-persistence.md §"Idempotencia"](file:///home/nixos/gastown/apps/api/docs/04-persistence.md).

---

## 8. State machines reject illegal transitions

**Every aggregate models its lifecycle as enum + `transition()` fn that returns `Err(InvalidTransition)` for illegal moves.**

```rust
pub enum SessionState { Spawned, Working, Done, Killed }
impl Session {
    pub fn transition(&mut self, to: SessionState) -> Result<(), InvalidTransition> {
        match (self.state, to) {
            (Spawned, Working) => { self.state = to; Ok(()) }
            (Working, Done)    => { self.state = to; Ok(()) }
            (_, _)             => Err(InvalidTransition { from: self.state, to }),
        }
    }
}
```

A session that jumps `Spawned → Done` is rejected at the type level. The pattern surfaces semantic gaps; never "log and continue".

**Provenance:** [06-observability.md §"State machines explícitas"](file:///home/nixos/gastown/apps/api/docs/06-observability.md).

---

## 9. Migrations are append-only

**`sqlx::migrate!` checksum-validates applied files. Never edit an applied migration — add a new one.**

- Each migration: `migrations/<module-id>/<YYYYMMDDHHMMSS>_<name>.sql`.
- Reordering history corrupts every replica that ran the old checksum.
- Disabling a module retains its migrations (`hq-mod-migrate.3`). Opt-in purge only.

**Provenance:** [04-persistence.md §"Migraciones"](file:///home/nixos/gastown/apps/api/docs/04-persistence.md).

---

## 10. Worktree workflow

**Worktree, never town root.** The auto-revert hook in the town root snaps any branch checkout back to `main`. Operate from a worktree on persistent FS.

```bash
git worktree add /home/nixos/gt-core-wt-<bead-id> -b <bead-id> main
```

- **Never `/tmp/wt-*`** — tmpfs RAM, reboot wipes uncommitted work.
- **Before claiming:** `git log --all --grep <bead-id>` to avoid hijacking a branch another agent is mid-edit on.
- **Close:** `cargo build` + `cargo test --workspace` green → ff-merge to main from town root (not worktree) → push → delete branch. Close the bead with the commit SHA.
- **Rebase on main BEFORE merge.** Hotspot files: composition roots (the app entrypoint's `main.rs`, `root.rs`). Conflicts are usually additive unions.

**Provenance:** [11-cutover-roadmap.md §"Reglas para todos los agentes"](file:///home/nixos/gastown/apps/api/docs/11-cutover-roadmap.md), and the [memory](../../.claude/projects/-home-nixos-gt-core/memory/feedback_worktree_hijack_parallel.md) on parallel hijack risk.

---

## 11. Replay byte-for-byte gate

**Any change touching `gt-events`, a reducer, or the event-log writer must pass the Paso 3 gate.**

1. Snapshot state pre-change.
2. Replay full event log under the new code.
3. Diff against snapshot → must equal zero.

This is the gate that makes every other guarantee real. If the gate fails, suspect #1 is rule #3 (clock/random in core) or rule #5 (event shape change without v2).

**Provenance:** [06-observability.md](file:///home/nixos/gastown/apps/api/docs/06-observability.md), Paso 3 / 9.B.

---

## 12. MCP is canonical for agent operations

**Agents talk to the orchestrator and to each other ONLY via MCP.** RBAC, scope checks, audit logging, replay safety — all enforced at the MCP boundary.

- ❌ `docker exec dolt sql ...` — operator-only escape hatch for forensics, NOT a development path.
- ❌ Direct PG / Dolt writes from CLI scripts that mutate hq state.
- ✅ `gt-mcp-cli call <tool>.<verb>.execute --args ...` always.

If a needed tool doesn't exist, file a gap via `meta.report_gap` instead of bypassing.

**Provenance:** [memory:mcp-canonical-for-agents](../../.claude/projects/-home-nixos-gt-core/memory/feedback_mcp_canonical_for_agents.md), [memory:dolt-sql-for-hq-beads](../../.claude/projects/-home-nixos-gt-core/memory/feedback_dolt_sql_for_hq_beads.md).

---

## 13. Bead-driven development

**Every development pass goes through a claimed bead.** No code change without a bead id.

- Claim before coding: transition to `working` + comment "claimed at <commit>".
- Re-read the epic + `bd list` (or `gt://issues?external_ref=<epic>`) just before starting (multi-agent state can drift in minutes — see [memory:check-commits-before-claim](../../.claude/projects/-home-nixos-gt-core/memory/feedback_check_commits_before_claim.md)).
- Close the bead with the commit SHA when merged.

Found a gap that no bead covers? `meta.report_gap` mints `hq-gap-<slug>-<ts>`. Pick that up; do not freelance.

**Provenance:** [11-cutover-roadmap.md §"Reglas para todos los agentes"](file:///home/nixos/gastown/apps/api/docs/11-cutover-roadmap.md), [memory:multi-bead-epic-discipline](../../.claude/projects/-home-nixos-gt-core/memory/feedback_multi_bead_epic_discipline.md).

---

## 14. Single tokio Runtime, lives in the app entrypoint

**One `tokio::Runtime`, created in the binary** — the app's entrypoint crate's `main` (its `[[bin]]`; for gt-core's consumers that is gastown). Domain crates never create a runtime; they receive handles.

- A domain test runs under `#[tokio::test]` in isolation.
- Multiple runtimes deadlock under shared `mpsc` channels — a class of bug that disappears when the rule is followed.

**Provenance:** [01-architecture.md §"Async en los bordes"](file:///home/nixos/gastown/apps/api/docs/01-architecture.md).

---

## 15. Workspace boundary (gt-core-specific, deriving from above)

**Every mutating command carries `workspace_id` server-injected from auth context.** Never from URL, body, or MCP payload.

- All domain repos take `&WorkspaceId` as first arg.
- **Per-workspace projection data is isolated by Postgres schema-per-workspace**, not a shared-table `workspace_id` column. Each workspace owns a schema `ws_<slug>` (`gt_store_pg::schema_for`); a `WorkspacePool` sets `search_path` to that schema on every connection checkout, so unqualified table names resolve to the tenant's own copy. Cross-tenant leak is prevented structurally — a query physically cannot see another tenant's rows — which is stronger than a `workspace_id` `WHERE` clause that a buggy query can forget.
- The shared `public` schema holds **only** cross-tenant catalogs — chiefly the `workspaces` table itself. Any table that must live in `public` (e.g. `flag_overrides`) carries a `workspace_id` column with FK to `workspaces.id` so even shared-schema rows stay tenant-anchored.
- SSE/WS channels keyed `(workspace_id, kind)` — cross-tenant leak is the bug class this prevents.

This is a gt-core extension of the gastown principles to the multi-tenant target.

**Provenance:** new — derived from rules 1, 6 applied to `hq-mt`. See [03-architecture-guardrails.md §6](03-architecture-guardrails.md). The schema-per-workspace clause supersedes the original "every projection table has a `workspace_id` column" wording, which mandated a shared-table model incompatible with the ratified `hq-mt-data` design (`schema_for` + `WorkspacePool`, commit `0e74b84`; `gt_create_workspace_schema`, `4ebb298`). Resolution of gap `hq-gap-spec-conflict-hq-mt-data-partitioning-model`, 2026-05-31.

---

## 16. Bead taxonomy is epic → sub-epic → bead (enforced in code)

**The tracking hierarchy is exactly three levels and is mandatory by design, not convention.**

| Level | Representation | Example |
|-------|----------------|---------|
| **Epic** | a bead with `issue_type=epic` | `hq-mod`, `hq-mt` |
| **Sub-epic** | the shared `external_ref` of its beads (the canonical grouping key) | `hq-mt-cli`, `hq-mod-flags` |
| **Bead** | `issue_type=task`/`spike`, id `<sub-epic>.<n>` | `hq-mt-cli.7` |

- Every non-epic bead MUST carry `external_ref` = its **sub-epic** (never the bare epic, never empty).
- Bead id MUST match `<sub-epic>.<n>` where `<sub-epic>` equals its `external_ref`.
- Sub-epic → epic linkage is by name prefix (`hq-mt-*` → `hq-mt`). There is no "parent" column.
- Bead → bead ordering is `depends_on` (dependency graph), orthogonal to the hierarchy.

**The code enforces this, not just the docs.** `issues.create` / `issues.update` validation in gt-mcp MUST reject, at the MCP boundary (the same way rule 15 rejects a spoofed `workspace_id`): a non-epic bead with empty `external_ref`, or an id that does not match `<external_ref>.<n>`. Epics are exempt. A validation gap → `meta.report_gap`; do not relax the rule. Enforcement tracked in `hq-mod-mcp.10`.

**Provenance:** new — gt-core process invariant. Battle scar: this session found the CLI work scattered with `external_ref` set inconsistently and ids that didn't reflect their sub-epic, making `gt://issues?external_ref=<sub-epic>` an unreliable grouping query.

---

## Stop-the-line protocol

If a bead description, a code review comment, or your own draft conflicts with one of these:

1. **Stop.** Do not "just this once" — the cost of a wrong assumption is multi-day rollback.
2. **File a gap** via `meta.report_gap` MCP tool with a descriptive slug.
3. **Wait** for an explicit decision before resuming.

Past violations have caused: split-brain Dolt corruption, cross-tenant leak (caught in test, not prod), Step 3 gate red for a week, mass bead duplication after bd auto-export race. The rules are the scar tissue.

---

## Provenance index

| Rule | Origin |
|------|--------|
| 1 | gastown apps/api/docs/01-architecture.md, README.md |
| 2 | 01-architecture.md |
| 3 | 06-observability.md (Regla de determinismo) |
| 4 | 01-architecture.md (Modelo de actores) |
| 5 | README.md (Principios no negociables) |
| 6 | 04-persistence.md (Reglas de consistencia) |
| 7 | 04-persistence.md (Idempotencia) |
| 8 | 06-observability.md (State machines) |
| 9 | 04-persistence.md (Migraciones) |
| 10 | 11-cutover-roadmap.md, memory:worktree-hijack |
| 11 | 06-observability.md, Paso 3 / 9.B |
| 12 | memory:mcp-canonical-for-agents |
| 13 | 11-cutover-roadmap.md, memory:multi-bead-epic-discipline |
| 14 | 01-architecture.md |
| 15 | gt-core-extension over rules 1, 6 for hq-mt |
| 16 | gt-core process invariant (bead taxonomy), enforced in gt-mcp validate; hq-mod-mcp.10 |
