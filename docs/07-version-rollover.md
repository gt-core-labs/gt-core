# Version rollover playbook (v1 → v2)

How a module evolves a published surface — an **event kind**, an **MCP tool**, or
a **DTO/contract** — without breaking the consumers already on the old version.

The governing invariant for events is [docs/03 Rule 5](03-architecture-guardrails.md#rule-5-events-are-versioned-replay-safe-additive):
versioned, replay-safe, additive. This document is the *operational procedure* that
applies that rule across all three surfaces and ties together the primitives that
already ship for it.

## When you need a rollover

Roll a surface to `v2` when a change is **breaking** — it removes, renames, or
changes the wire shape of an existing field. Additive-only changes (a new optional
field consumers can ignore) do **not** need a new version.

| Surface | Breaking change looks like | Coexistence primitive |
|---------|----------------------------|-----------------------|
| Event   | drop/rename/retype a field of `<m>.<noun>.v1` | dual-emit `.v1` + `.v2`, `Capability::emits` |
| MCP tool| change a tool's input/output schema | second tool `…​.v2`, registry + `meta.help` |
| DTO/contract | change a JSON Schema the frontend codegens from | semver **major** bump + new frozen baseline |

The three rotate together when one drives the others (e.g. an event reshape that a
DTO mirrors), but each is independently versioned — bump only what actually broke.

## The sequence

```
1. emit/serve v2 ALONGSIDE v1   (never replace in place)
2. mark v1 deprecated           (warn, don't remove)
3. migrate consumers to v2
4. freeze the new baseline       (CI guards drift)
5. drop v1 after the deprecation window
```

Steps 1–2 are non-breaking and ship together. Step 5 is the only breaking step and
happens in a *later* release, after every known consumer is on v2.

### 1. Add v2 alongside v1

- **Events** — keep the old code path emitting `<m>.<noun>.v1`; emit
  `<m>.<noun>.v2` from the new path. Declare **both** in `Capability::emits`
  (Rule 5, step 3). The event log is append-only — never rewrite v1 rows to v2.
- **MCP tools** — register a sibling tool under the `<module-id>.<action>.<verb>`
  namespace with a `.v2` action (e.g. `beads.create.v2`). The old tool keeps
  working; both appear in `meta.help`.
- **DTO/contracts** — emit the new JSON Schema and bump the contract's semver
  **major** (`contracts.4` enforces: a breaking change that does not bump major
  fails CI). The frontend codegens the new types from the new schema version.

### 2. Mark v1 deprecated

- **Events** — flag the kind+version via the deprecation API (`events.6`); the
  warning surfaces to emitters/subscribers still on v1 without removing it.
- **MCP tools** — note the superseding tool in the deprecated tool's description so
  it shows in `meta.help`.
- **DTO/contracts** — the previous version file stays frozen; the deprecation is
  implied by the existence of the higher major.

### 3. Migrate consumers

Move every downstream — frontend subscriptions, other modules' cross-module
subscribers (read-only projections only, Rule 5 forbidden #4), CLI/automation — to
the v2 surface. Track this out-of-band; the platform cannot tell you who still
reads v1, only that v1 is still declared.

### 4. Freeze the new baseline

Snapshot the v2 schemas as the frozen baseline so `contracts.3`'s CI drift check
guards them: any later unintended change to a frozen version fails the build. This
is what makes v2 a stable contract rather than a moving target.

### 5. Drop v1

Only after the deprecation window closes and no consumer reads v1:

- remove the v1 emit path (the `.v1` rows already in the append-only log stay —
  replay still matches them; you remove the *producer*, not the history);
- drop `<X>.v1` from `Capability::emits`;
- unregister the `.v1` MCP tool;
- the old major contract version file is retained for historical replay/audit but
  no longer emitted.

## Checklist

- [ ] Change is genuinely breaking (else: additive, no rollover).
- [ ] v2 added; v1 still emitted/served.
- [ ] `Capability::emits` (or the MCP registry) declares both versions.
- [ ] DTO bumped **major** + new schema version committed (`contracts.4` green).
- [ ] v1 marked deprecated (`events.6` / tool description).
- [ ] New baseline frozen (`contracts.3` drift check green).
- [ ] Consumers migrated and verified on v2.
- [ ] (Later release) v1 producer removed; append-only log untouched; replay gate green.

## See also

- [docs/03 Rule 5](03-architecture-guardrails.md#rule-5-events-are-versioned-replay-safe-additive) — the event-versioning invariant.
- [docs/03 Rule 7](03-architecture-guardrails.md#rule-7-keep-the-replay-byte-for-byte-gate-green) — the replay gate that proves old events still decode.
