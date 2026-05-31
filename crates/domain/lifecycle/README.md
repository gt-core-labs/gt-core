# crates/domain/lifecycle/

State-machine entities. Reserved for crates that migrate from gastown in Phase 4:

- `gt-agent` — agent identity + claim lifecycle
- `gt-polecat` — polecat session state-machine

Pattern: each entity owns a state reducer + repo port + actor (see gt-workspace as reference once it's implemented).
