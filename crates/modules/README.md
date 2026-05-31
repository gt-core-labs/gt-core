# crates/modules/

Pluggable feature modules. Each is a self-contained crate that `impl GtModule` and plugs into the composition root with one line.

Reserved (none shipped yet):

- `mod-kanban` — Plane-style board over beads (depends on mod-beads + mod-rigs)
- `mod-pages` — Rich-text docs linked to rigs (depends on mod-rigs)
- `mod-cycles` — Sprint/iteration grouping (depends on mod-beads)
- `mod-intake` — Triage workflow before formal bead (depends on mod-beads)

Pattern (see `examples/mod-hello` once implemented):

```
crates/modules/mod-<name>/
├── Cargo.toml
├── src/
│   ├── lib.rs          impl GtModule
│   ├── module.rs       register(b: &mut RootBuilder)
│   ├── domain/         state + commands + events
│   ├── repo.rs         port + InMemory
│   ├── actor.rs        optional
│   ├── routes.rs       axum
│   ├── mcp.rs          MCP tools
│   ├── subscriptions.rs  cross-module event handlers
│   └── hooks.rs        HookHandler subscriptions
├── migrations/
├── contracts/v<N>.json
└── tests/
```

Frontend (UI bundle for nav/routes/widgets) lives in the separate frontend repo, codegen'd from `contracts/v<N>.json`.
