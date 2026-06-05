# crates/domain/roles/

Behavioral actors that watch state and react. Reserved for role crates migrating from the upstream app:

- `gt-sheriff` — pre-merge watchdog + dispatch timeout
- `gt-deacon` — daemon heartbeat consumer + e-stop drain
- `gt-refinery` — MERGE_READY queue processor
- `gt-witness` — polecat health monitor
- `gt-mayor` — global coordinator + escalations

Roles consume domain events; mutations go through CommandBus, never direct.
