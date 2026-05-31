# crates/bins/

Application binaries that ship alongside gt-core. Currently empty.

Apps (gastown, future-app-X) keep their own binaries. gt-core only adds a binary here when it needs a CLI of its own (rare; `gt-mod-contracts` already ships its own `bin/`).

Convention: one folder per binary, `Cargo.toml` declares `[[bin]]`.
