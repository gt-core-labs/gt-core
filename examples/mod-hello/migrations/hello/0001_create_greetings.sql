-- mod-hello, migration v1.
--
-- A module owns its schema. The builder collects this migration via
-- `GtModule::migrations`, namespaces it under the module id, and the migrate
-- loader applies it inside one transaction with its tracking insert. Authors
-- ship the SQL with `include_str!` so the migration body lives next to the
-- module, not inlined in Rust.
CREATE TABLE IF NOT EXISTS hello_greetings (
    id          BIGSERIAL PRIMARY KEY,
    recipient   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
