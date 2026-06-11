# gt-mcp-server — the gt-core MCP server (hq-core-host.3/.5).
#
# Multi-stage with cargo-chef (hq-docker-build-opt): deps are compiled in a
# dedicated `cook` layer keyed on Cargo.lock. Only the project-code layer
# (after the second `COPY . .`) reruns on .rs changes — ~30-60 s vs 5 min.
# Uses the pre-built lukemathwalker image so cargo-chef is already installed.
FROM lukemathwalker/cargo-chef:latest-rust-1-slim-bookworm AS chef
WORKDIR /build

# --- recipe: captures dep graph; invalidated only on Cargo.toml/Cargo.lock changes ---
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- cook: compiles all deps as a cached layer; skipped entirely on warm builds ---
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
# Project code only — this COPY invalidates the layer above only when source changes.
# This image builds the DEFAULT binary (no `ocr-tesseract` / `embeddings-fastembed`
# features) — PDF/Office extraction + blob store + pgvector are all pure-Rust.
COPY . .
RUN cargo build --release --locked -p gt-composition --bin gt-mcp-server

FROM debian:bookworm-slim
# git: S3 readiness clause (docs/10 §S4). tmux: PTY terminal WS (hq-terminal).
# node + @anthropic-ai/claude-code: interactive role terminal sessions spawn claude
# inside the container — must be baked in, not mounted from the host (hq-doctor).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gnupg tmux \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g @anthropic-ai/claude-code \
    && npm cache clean --force \
    && apt-get purge -y gnupg \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
# graphify backs the gt:// graph tool (gt-graphindex): the Rust indexer drives the
# `graphifyy` python package through an interpreter, falling back to GT_GRAPHIFY_PYTHON
# when a rig carries no `.graphify-venv`. The runtime base has no python, so without this
# the graph driver dies on `import networkx` (networkx ships as a graphifyy dependency).
# A dedicated venv keeps these deps off the system interpreter; GT_GRAPHIFY_PYTHON pins it
# as the repo-agnostic fallback so every freshly-cloned rig resolves a working interpreter.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv \
    && python3 -m venv /opt/graphify-venv \
    && /opt/graphify-venv/bin/pip install --no-cache-dir graphifyy \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
ENV GT_GRAPHIFY_PYTHON=/opt/graphify-venv/bin/python
COPY --from=builder /build/target/release/gt-mcp-server /usr/local/bin/gt-mcp-server
# Bake the `gt` CLI binary so role-session MCP proxies (`gt mcp`) work out of the box.
# The entrypoint refreshes this over the network on every start so the container tracks
# the `latest` release without a rebuild (hq-mcp-ready-probe.3).
# MUSL not gnu: bookworm (GLIBC 2.36) < gnu release's requirement; musl is static.
RUN curl -fsSL https://github.com/gt-core-labs/gt/releases/download/latest/gt-x86_64-unknown-linux-musl.tar.gz \
    | tar -xz -C /usr/local/bin gt
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh
# Env (GT_DOLT_URL, GT_MCP_HTTP_BIND, GT_MCP_ACTOR, GT_MCP_SCOPE_CONFIG, and
# optionally GT_REPO_DIR for S3 surface validation) is supplied by the compose
# service; see docker-compose gt-mcp-server.
#
# gt://issues pager tuning (hq-core-mcp.13), both optional:
#   GT_ISSUES_DEFAULT_LIMIT  page size when ?limit is omitted (default 200)
#   GT_ISSUES_MAX_LIMIT      hard ceiling a ?limit is clamped to (default 10000)
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["gt-mcp-server"]