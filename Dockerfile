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
# `git` backs the S3 readiness clause (docs/10 §S4): GitSurfaceTree shells out to
# `git ls-tree -r main` over the GT_REPO_DIR checkout so `?ready=true` hides a bead
# whose own non-`planned` surface is absent from gt-core's `main`. Without git the
# check degrades to accept-all and the frontier surfaces beads whose surfaces live
# only in a source repo (the upstream app).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git tmux \
    && rm -rf /var/lib/apt/lists/*
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