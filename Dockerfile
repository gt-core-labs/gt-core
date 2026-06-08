# gt-mcp-server — the gt-core MCP server (hq-core-host.3/.5).
#
# Multi-stage: build the single bin from the workspace, then ship it on a slim
# runtime. The `gt-mcp-server` binary now lives in gt-composition (the modules
# tier — the only Rule-4-legal home for the per-domain dispatch handlers,
# hq-mcp-dispatch), so the package is `gt-composition` while the binary name is
# unchanged. Connects to Dolt over the MySQL wire (no TLS) and, when GT_PG_URL is
# set, to Postgres for the domain dispatch handlers.
FROM rust:1-slim-bookworm AS build
WORKDIR /build
# This image builds the DEFAULT binary (no `ocr-tesseract` / `embeddings-fastembed`
# features), so it needs none of those system libraries — the OCR/embedding engines are
# decoupled, opt-in builds (docs/11). PDF/Office extraction + the blob store (opendal) +
# pgvector binding are all pure-Rust. A future OCR/embeddings image adds the relevant libs
# (libtesseract-dev/libleptonica-dev/clang, or the onnxruntime stack) alongside the feature.
COPY . .
RUN cargo build --release -p gt-composition --bin gt-mcp-server

FROM debian:bookworm-slim
# `git` backs the S3 readiness clause (docs/10 §S4): GitSurfaceTree shells out to
# `git ls-tree -r main` over the GT_REPO_DIR checkout so `?ready=true` hides a bead
# whose own non-`planned` surface is absent from gt-core's `main`. Without git the
# check degrades to accept-all and the frontier surfaces beads whose surfaces live
# only in a source repo (the upstream app) — the gap hq-gap-ready-set-has-no-unblocked-gt-core-work.
# The operator wires GT_REPO_DIR + a read-only checkout via compose (out of this repo).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git tmux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/gt-mcp-server /usr/local/bin/gt-mcp-server
# Env (GT_DOLT_URL, GT_MCP_HTTP_BIND, GT_MCP_ACTOR, GT_MCP_SCOPE_CONFIG, and
# optionally GT_REPO_DIR for S3 surface validation) is supplied by the compose
# service; see docker-compose gt-mcp-server.
#
# gt://issues pager tuning (hq-core-mcp.13), both optional:
#   GT_ISSUES_DEFAULT_LIMIT  page size when ?limit is omitted (default 200)
#   GT_ISSUES_MAX_LIMIT      hard ceiling a ?limit is clamped to (default 10000)
# The operator retunes the page size here without recompiling.
ENTRYPOINT ["gt-mcp-server"]
