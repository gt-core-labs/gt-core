# gt-mcp-server — the gt-core MCP server (hq-core-host.3/.5).
#
# Multi-stage: build the single bin from the workspace, then ship it on a slim
# runtime. Only gt-mcp-server + its deps are compiled (`-p`), so the Postgres /
# sqlx stack of unrelated crates never enters the image. Connects to Dolt over
# the MySQL wire (no TLS) — the runtime needs nothing beyond glibc + CA certs.
FROM rust:1-slim-bookworm AS build
WORKDIR /build
COPY . .
RUN cargo build --release -p gt-mcp-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/gt-mcp-server /usr/local/bin/gt-mcp-server
# Env (GT_DOLT_URL, GT_MCP_HTTP_BIND, GT_MCP_ACTOR, GT_MCP_SCOPE_CONFIG) is
# supplied by the compose service; see docker-compose gt-mcp-server.
ENTRYPOINT ["gt-mcp-server"]
