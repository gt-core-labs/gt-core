#!/bin/sh
# Container entrypoint for the embeddings image (hq-gt-container).
#
# Keeps the in-container `gt` binary current so the per-role MCP proxy (`gt mcp`, launched by the
# interactive terminal's claude — see gt_composition::terminal, hq-role-mcp) never falls behind the
# published release between image rebuilds. On every start it best-effort downloads the latest
# COMPILED `gt` release asset over the baked baseline, then execs the real command.
#
# Best-effort by design: a failed/offline fetch keeps the binary baked at build time, so the server
# still starts. Tunables:
#   GT_RELEASE_URL  source tarball (default: gt-core-labs/gt `latest`, linux-gnu)
#   GT_SKIP_UPDATE=1  skip the runtime fetch entirely (use the baked binary)
set -e

GT_RELEASE_URL="${GT_RELEASE_URL:-https://github.com/gt-core-labs/gt/releases/download/latest/gt-x86_64-unknown-linux-gnu.tar.gz}"

if [ "${GT_SKIP_UPDATE:-0}" != "1" ]; then
    tmp="$(mktemp -d)"
    if curl -fsSL --max-time 60 "$GT_RELEASE_URL" -o "$tmp/gt.tar.gz" \
        && tar -xzf "$tmp/gt.tar.gz" -C "$tmp" gt; then
        install -m 0755 "$tmp/gt" /usr/local/bin/gt
        echo "[entrypoint] gt refreshed from $GT_RELEASE_URL ($(/usr/local/bin/gt --version 2>/dev/null || echo '?'))"
    else
        echo "[entrypoint] gt refresh skipped (fetch failed); using baked binary" >&2
    fi
    rm -rf "$tmp"
fi

exec "$@"
