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
#   GT_RELEASE_URL  source tarball (default: gt-core-labs/gt `latest`, linux-MUSL)
#   GT_SKIP_UPDATE=1  skip the runtime fetch entirely (use the baked binary)
#
# MUSL not gnu: the runtime base is debian:bookworm (GLIBC 2.36) but the gnu release is built on a
# newer GLIBC (ubuntu-latest), so the gnu binary aborts with `GLIBC_2.38 not found`. The musl asset
# is statically linked and runs on any linux.
set -e

GT_RELEASE_URL="${GT_RELEASE_URL:-https://github.com/gt-core-labs/gt/releases/download/latest/gt-x86_64-unknown-linux-musl.tar.gz}"

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

# Claude binary: npm -g (prefix /usr) installs to /usr/bin/claude but claude's own /doctor and
# self-update checks expect the binary at $HOME/.local/bin/claude (/root/.local/bin inside the
# container). Symlink once on every start so those checks pass.
if [ -f /usr/bin/claude ]; then
    mkdir -p /root/.local/bin
    ln -sf /usr/bin/claude /root/.local/bin/claude
    echo "[entrypoint] claude: /root/.local/bin/claude → /usr/bin/claude ($(/usr/bin/claude --version 2>/dev/null || echo '?'))"
fi

exec "$@"
