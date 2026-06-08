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

# Claude binary: mounted from the host at /home/nixos/.local/bin/claude but the container runs as
# root ($HOME=/root), so claude's own /doctor and self-checks look in /root/.local/bin and report
# "missing". Symlink it so $HOME-relative lookups find the same binary.
if [ -f /home/nixos/.local/bin/claude ]; then
    mkdir -p /root/.local/bin /root/.local/share
    ln -sf /home/nixos/.local/bin/claude /root/.local/bin/claude
    # claude stores config/plugins under .local/share/claude — share the same dir.
    if [ -d /home/nixos/.local/share/claude ] && [ ! -e /root/.local/share/claude ]; then
        ln -sf /home/nixos/.local/share/claude /root/.local/share/claude
    fi
    echo "[entrypoint] claude symlinked: /root/.local/bin/claude → /home/nixos/.local/bin/claude"
fi

exec "$@"
