#!/usr/bin/env bash
# Download + install the latest gt-mcp-cli release binary from gt-core-labs/gt-mcp-cli.
# Idempotent: re-running upgrades to the newest published release.
#
# Env overrides:
#   GT_MCP_CLI_REPO    GitHub repo (default gt-core-labs/gt-mcp-cli)
#   GT_MCP_CLI_TAG     release tag (default: latest published release)
#   GT_MCP_CLI_BINDIR  install dir (default: $HOME/.local/bin)
#   GT_MCP_CLI_LIBC    linux libc flavour: musl | gnu (default: musl — static, NixOS-safe)
set -euo pipefail

REPO="${GT_MCP_CLI_REPO:-gt-core-labs/gt-mcp-cli}"
BINDIR="${GT_MCP_CLI_BINDIR:-$HOME/.local/bin}"
LIBC="${GT_MCP_CLI_LIBC:-musl}"

# --- resolve target triple -------------------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
  Linux/x86_64)   target="x86_64-unknown-linux-${LIBC}" ;;
  Darwin/arm64)   target="aarch64-apple-darwin" ;;
  Darwin/aarch64) target="aarch64-apple-darwin" ;;
  *) echo "unsupported platform: $os/$arch" >&2; exit 1 ;;
esac
asset="gt-mcp-cli-${target}.tar.gz"

# --- resolve tag -----------------------------------------------------------
tag="${GT_MCP_CLI_TAG:-}"
if [ -z "$tag" ]; then
  if command -v gh >/dev/null 2>&1; then
    tag="$(gh release view --repo "$REPO" --json tagName -q .tagName)"
  else
    tag="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep -m1 '"tag_name"' | cut -d'"' -f4)"
  fi
fi
[ -n "$tag" ] || { echo "could not resolve release tag" >&2; exit 1; }

base="https://github.com/${REPO}/releases/download/${tag}"
echo "==> gt-mcp-cli ${tag} (${target})"

# --- download + verify -----------------------------------------------------
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
curl -fsSL "${base}/${asset}"        -o "$tmp/$asset"
curl -fsSL "${base}/${asset}.sha256" -o "$tmp/$asset.sha256"

( cd "$tmp"
  # asset.sha256 holds: "<hash>  <asset-name>"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "$asset.sha256"
  else
    shasum -a 256 -c "$asset.sha256"
  fi )

# --- extract + install -----------------------------------------------------
tar -xzf "$tmp/$asset" -C "$tmp"
bin="$(find "$tmp" -type f -name gt-mcp-cli | head -1)"
[ -n "$bin" ] || { echo "binary not found in archive" >&2; exit 1; }

mkdir -p "$BINDIR"
install -m 0755 "$bin" "$BINDIR/gt-mcp-cli"
echo "==> installed: $BINDIR/gt-mcp-cli"

case ":$PATH:" in
  *":$BINDIR:"*) ;;
  *) echo "NOTE: $BINDIR not on PATH — add: export PATH=\"$BINDIR:\$PATH\"" >&2 ;;
esac

"$BINDIR/gt-mcp-cli" --version