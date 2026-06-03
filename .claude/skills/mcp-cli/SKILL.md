---
name: mcp-cli
description: Install and use gt-mcp-cli — the CLI client for the gt-mcp server (work tracking, resources, deploy stack). Downloads the prebuilt release binary, verifies it, puts it on PATH, then drives the gt-mcp surface.
trigger: /mcp-cli
---

# /mcp-cli

`gt-mcp-cli` is the command-line client for **gt-mcp**, the MCP surface over the
Gas Town orchestrator. Use it for all work tracking (`hq.issues` beads), reading
domain snapshot resources, and managing the deploy stack — over a real MCP
handshake (streamable-HTTP, `rmcp` SDK).

This skill installs the **prebuilt release binary** (no Rust toolchain needed)
from [`gt-core-labs/gt-mcp-cli`](https://github.com/gt-core-labs/gt-mcp-cli) and
then drives it.

## Install

Run the bundled installer. Idempotent — re-run to upgrade to the newest release.

```sh
bash .claude/skills/mcp-cli/install.sh
```

What it does:
1. Detects platform (`x86_64-unknown-linux-musl`, `-gnu`, or `aarch64-apple-darwin`).
   Linux defaults to **musl** (static → works on NixOS where glibc-linked binaries
   miss the `/lib64` interpreter). Force glibc with `GT_MCP_CLI_LIBC=gnu`.
2. Resolves the latest release tag (`gh` if present, else GitHub API).
3. Downloads the tarball + `.sha256`, **verifies the checksum**, aborts on mismatch.
4. Installs `gt-mcp-cli` to `~/.local/bin` (override `GT_MCP_CLI_BINDIR`).
5. Prints `--version` to confirm.

Env overrides: `GT_MCP_CLI_REPO`, `GT_MCP_CLI_TAG`, `GT_MCP_CLI_BINDIR`, `GT_MCP_CLI_LIBC`.

If `~/.local/bin` is not on PATH, the installer prints the `export` line to add.

## Use

```sh
gt-mcp-cli tools                       # list tools (name + description)
gt-mcp-cli tools --full                # full input schemas (JSON)
gt-mcp-cli resources                   # list domain snapshot resource URIs
gt-mcp-cli call <name> [--arg k=v ...] [--json '{...}']
gt-mcp-cli read <uri>                  # e.g. gt-mcp-cli read gt://agent/sessions
gt-mcp-cli prime                       # report active workspace/role/rig (offline)
gt-mcp-cli workspace list|create|info|use
gt-mcp-cli compose up|down             # deploy stack (offline; drives git + docker)
```

- **Endpoint:** `--url` flag, else `GT_MCP_URL`, else `config.toml`, else the
  builtin default `http://127.0.0.1:8765/mcp`.
- **`--arg k=v`** values parse as JSON (`priority=0` → number, `weekly=true` → bool),
  falling back to a string. **`--json '{...}'`** supplies the whole argument
  object and wins over `--arg`.
- Exit is **nonzero** when the tool reports `isError` or the call fails.
- **Never put `workspace_id` in args/URL/body** — the server injects it from the
  auth context; spoofing is rejected (gt-core NN invariant).

### Work-tracking drill (gt-core `hq.issues` beads)

```sh
gt-mcp-cli call meta.help
gt-mcp-cli read gt://issues?external_ref=hq-mod-core      # review sub-epic before starting
gt-mcp-cli call issues.transition.execute --arg id=hq-mod-core.3 --arg to=in_progress
```

Always use the `<tool>.<verb>.execute` form for mutations.

## Requirements

`curl`, `tar`, and `sha256sum`/`shasum`. `gh` optional (used for tag resolution
when available). A running **HTTP** gt-mcp endpoint is needed for online commands
(`tools`/`resources`/`call`/`read`); `prime`/`compose` run offline.