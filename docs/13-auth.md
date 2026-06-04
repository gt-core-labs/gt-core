# Authentication & tenant resolution

How a frontend (or any client) authenticates to gt-core and reaches the HTTP
surface, and how the server turns a bearer token into a verified, tenant-scoped,
scope-checked request. This is the `hq-auth` epic; the pieces below land bead by
bead and this doc is the operator/integrator reference for the whole.

## TL;DR

- **Access tokens are stateless RS256 JWTs.** The server verifies signatures with
  a **public** key (clients never hold the signing secret); only the minting tier
  holds the **private** key.
- **Refresh tokens are opaque, server-side, rotating.** They exchange for a fresh
  access token without re-login; reuse of a rotated token revokes its whole family.
- **The workspace is server-injected, never client-supplied** (docs/04 rule 15):
  it comes from the `X-GT-Workspace` header or the verified `workspace` claim, and
  a header that disagrees with the claim is a `403`.
- **Everything folds into existing crates behind Cargo features** (docs/03 Rule 4):
  `gt-auth` owns verify/mint/password adapters behind `jsonwebtoken`/`password-hash`;
  the HTTP middleware that ties them to the tenant types lives one tier up in
  `gt-composition`.

## The pipeline

```
LOGIN                                                         (gt-auth)
  POST /api/v1/auth/login {email, password, workspace}
        │  IdentityProvider.authenticate (argon2, PG users)   PasswordProvider
        ▼
  VerifiedIdentity {sub, workspace, scopes}
        │  into_claims(exp, iat)  →  JwtMinter.mint (RS256, private key)
        ▼
  { access_token (JWT, short TTL), refresh_token (opaque, long TTL) }

REQUEST                                                       (gt-composition)
  GET /api/v1/<module>/...   Authorization: Bearer <access>
        │  auth middleware: JwtAuthenticator.verify (RS256, public key by kid)
        │     → insert JwtClaims + WorkspaceClaim into request extensions
        ▼
  WorkspaceContext extractor  → tenant from header or claim (mismatch ⇒ 403)
  scope guard                 → JwtClaims.scopes → CallerScopes → required scope
        ▼
  handler → RootCommand → CommandBus   (same bus the MCP tools dispatch to)

REFRESH
  POST /api/v1/auth/refresh {refresh_token}
        │  RefreshStore.rotate: validate → issue successor → retire presented
        │     (replay of a rotated token ⇒ Reused ⇒ revoke family)
        ▼
  { access_token (fresh), refresh_token (rotated) }
```

The HTTP routes and the MCP tools are two driving adapters of the **same** domain:
both land in the same `CommandBus`. Auth gates the HTTP routes; the MCP server has
its own `X-Actor`/RBAC path.

## Tokens

### Access token (RS256 JWT, stateless)

Claims ([`gt_auth::JwtClaims`]):

| claim | meaning |
|-------|---------|
| `sub` | authenticated principal (user / agent id) |
| `workspace` | tenant the token is scoped to — **required** (grace toggle below) |
| `scopes` | authorization scopes (checked by the route guard) |
| `exp` | expiry (Unix secs) |
| `nbf` | not-before (Unix secs, optional) |
| `iat` | issued-at (Unix secs) |

The verifier checks the **signature only**; the `exp`/`nbf`/`iat`/`workspace`
gates are `JwtClaims::validate_with_leeway(now, leeway, workspace_optional)`,
which tolerates clock skew (widens `exp`, narrows `nbf`/`iat`) per RFC 7519.
Keep the access TTL short — there is no server-side revocation of an access token;
it expires on its own.

### Refresh token (opaque, server-side, rotating)

A high-entropy opaque string (256 bits from the OS CSPRNG via `getrandom`), stored
server-side, belonging to a **family**. `RefreshStore.rotate` validates the
presented token is active and unexpired, retires it, and issues a successor in the
same family. Presenting an **already-rotated** token is reuse — a theft signal —
and revokes the entire family. `revoke_family` is logout. The in-memory adapter
ships today; a Postgres-backed store is the follow-up.

## Key management & rotation (RS256, by `kid`)

Asymmetric on purpose: the verifier holds public keys, the minter the private one.
Tokens name their signing key in the JWT header's `kid`, so keys rotate without a
flag day: publish the new `kid`, sign with it, retire the old one after a window.

Environment (loaded by `JwtAuthenticator::from_env` / `JwtMinter::from_env`):

| var | side | meaning |
|-----|------|---------|
| `GT_JWT_RS256_KEYS` | verify | `;`-separated rotation set of `kid=<pem-path>` public keys |
| `GT_JWT_RS256_PUBLIC_KEY_FILE` | verify | single PEM, the un-keyed default (single-key deploys) |
| `GT_JWT_RS256_PRIVATE_KEY_FILE` | mint | PEM holding the RS256 signing key |
| `GT_JWT_RS256_SIGNING_KID` | mint | optional `kid` stamped into minted tokens’ headers |
| `GT_JWT_WS_OPTIONAL` | verify | grace flag — accept a token with no `workspace` claim during rollout |

A `kid`-less token resolves against the un-keyed default (or a sole keyed key).
An unknown `kid` is `AuthError::UnknownKey` — distinct from `InvalidSignature`:
the token may be valid but is unverifiable here.

### `GT_JWT_WS_OPTIONAL` rollout

`hq-mt-auth.1` makes the `workspace` claim required. During the window where some
issuers don’t yet stamp it, set `GT_JWT_WS_OPTIONAL=1` (truthy: `1`/`true`/`yes`/`on`)
so a claim-less token still validates. Drop it once every issuer stamps the claim;
then a missing `workspace` is `AuthError::MissingWorkspace`.

## Tenant resolution (the workspace boundary)

`gt_workspace::WorkspaceContext` is the single sanctioned injection point. It
resolves the tenant from, in order:

1. the `X-GT-Workspace` header (an explicit selector), else
2. a `WorkspaceClaim` the auth middleware left in the request extensions (from the
   verified token’s `workspace` claim).

When **both** are present they must name the same workspace — a header that asserts
a different tenant than the token authorizes is a spoof attempt, rejected `403`
(`WorkspaceContextRejection::Mismatch`). A missing/malformed selector is `400`.
The workspace is **never** read from a URL path or request body.

Why the claim arrives as an extension and not a token: `gt-workspace` is a
`platform` crate and may not depend on `gt-auth` (platform→platform is forbidden,
docs/03 Rule 4). The auth middleware sits in `gt-composition` (the `modules` tier,
which may depend on both) and is the only sanctioned producer of `WorkspaceClaim`.

## Authorization (route ↔ scope)

Routes mount under `/api/v1/<module>` (`gt_module::routes::API_BASE`). A module
that declares scopes in its `Capability` opts every route into RBAC. The required
scope is derived per request from the HTTP method:

| method | required scope |
|--------|----------------|
| `GET` / `HEAD` / `OPTIONS` | `<module>.read` |
| `POST` / `PUT` / `PATCH` / `DELETE` … | `<module>.write` |

The auth middleware injects `JwtClaims`; the scope bridge turns `claims.scopes`
into the kernel’s `CallerScopes` extension; `guard_module_scopes` checks the
route’s required scope against it. No `CallerScopes` at all ⇒ unauthenticated
(`401`); present but missing the scope ⇒ forbidden (`403`).

## Streaming (SSE) auth

The per-workspace event feed (docs/02) authenticates by **cookie**, not the bearer
header — `EventSource` cannot set custom HTTP headers. The browser sends the access
JWT in the `gt_web_token` cookie (the mirror of the localStorage bearer used for
`/api` calls), with `withCredentials: true`; the backend reads the JWT from that
cookie and keys the feed by its `workspace` claim (server-injected, never a URL/body
field), enforcing scope + workspace before the stream opens. The token never goes in
the query string (it leaks into proxy logs), and the client reconnects with
`Last-Event-ID`. See [02-sse-pattern.md](02-sse-pattern.md) §"Auth: cookie, not header".

## Implementation status (hq-auth)

| Piece | Bead | State |
|-------|------|-------|
| RS256 verifier (`JwtAuthenticator`) | `hq-auth-verify.1` | ✅ |
| Keyset by `kid` + env loading | `hq-auth-verify.2` | ✅ |
| `nbf`/`iat`/clock-skew + ws gate | `hq-auth-verify.3` | ✅ |
| Golden-token contract gate | `hq-auth-verify.4` | ✅ |
| RS256 minter (`JwtMinter`, `from_env`) | `hq-auth-mint.1` | ✅ |
| Password + PG user store | `hq-auth-mint.2` | in progress |
| Refresh tokens (rotation, reuse) | `hq-auth-mint.3` | ✅ |
| Login flow integration test | `hq-auth-mint.4` | planned |
| `WorkspaceContext` JWT fallback | `hq-auth-context.1` | ✅ |
| Header/claim reconciliation | `hq-auth-context.2` | ✅ |
| Auth middleware (claim injection) | `hq-auth-context.3` | ✅ |
| Scope guard bridge | `hq-auth-guard.1` | ✅ |
| Capability→scope per route | `hq-auth-guard.2` | planned |
| 401/403 vocab + audit | `hq-auth-guard.3` | planned |
| Auth endpoints (login/refresh/logout/me) | `hq-auth-routes.1` | planned |
| `register_routes` pilot (issues) | `hq-auth-routes.2` | planned |
| JWKS endpoint | `hq-auth-routes.3` | planned |
| Logout / refresh revocation | `hq-auth-session.1` | planned |
| RS256 key rotation runbook | `hq-auth-session.2` | planned |

Exposing all module functions over HTTP (the `register_routes` surface for the
67 MCP tools) is the sibling epic `hq-fe-api`, which depends on this one: auth
gates, routes expose.

## See also

- [02-sse-pattern.md](02-sse-pattern.md) — streaming endpoint conventions.
- [03-architecture-guardrails.md](03-architecture-guardrails.md) — Rule 4 (adapters
  fold into their crate), Rule 6 (workspace boundary).
- [04-non-negotiables.md](04-non-negotiables.md) — rule 15 (workspace is
  server-injected, never from URL/body).
