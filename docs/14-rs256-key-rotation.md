# RS256 signing-key rotation runbook

Operator procedure to roll the RS256 signing key (`kid` rollover) **without
invalidating tokens already in flight**. This is the step-by-step companion to
[13-auth.md](13-auth.md) §"Key management & rotation"; that doc explains *why*
the scheme works, this one is the *how* you follow during an actual rotation.

## Why no flag day

Tokens name their signing key in the JWT header's `kid`. The verifier holds a
**set** of public keys indexed by `kid`; the minter signs with **one** private
key and stamps its `kid`. So the verifier can trust *both* the old and the new
public key at once. The safe ordering follows from one rule:

> **A token must be verifiable for its entire lifetime.** Never retire a public
> key while any live (unexpired) token was signed with it.

Therefore: **publish the new public key on the verifier *before* the minter
starts signing with it, and retire the old public key only *after* the last
token it signed has expired.** Adding a verifier key is always safe; removing
one is the only dangerous step.

## The environment (recap from docs/13)

| var | side | meaning |
|-----|------|---------|
| `GT_JWT_RS256_KEYS` | verify | `;`-separated rotation set of `kid=<pem-path>` public keys |
| `GT_JWT_RS256_PUBLIC_KEY_FILE` | verify | single un-keyed PEM (single-key deploys) |
| `GT_JWT_RS256_PRIVATE_KEY_FILE` | mint | PEM holding the RS256 signing key |
| `GT_JWT_RS256_SIGNING_KID` | mint | `kid` stamped into minted tokens' headers |

Names verified against the code: `ENV_KEYS` / `ENV_KEY_FILE` in
[crates/domain/platform/gt-auth/src/jwt.rs](../crates/domain/platform/gt-auth/src/jwt.rs)
and `ENV_PRIVATE_KEY_FILE` / `ENV_SIGNING_KID` in
[crates/domain/platform/gt-auth/src/mint.rs](../crates/domain/platform/gt-auth/src/mint.rs).

Behaviour that drives the timing:

- `GT_JWT_RS256_KEYS` parses as `;`-separated `kid=path` entries (a blank entry
  is skipped; a missing `=` or empty `kid`/`path` is `AuthError::Malformed`).
- An **unknown** `kid` on a token is `AuthError::UnknownKey` — *distinct* from
  `InvalidSignature`. If you retire a key too early, live tokens fail as
  `UnknownKey`, not as bad signatures: that is your tell that the window was cut
  short.
- A `kid`-less token resolves against `GT_JWT_RS256_PUBLIC_KEY_FILE`, or against
  the sole keyed key when exactly one is configured. Once two or more keyed keys
  exist, a `kid`-less token no longer resolves — so during rotation **every
  minted token must carry a `kid`** (`GT_JWT_RS256_SIGNING_KID` set).

## Preconditions

- You know **`T_access`**, the maximum access-token TTL (the longest a minted
  token stays valid). The retirement step waits this long. There is no
  server-side revocation of an access token (docs/13 §Tokens) — it expires on
  its own, so the clock is `T_access`, not "until we say so".
- The verifier and minter tiers can be reconfigured (env change + restart, or
  hot reload if the deployment supports it) **independently**.
- Old key id `kid_old`, new key id `kid_new`. Use distinct, dated ids, e.g.
  `2026-06` → `2026-09`, so logs read cleanly. Never reuse a retired `kid`.

## Rotation steps

### 0. Generate the new key pair

```sh
# Private signing key (minter) — keep OFF the verifier tier.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out rs256-2026-09.key
# Public key (verifier) — distribute freely.
openssl rsa -pubout -in rs256-2026-09.key -out rs256-2026-09.pub
```

Treat `*.key` as a secret (the minter alone holds it); `*.pub` is publishable.

### 1. Publish the new PUBLIC key on the verifier (add `kid_new`)

Add the new public key to the verifier's set **alongside** the old one, then
restart/reload the verifier tier:

```sh
GT_JWT_RS256_KEYS="2026-06=/keys/rs256-2026-06.pub;2026-09=/keys/rs256-2026-09.pub"
```

The verifier now trusts both `kid`s. The minter is unchanged — still signing
with `kid_old` — so nothing has changed for issued tokens yet. **Verify before
moving on** (see §Verify): a token signed with `kid_old` still validates, and a
test token signed with `kid_new` validates too.

> Roll this out to **every** verifier instance before step 2. A token signed
> with `kid_new` reaching a verifier that has not yet loaded it fails
> `UnknownKey`.

### 2. Switch the minter to sign with `kid_new`

Point the minter at the new private key and stamp the new `kid`, then
restart/reload the minter tier:

```sh
GT_JWT_RS256_PRIVATE_KEY_FILE=/keys/rs256-2026-09.key
GT_JWT_RS256_SIGNING_KID=2026-09
```

New tokens now carry `kid_new`; verifiers select the new public key. Tokens
minted *before* this moment still carry `kid_old` and keep validating against
the old public key still in the set. This is the **overlap window**.

### 3. Hold the overlap window

Wait **at least `T_access`** (one full access-token lifetime) after step 2.
After that, no live token bears `kid_old` — every `kid_old` token has expired.
Add a safety margin for clock skew and any cached/long-lived sessions; waiting
*longer* is always safe (the old public key just sits unused).

### 4. Retire the old PUBLIC key (drop `kid_old`)

Only now remove `kid_old` from the verifier set and restart/reload:

```sh
GT_JWT_RS256_KEYS="2026-09=/keys/rs256-2026-09.pub"
```

A token still bearing `kid_old` past this point is now `UnknownKey` — but by
§3's wait there should be none. Securely destroy the old **private** key
(`rs256-2026-06.key`); the old **public** key can be archived.

End state: a single keyed key. (If you prefer a `kid`-less single-key deploy,
you may instead move the lone public key to `GT_JWT_RS256_PUBLIC_KEY_FILE` and
unset `GT_JWT_RS256_SIGNING_KID` on the minter — but only once you are back to
exactly one key.)

## Verify

After **step 1** (both keys trusted) and after **step 4** (old key gone):

- Mint a token from the minter tier and decode its header — confirm `kid`
  matches the expected key id (`jsonwebtoken`'s `decode_header`, as the
  `stamps_the_signing_kid_and_round_trips_through_a_keyset` test in
  [mint.rs](../crates/domain/platform/gt-auth/src/mint.rs) does).
- Hit an authenticated route (`Authorization: Bearer <token>`) and confirm
  `200`, not `401`.
- Watch verifier logs for `UnknownKey` during the whole rotation: **zero** is
  the success signal. Any `UnknownKey` means a verifier is missing a key it was
  handed a token for — a step was skipped or a window cut short.

## Rollback

Each step is independently reversible; the danger is always "retired too early".

- **Regret step 2** (new signing key bad): revert the minter env to
  `kid_old` + old private key and restart. Safe immediately — `kid_old`'s public
  key is still in the verifier set (you have not done step 4). Tokens minted with
  `kid_new` in the interim still verify as long as `kid_new` remains in the set,
  so leave it there until they expire.
- **Regret step 4** (retired too early — `UnknownKey` storms): re-add the dropped
  `kid_old=<path>` entry to `GT_JWT_RS256_KEYS` and restart the verifier. Tokens
  validate again the instant the key is back. This is why you keep the old
  **public** key archived even after destroying the private one.
- **Never** roll back step 1 while the minter signs with `kid_new` — removing a
  key the minter is actively using breaks every fresh token.

## See also

- [13-auth.md](13-auth.md) — the auth subsystem reference; §"Key management &
  rotation (RS256, by `kid`)" is the design this runbook operationalizes.
- [crates/domain/platform/gt-auth/src/jwt.rs](../crates/domain/platform/gt-auth/src/jwt.rs)
  — verifier: `from_env`, the `kid` keyset, `UnknownKey`.
- [crates/domain/platform/gt-auth/src/mint.rs](../crates/domain/platform/gt-auth/src/mint.rs)
  — minter: `from_env`, `with_kid`, the signing-`kid` header.
