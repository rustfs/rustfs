# Two-Factor Authentication

> Scope: the self-service account surface (`/rustfs/admin/v3/account/*`), the
> login gate on `AssumeRole`, and the administrative reset
> (`/rustfs/admin/v3/user/mfa`).

This document records what the second factor does and does not protect, and why.
The boundaries are deliberate; several of them look like gaps until the
alternative is spelled out.

## What is protected

TOTP gates **session minting**: the `AssumeRole` call that turns a long-term
credential into a short-lived STS session. That is the only interactive login
RustFS has — the Console holds nothing but an STS session, and obtains it by
signing an `AssumeRole` request with the access key the user typed.

When an identity has an active enrollment:

```
access key + secret key
        │
        ▼
GET /v3/mfa/challenge          (SigV4-signed; answers "is a factor needed?")
        │  required: true
        ▼
POST /  Action=AssumeRole
        SerialNumber=<challenge>
        TokenCode=<6-digit TOTP | recovery code>
        │
        ▼
STS credentials, with the claim  x-rustfs-mfa-verified: true
```

Without a `TokenCode`, `AssumeRole` fails with `AccessDenied` and a message
carrying the `MultiFactorAuthRequired` marker. Clients match on that marker to
prompt for a code rather than reporting a failed login — the password *was*
accepted.

`SerialNumber` and `TokenCode` are `AssumeRole`'s own parameters, so an SDK or a
script authenticates the same way the Console does, with no RustFS-specific
protocol.

## What is deliberately not protected

**A request signed directly with a long-term access key is not gated.** This is
the most important boundary in the design, and it is intentional:

- Gating it would break every script, SDK client and `rc` invocation the moment
  a human enabled 2FA on their own account. An operator who turned on a security
  feature would discover it by way of a production outage.
- It would add no protection. Whoever holds the secret key already has full
  access to everything that identity can reach; they never need to present a
  code, because they never need to mint a session.

This is the same division AWS draws: MFA gates `AssumeRole` and is enforced for
API calls through the `aws:MultiFactorAuthPresent` policy condition, not by
refusing signed requests.

**Consequence to state plainly:** 2FA raises the cost of a stolen *password*. It
does not contain a stolen *secret key*. Because in RustFS the password **is** the
S3 secret key (see below), those are the same string — so 2FA protects the
console login path against credential reuse and phishing, and nothing more,
until the policy-condition work lands.

The tracked follow-up is an `aws:MultiFactorAuthPresent` condition key populated
from the `x-rustfs-mfa-verified` session claim, which would let an operator write
a policy that denies administrative actions to a session that presented no
second factor. That is the mechanism that makes 2FA meaningful for API access.

**OIDC and Keystone sessions are not gated either.** Those identities are
authenticated by their provider; a RustFS-side TOTP enrollment would not be
consulted at login and would give a false impression of protection. MFA for a
federated identity belongs to its IdP. `CallerIdentity` reports such sessions as
`FederatedIdentity` and refuses enrollment.

**Service-account credentials cannot manage their parent's factor.** A machine
credential must not be able to take over the human identity it was minted from.

## Password reality: there is no password hash

RustFS is an S3 server. SigV4 requires the server to know the secret key itself
in order to recompute a request signature, so secret keys **cannot** be hashed —
not here, and not in any S3-compatible implementation. The "password" a user
types into the Console is their S3 secret key.

What protects it instead:

| Protection | Mechanism |
| --- | --- |
| At rest | `RUSTFS_IAM_MASTER_KEY` + `encrypt_stream_io` (Argon2id → AES-GCM / ChaCha20-Poly1305) |
| Length floor | `is_secret_key_valid` (`SECRET_KEY_MIN_LEN`) |
| Rotation | `POST /v3/account/password`, requiring the current secret |
| Session cleanup | Every STS session minted from the identity is revoked on rotation |

There is deliberately **no maximum length**. The previous Console capped
passwords at 40 characters, which was a client-side invention with no server
constraint behind it; capping password length is an anti-pattern.

## At-rest protection is mandatory for TOTP secrets

A TOTP secret is credential-equivalent: anyone holding it can mint valid codes
forever. So enrollment is **refused** when `RUSTFS_IAM_MASTER_KEY` is not
configured, rather than writing the secret in plaintext:

```
POST /v3/account/mfa/enroll  →  501 NotImplemented
"two-factor authentication requires RUSTFS_IAM_MASTER_KEY to be configured
 so the shared secret can be encrypted at rest"
```

IAM *identities* tolerate a missing master key for backward compatibility with
existing deployments. A new feature has no such history to honour, and a second
factor that can be lifted off a disk is worse than none, because the user
believes they have one.

`GET /v3/account/mfa` reports `enrollment_available: false` with the reason, so
the Console and `rc` explain the remedy instead of offering a control that fails.

## Root credentials cannot be changed at runtime

The root identity comes from `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` and lands
in a process-wide `OnceLock` (`crates/credentials/src/credentials.rs`). It cannot
be rotated while the server runs, and the account surface reports this as
`credentials_source: "env"` with `mutable.password: false`.

This is not merely a missing feature. The root secret key feeds three things:

1. **STS session token signing** (`root_credentials::token_signing_key`) — every
   live session in the cluster is HMAC-signed with it.
2. **The internode RPC secret** (`derive_rpc_secret`), unless
   `RUSTFS_RPC_SECRET` is set explicitly.
3. **Legacy IAM at-rest decryption** for blobs migrated from MinIO.

Rotating it at runtime would therefore invalidate every session cluster-wide and
break node-to-node authentication. Making root mutable is a separate piece of
work with those three couplings as prerequisites; it is not a side effect of
adding a profile page.

**Operational recommendation:** treat root as a bootstrap identity. Create a
built-in IAM user with the `consoleAdmin` policy for day-to-day administration.
That identity has a working password change and full 2FA support.

## Rate limiting, replay and expiry

| Control | Value | Where |
| --- | --- | --- |
| Failed attempts before lockout | 5 | `mfa/record.rs` |
| First lockout | 15 minutes, doubling per further run | `mfa/record.rs` |
| Lockout ceiling | 1 hour | so a sustained attack cannot deny the owner indefinitely |
| TOTP clock skew | ±1 step (±30s) | three codes valid at once, no more |
| TOTP replay | Consumed time step is a high-water mark; `step <= last_used` is refused | closes the ~90s window a captured code would otherwise have |
| Recovery code replay | `used_at` stamp, single use | |
| Login challenge TTL | 5 minutes | |
| Pending enrollment TTL | 10 minutes | an abandoned enrollment leaves no usable secret |

A wrong code, a replayed code and a malformed code are **indistinguishable** on
the wire: all three return `AccessDenied` with the same message. The distinction
survives only in the audit trail, so an operator can tell a guessing attempt from
a replay without an attacker learning that a captured code was genuine.

The lockout is stored in the record and updated under an optimistic
compare-and-set, so it holds across the cluster rather than per node.

## Storage

```
.rustfs.sys/config/mfa/<access-key>/totp.json     (encrypted with the IAM master key)
```

A sibling of `config/iam/`, not a child: the IAM cache loader walks the whole
`config/iam/` tree on startup and buckets what it finds by first path segment, so
a new prefix under there would be swept into that walk for no benefit.

Records are **not cached**. Every verification reads from the store, because a
cache would need cluster-wide invalidation to keep the replay mark and the
lockout counter honest, and getting that wrong reopens exactly the holes this
design closes. Verifications are rare enough that the read is not worth
optimising.

Writes are read-modify-write under an `If-Match` precondition with bounded
retries — the same optimistic scheme the IAM lazy-rewrite path uses. It degrades
to a retry rather than to a distributed lock a crashed node would have to time
out.

## Login challenges are stateless

A challenge is `HMAC-SHA256(root_secret, "rustfs-mfa-challenge:v1" ‖ access_key ‖
issued_at)`, base64url-encoded with its payload.

The obvious alternative is a TTL cache, the way the OIDC flow stores its PKCE
verifiers. That store is node-local, which is fine for OIDC because the whole
authorization round trip returns to the node that started it. A second factor
does not: a cluster behind a load balancer without session affinity would issue
the challenge on one node and receive the code on another, and a node-local
challenge would fail there for reasons no operator could debug.

Statelessness costs nothing, because the challenge is not what makes the exchange
single-use — the consumed TOTP time step is.

## Authorization model

| Route | Gate |
| --- | --- |
| `GET /v3/account/info` | possession of the credential |
| `POST /v3/account/password` | credential **+ knowledge of the current secret** |
| `GET /v3/account/mfa` | possession of the credential |
| `POST /v3/account/mfa/enroll` | credential, and the credential kind must be mutable |
| `POST /v3/account/mfa/activate` | credential + a valid code from the pending secret |
| `POST /v3/account/mfa/disable` | credential + **a valid code and the account password** |
| `POST /v3/account/mfa/recovery-codes` | credential + a valid code |
| `GET /v3/mfa/challenge` | possession of the credential |
| `GET /v3/user/mfa` | `admin:GetUser` |
| `DELETE /v3/user/mfa` | `admin:EnableUser` |
| `PUT /v3/set-user-secret-key` | `admin:CreateUser` |

The self-service routes carry **no admin action**. Giving them one would be wrong
in both directions: it would stop an ordinary user from changing their own
password, and it would let any holder of that action change somebody else's.
They are registered as `CredentialOnly` in the route-policy matrix.

`POST /v3/account/password` and `POST /v3/account/mfa/disable` require a
proof-of-knowledge step because a signature only proves a credential was *used*.
The Console signs with a short-lived session, so without it a hijacked browser
tab could rewrite the account's credentials or strip its second factor.

### Why turning the factor off needs the password too

Requiring only a code would mean a single shoulder-surfed number, in a session
someone walked away from, is enough to remove the protection. Requiring the
password makes disabling the factor as hard as the thing the factor protects.

### Break-glass

`DELETE /v3/user/mfa` clears another identity's factor, for a user who lost both
their authenticator and their recovery codes. It is gated on `admin:EnableUser`
rather than a bespoke action, because that is the same capability that can
already re-enable a disabled account — anyone who can do that can already take
the identity over, so a separate action would be a distinction without a security
difference.

The record is deleted outright rather than disabled, so no stale lockout counter
survives to block the user's next enrollment. The acting administrator is
recorded in the audit entry.

## Recovery codes

Ten codes, `XXXX-XXXX-XXXX-XXXX-XXXX`, 100 bits of uniform randomness each, in a
Crockford base32 alphabet with `I`, `L`, `O` and `U` removed so a handwritten
code cannot be ambiguous.

Stored as domain-separated SHA-256 digests, **not** a password KDF. With 100 bits
of uniform randomness there is no dictionary to try and no human-chosen pattern
to exploit, so the attacks a slow KDF defends against do not apply — while a
memory-hard KDF would have to run once per stored code on every verification
attempt, turning each guess into an attacker-controlled multiple of that cost.
This is the standard treatment for high-entropy bearer tokens, and the same
reasoning is why there is no per-code salt.

Codes are returned in plaintext exactly once. Activation always replaces the set:
reusing a previous one would leave codes valid for a secret they were never
issued against. Disabling clears them, so no live bypass survives a factor the
user believes is gone.

## Audit

Two `EventName` variants carry the whole surface:

- `iam:Identity:CredentialChanged` — password rotation, enrollment, activation,
  disable, recovery-code regeneration, administrative reset.
- `iam:Identity:AuthChallenge` — challenge issuance and second-factor
  verification.

The per-operation detail lives in `api.name` and the `iamOperation` tag, which is
what a SIEM filters on. The enum is coarse because `EventName::mask()` gives every
variant its own bit in a `u64` and the budget is nearly spent — 63 of 64 used
after these two. Splitting these per-operation needs `mask()` widened first.

**Redaction:** no secret key, TOTP secret, provisioning URI, submitted code or
recovery code enters an audit entry — not even hashed, and not on the failure
paths where the submitted value would be the most tempting thing to record.
Failures are described by a closed set of static strings
(`AccountAuditFailure`), so no caller-supplied bytes can reach a log target
through this module.

## Known limitations

1. **2FA does not gate direct SigV4 access.** By design; see above. The fix is
   the `aws:MultiFactorAuthPresent` policy condition.
2. **Root cannot rotate its own credentials at runtime.** By design; see above.
3. **GHSA-m77q-r63m-pj89 is unaffected.** STS session tokens are signed with the
   root secret key, so anyone holding it can still forge a session token —
   including one carrying `x-rustfs-mfa-verified`. 2FA does not close this; a
   dedicated STS signing key does, and that advisory is tracked separately.
4. **Username changes are not supported for anyone.** The access key is the
   primary key for policy mappings, group membership, service-account parents and
   bucket-policy principals. A rename is a migration that orphans service
   accounts and silently breaks bucket-policy ARNs, not an edit;
   `mutable.username` is `false` for every identity.
