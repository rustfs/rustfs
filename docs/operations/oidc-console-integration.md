# OIDC Console integration

**Use this when:** connecting RustFS Console login to an OpenID Connect provider (Keycloak, Authing, or any standards-compliant IdP), or debugging an OIDC redirect, token, or policy-mapping failure.
**Source of truth:** `crates/config/src/constants/oidc.rs` (provider keys and `RUSTFS_IDENTITY_OPENID_*`), `crates/iam/src/oidc.rs` (discovery, PKCE, token validation, per-provider env suffixes), `rustfs/src/admin/handlers/oidc.rs` (authorize/callback handlers), `crates/config/src/constants/app.rs` (`ENV_RUSTFS_BROWSER_REDIRECT_URL`), `crates/utils/src/egress.rs` (`ENV_OUTBOUND_ALLOW_ORIGINS`), `crates/policy/src/policy/policy.rs` (built-in policies).

The RustFS side is vendor-neutral and is described once; what RustFS requires from any provider is tabulated in [oidc-provider-requirements.md](oidc-provider-requirements.md). The [Keycloak](#keycloak) and [Authing](#authing) sections contain only IdP-side steps and vendor caveats. Examples use provider id `default` and public origin `https://rustfs.example.com`.

## Integration model

RustFS requires a standards-compliant OpenID Connect provider: discovery at `<issuer>/.well-known/openid-configuration`, authorization and token endpoints, a JWKS, and an authorization-code flow that returns an `id_token`. RustFS never calls a vendor's authorization API; access is decided by RustFS IAM policies after claim mapping. Protocol requirements for IdP vendors are collected in [oidc-provider-requirements.md](oidc-provider-requirements.md).

Login flow:

1. The browser opens `https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/<provider_id>`.
2. RustFS creates `state`, `nonce`, and a PKCE S256 challenge and redirects to the IdP.
3. The IdP redirects back to `/rustfs/admin/v3/oidc/callback/<provider_id>?code=...&state=...`.
4. RustFS exchanges the code at the token endpoint, sending `client_id` and `client_secret` in the request body (`client_secret_post`) together with the PKCE verifier.
5. RustFS validates the ID token signature (JWKS), issuer, audience, expiry, and nonce.
6. RustFS maps claim values to policy names and issues one-hour STS credentials to the Console.

In-flight `state` and PKCE verifiers are node-local: the authorize and callback requests must reach the same RustFS node.

## Configuration keys

Every provider key can be set as `RUSTFS_IDENTITY_OPENID_<KEY>` in the process environment or as `identity_openid` `<key>=<value>` through `mc admin config set`. Names are constants in `crates/config/src/constants/oidc.rs`.

| Provider key | Environment variable | Purpose |
| --- | --- | --- |
| `enable` | `RUSTFS_IDENTITY_OPENID_ENABLE` | `on` loads the provider. |
| `config_url` | `RUSTFS_IDENTITY_OPENID_CONFIG_URL` | Issuer URL used for discovery. A trailing `/.well-known/openid-configuration` is stripped; any other `.well-known` path is rejected. |
| `issuer` | `RUSTFS_IDENTITY_OPENID_ISSUER` | Expected `iss` when it differs from `config_url` (internal discovery URL, public token issuer). |
| `client_id`, `client_secret` | `RUSTFS_IDENTITY_OPENID_CLIENT_ID`, `RUSTFS_IDENTITY_OPENID_CLIENT_SECRET` | Confidential client credentials. |
| `scopes` | `RUSTFS_IDENTITY_OPENID_SCOPES` | Comma-separated; `openid` is required. |
| `other_audiences` | `RUSTFS_IDENTITY_OPENID_OTHER_AUDIENCES` | Additional accepted `aud` values. |
| `redirect_uri` | `RUSTFS_IDENTITY_OPENID_REDIRECT_URI` | Callback URL sent to the IdP; must equal the URL registered there. |
| `redirect_uri_dynamic` | `RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC` | `on` derives the callback from request headers. Keep `off` behind proxies. |
| `claim_name`, `claim_prefix` | `RUSTFS_IDENTITY_OPENID_CLAIM_NAME`, `RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX` | Policy claim name and a fixed string prepended to each value. `claim_prefix` is not a mapping table. |
| `groups_claim`, `roles_claim` | `RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM`, `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM` | Flat top-level array claims whose values are RustFS policy names. |
| `email_claim`, `username_claim` | `RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM`, `RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM` | Identity claims shown in the Console. |
| `role_policy` | `RUSTFS_IDENTITY_OPENID_ROLE_POLICY` | One fixed policy for every login from this provider. Connectivity testing only. |
| `display_name` | `RUSTFS_IDENTITY_OPENID_DISPLAY_NAME` | Login button label. |
| `hide_from_ui` | `RUSTFS_IDENTITY_OPENID_HIDE_FROM_UI` | Hides the provider from `/oidc/providers`. |

Process-level settings (environment only, never suffixed per provider):

| Variable | Purpose |
| --- | --- |
| `RUSTFS_BROWSER_REDIRECT_URL` | Public browser origin used for callback generation, Console success redirects, and logout fallback. |
| `RUSTFS_OUTBOUND_ALLOW_ORIGINS` | Exact `scheme://host[:port]` origins RustFS may contact for discovery, JWKS, and token requests when the IdP resolves to a private, loopback, or container-network address. See [outbound-connection-policy.md](outbound-connection-policy.md). |

Named providers: to use provider id `<id>`, suffix every provider env var with `_<id>` (for example `RUSTFS_IDENTITY_OPENID_CLIENT_ID_keycloak`) and register the callback `/rustfs/admin/v3/oidc/callback/<id>`. Suffix scanning is `parse_single_provider` in `crates/iam/src/oidc.rs`.

Restart RustFS after changing any of these settings.

### Environment example

```bash
export RUSTFS_BROWSER_REDIRECT_URL="https://rustfs.example.com"

export RUSTFS_IDENTITY_OPENID_ENABLE=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="<ISSUER>"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID="<CLIENT_ID>"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET="<CLIENT_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES="openid,profile,email"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME="<IdP name>"
export RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM="groups"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"
export RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM="email"
export RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM="preferred_username"
```

The same keys through admin config:

```bash
mc admin config set rustfs identity_openid \
  enable=on config_url="<ISSUER>" client_id="<CLIENT_ID>" client_secret="<CLIENT_SECRET>" \
  scopes="openid,profile,email" \
  redirect_uri="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default" \
  redirect_uri_dynamic=off display_name="<IdP name>" \
  groups_claim="groups" roles_claim="roles" email_claim="email" username_claim="preferred_username"
mc admin service restart rustfs
```

`RUSTFS_BROWSER_REDIRECT_URL` is not an `identity_openid` key; it must still be set in the process environment.

## Redirect URL priority

1. Provider `redirect_uri`, when set, is the callback URL sent to the IdP.
2. `RUSTFS_BROWSER_REDIRECT_URL`, when set, is the public origin for callback generation when no provider `redirect_uri` exists, and for Console success and logout fallback redirects.
3. Request headers (`Host`, `X-Forwarded-Proto`) are used only when `redirect_uri_dynamic=on` and no browser redirect URL is configured.

Behind a reverse proxy or load balancer, set `RUSTFS_BROWSER_REDIRECT_URL` and keep session affinity for the authorize and callback requests.

## Policy mapping

Claim values are used verbatim as policy names (after `claim_prefix`, if any). Names must satisfy `is_safe_claim_policy_name` in `crates/iam/src/sys.rs`: ASCII letters, digits, `_`, `-`, `:`, `.` only, so a value containing `/` (for example Keycloak's full group path `/consoleAdmin`) never matches. Built-in policies:

| Policy | Grants |
| --- | --- |
| `consoleAdmin` | Full Console, admin, KMS, and S3 access. |
| `readwrite` | S3 read/write. |
| `readonly` | S3 read-only. |
| `writeonly` | S3 write-only. |
| `diagnostics` | Diagnostic admin access. |

For first-contact testing only, `RUSTFS_IDENTITY_OPENID_ROLE_POLICY=consoleAdmin` grants every login full access; remove it before production.

## Validation

1. Discovery:

```bash
curl -fsS "<ISSUER>/.well-known/openid-configuration" | jq '{issuer, authorization_endpoint, token_endpoint, jwks_uri, code_challenge_methods_supported, token_endpoint_auth_methods_supported, scopes_supported}'
```

   `issuer` must equal `RUSTFS_IDENTITY_OPENID_ISSUER` when set, otherwise the issuer derived from `RUSTFS_IDENTITY_OPENID_CONFIG_URL`; `code_challenge_methods_supported` must include `S256`; `token_endpoint_auth_methods_supported` must include `client_secret_post`; `scopes_supported` must include every configured scope.

2. Provider visibility: `curl -fsS https://rustfs.example.com/rustfs/admin/v3/oidc/providers | jq` lists the provider unless `hide_from_ui=on`.

3. Browser login: open `https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/default`. Expect a redirect to the IdP, sign-in, a redirect to `/rustfs/admin/v3/oidc/callback/default?code=...&state=...`, and then the Console with the mapped permissions.

4. ID token claims (decode the token after a test login): `iss` matches the issuer, `aud` includes the client id, `email` and `preferred_username` are present when configured, `groups` or `roles` is a flat array of policy names.

## Troubleshooting

| Symptom | Common cause | Fix |
| --- | --- | --- |
| `/oidc/providers` does not list the provider | provider failed to load, or RustFS was not restarted | Check env/admin config and restart RustFS. |
| Provider or login button missing; startup logs `OIDC provider discovery blocked by outbound policy` | IdP origin is private/internal and not allowlisted | Add the exact origin to `RUSTFS_OUTBOUND_ALLOW_ORIGINS` on every node and restart. |
| IdP reports a redirect mismatch (`invalid redirect_uri`) | registered callback differs from RustFS `redirect_uri` | Use the exact `/rustfs/admin/v3/oidc/callback/<provider_id>` URL on both sides. |
| Callback reports missing `code` or `state` | proxy dropped the query string | Preserve the full callback URL and query string. |
| Token exchange fails | wrong secret, or the IdP rejects request-body client authentication | Confirm the client is confidential and accepts `client_secret_post`. |
| No `id_token` in the token response | `openid` scope missing or a non-OIDC OAuth flow | Add `openid`; use the authorization-code flow. |
| ID token verification fails | issuer, audience, algorithm, or JWKS mismatch | Compare discovery metadata with `CONFIG_URL`/`ISSUER`/`CLIENT_ID`; prefer `RS256`. |
| Login succeeds, access denied | no claim value matches a policy name | Emit `groups` or `roles` as a flat array equal to policy names; check for `/` prefixes. |
| Console redirects to an internal host | `RUSTFS_BROWSER_REDIRECT_URL` unset or proxy headers wrong | Set `RUSTFS_BROWSER_REDIRECT_URL` to the public origin. |
| Invalid or expired OIDC state | callback reached a different node | Configure load-balancer session affinity for authorize and callback. |

## Production checklist

- [ ] RustFS and the IdP use HTTPS.
- [ ] The IdP registers the exact callback URL (no wildcard) and `RUSTFS_IDENTITY_OPENID_REDIRECT_URI` matches it.
- [ ] `RUSTFS_BROWSER_REDIRECT_URL` is the public browser origin.
- [ ] PKCE S256 is allowed or required at the IdP.
- [ ] ID tokens carry `groups` or `roles` values equal to RustFS policy names.
- [ ] `role_policy` is not used as a permanent shortcut.
- [ ] The load balancer preserves query strings and pins authorize/callback to one node.
- [ ] Internal IdP origins are listed exactly in `RUSTFS_OUTBOUND_ALLOW_ORIGINS` on every node.

## Keycloak

| Value | Example |
| --- | --- |
| Realm | `rustfs` |
| Issuer (`config_url`) | `https://keycloak.example.com/realms/rustfs` |
| Discovery URL | `https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration` |
| Client id | `rustfs-console` |
| Scopes | `openid,profile,email` |
| Groups claim | `groups` (flat array) |

Client setup in the Keycloak Admin Console:

1. Create or select the realm and confirm discovery returns `issuer` equal to `https://keycloak.example.com/realms/rustfs`.
2. `Clients` → create: `Client type` = `OpenID Connect`, `Client ID` = `rustfs-console`.
3. Enable `Client authentication` and `Standard flow`; disable `Implicit flow`, `Direct access grants`, and `Service accounts roles`.
4. `Valid redirect URIs` = `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default`; `Web origins` = `https://rustfs.example.com`.
5. `Proof Key for Code Exchange Code Challenge Method` = `S256`.
6. Save and copy the secret from `Credentials`. Do not apply a client policy that disables `client_secret_post`.

Group mapper (a `Group Membership` mapper in the client's dedicated scope):

| Mapper field | Value |
| --- | --- |
| Name | `rustfs-groups` |
| Token Claim Name | `groups` |
| Full group path | `Off` (a leading `/` breaks policy matching) |
| Add to ID token / access token / userinfo | `On` |
| Multivalued | `On` |

Create Keycloak groups named after RustFS policies (`consoleAdmin`, `readonly`, ...) and add users to them.

Roles instead of groups: assign realm or client roles named after policies, add a `User Realm Role` or `User Client Role` mapper that emits a flat top-level `roles` claim, and set `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM=roles`. RustFS does not read Keycloak's nested `realm_access.roles` claim.

Internal discovery URL with a public issuer (for example in-cluster Keycloak on Kubernetes):

```bash
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="http://keycloak.keycloak.svc.cluster.local:8080/realms/rustfs"
export RUSTFS_IDENTITY_OPENID_ISSUER="https://keycloak.example.com/realms/rustfs"
export RUSTFS_OUTBOUND_ALLOW_ORIGINS="http://keycloak.keycloak.svc.cluster.local:8080"
```

Discovery and issuer-relative JWKS requests use the `CONFIG_URL` base; `iss` validation uses `ISSUER`. The allowlist entry is the origin only (no realm or discovery path) and is read at startup on every node. Prefer HTTPS with a trusted CA for the internal URL: discovery and JWKS define the token-signing trust root, so plain HTTP is acceptable only where DNS and traffic cannot be tampered with.

## Authing

| Value | Example | Note |
| --- | --- | --- |
| Application domain | `https://example.authing.cn` | From the Authing application page. |
| Issuer (`config_url`) | `https://example.authing.cn/oidc` | Tenants differ (`/oidc`, `/oauth/oidc`): copy the issuer from the console and confirm discovery returns the same `issuer`. |
| App ID / App Secret | `<AUTHING_APP_ID>` / `<AUTHING_APP_SECRET>` | RustFS `client_id` / `client_secret`. |
| Scopes | `openid,profile,email,roles` | `roles` is needed when Authing emits role claims. |
| Roles claim | `roles` | Set `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM=roles`. |

Application settings in the Authing console:

| Setting | Value |
| --- | --- |
| Protocol | OpenID Connect |
| Grant type / response type | Authorization Code / `code` |
| Token endpoint authentication | `client_secret_post` |
| PKCE | allow or require `S256` |
| ID token signing algorithm | `RS256` |
| Redirect URL | `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default` (exact) |

Assign Authing roles whose names equal RustFS policy names; a test user with role `consoleAdmin` should produce `"roles": ["consoleAdmin"]` in the ID token. `claim_prefix` only prepends a fixed string, so keep role values equal to policy names unless policies with that prefix already exist.
