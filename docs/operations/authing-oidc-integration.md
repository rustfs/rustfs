# Authing OIDC Integration Runbook

This runbook helps operators connect the RustFS Console to Authing through standard OpenID Connect. The examples use the default RustFS provider id, `default`.

## 1. Integration Model

RustFS expects a standards-compliant OpenID Connect provider, not an Authing-specific plugin. The Authing application must provide:

- issuer metadata through `.well-known/openid-configuration`
- authorization endpoint
- token endpoint
- JWKS or another verifiable ID token signature path
- authorization-code flow that returns an `id_token`

The RustFS browser login flow is:

1. The user opens the RustFS OIDC authorize endpoint.
2. RustFS creates `state`, `nonce`, and a PKCE S256 challenge.
3. The browser is redirected to Authing.
4. Authing redirects back to RustFS with `code` and `state`.
5. RustFS exchanges the code with `client_id`, `client_secret`, and the PKCE verifier.
6. RustFS validates the ID token signature, issuer, audience, expiry, and nonce.
7. RustFS reads identity and authorization claims from the ID token.
8. RustFS maps claim values to RustFS policy names and issues one-hour STS credentials for the Console.

## 2. Required Values

Collect these values before deployment:

| Value | Example | Notes |
| --- | --- | --- |
| Public RustFS browser origin | `https://rustfs.example.com` | The scheme and authority users open in the browser. |
| Provider id | `default` | This runbook uses the default provider. |
| RustFS callback URL | `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default` | Register this exact URL in Authing. |
| Authing application domain | `https://example.authing.cn` | Use the value shown in the Authing application. |
| Authing issuer | `https://example.authing.cn/oidc` | Copy the issuer from Authing; do not guess the path. |
| Authing App ID | `<AUTHING_APP_ID>` | RustFS `client_id`. |
| Authing App Secret | `<AUTHING_APP_SECRET>` | RustFS `client_secret`. |
| RustFS scopes | `openid,profile,email,roles` | `openid` is required; include `roles` when Authing emits role claims. |

Authing deployments can use different issuer paths, such as `/oidc` or `/oauth/oidc`. Always copy the issuer from the Authing console and verify that discovery returns the same `issuer` value.

## 3. Authing Configuration

### 3.1 Create the Application

1. Open the Authing console.
2. Create a self-hosted application named `RustFS Console`.
3. Record the App ID, App Secret, application domain, issuer, and discovery URL.

### 3.2 Configure OIDC

Use these protocol settings:

| Setting | Value |
| --- | --- |
| Protocol | OpenID Connect |
| Grant type | Authorization Code |
| Response type | `code` |
| Token endpoint authentication | `client_secret_post` |
| PKCE | Allow or require `S256` |
| ID token signing algorithm | `RS256` recommended |

RustFS sends the client secret in the request body. Do not configure Authing to reject `client_secret_post`.

### 3.3 Register the Redirect URL

Add this exact callback URL in Authing:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default
```

The scheme, host, port, path, and provider id must match the RustFS configuration.

### 3.4 Map Roles to RustFS Policies

RustFS does not call Authing authorization APIs. It reads `roles` or `groups` from the ID token and maps each value to a RustFS policy name.

Recommended policy names:

| Authing claim value | RustFS policy | Purpose |
| --- | --- | --- |
| `consoleAdmin` | `consoleAdmin` | Full Console, admin, KMS, and S3 access. |
| `readwrite` | `readwrite` | S3 read/write access. |
| `readonly` | `readonly` | S3 read-only access. |
| `writeonly` | `writeonly` | S3 write-only access. |
| `diagnostics` | `diagnostics` | Diagnostic admin access. |

For initial validation, assign a test user the `consoleAdmin` role and confirm that the ID token contains:

```json
{
  "roles": ["consoleAdmin"]
}
```

`claim_prefix` only prepends a fixed string. It does not perform arbitrary role mapping. Keep Authing role values equal to RustFS policy names unless you already created policies with a fixed prefix.

## 4. RustFS Configuration

### 4.1 Environment Variables

Set the OIDC provider and the public browser origin:

```bash
export RUSTFS_BROWSER_REDIRECT_URL="https://rustfs.example.com"

export RUSTFS_IDENTITY_OPENID_ENABLE=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="<AUTHING_ISSUER>"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID="<AUTHING_APP_ID>"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET="<AUTHING_APP_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES="openid,profile,email,roles"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME="Authing"
export RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM="email"
export RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM="preferred_username"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"
```

For short-lived connectivity testing only, you may temporarily add:

```bash
export RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

Do not keep `role_policy=consoleAdmin` in production unless every Authing user for this client should receive full Console access.

Restart RustFS after changing OIDC settings.

### 4.2 Admin Config

If the deployment manages OIDC through compatible admin configuration commands, set the provider like this:

```bash
mc admin config set rustfs identity_openid \
  enable=on \
  config_url="<AUTHING_ISSUER>" \
  client_id="<AUTHING_APP_ID>" \
  client_secret="<AUTHING_APP_SECRET>" \
  scopes="openid,profile,email,roles" \
  redirect_uri="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default" \
  redirect_uri_dynamic=off \
  display_name="Authing" \
  email_claim="email" \
  username_claim="preferred_username" \
  roles_claim="roles"

mc admin service restart rustfs
```

`RUSTFS_BROWSER_REDIRECT_URL` is a process environment variable, not an `identity_openid` provider key. Configure it in the RustFS service environment even when the provider itself is stored through admin config.

### 4.3 Redirect URL Priority

RustFS builds browser-facing URLs with this priority:

1. Provider `redirect_uri`, when configured, is used for the OIDC callback URL sent to Authing.
2. `RUSTFS_BROWSER_REDIRECT_URL`, when configured, is used as the public origin for OIDC callback generation when no provider `redirect_uri` exists, and for Console success redirects and logout fallback redirects.
3. Request headers are used only when provider dynamic redirects are enabled and no browser redirect URL is configured.

For reverse-proxy or load-balancer deployments, set `RUSTFS_BROWSER_REDIRECT_URL` to avoid depending on `Host` and `X-Forwarded-Proto` for Console redirects. OIDC authorize and callback requests must still reach the same RustFS node because in-flight OIDC `state` is local to the node.

## 5. Validation

### 5.1 Validate Authing Discovery

```bash
AUTHING_ISSUER="<AUTHING_ISSUER>"

curl -fsS "$AUTHING_ISSUER/.well-known/openid-configuration" | jq '{
  issuer,
  authorization_endpoint,
  token_endpoint,
  jwks_uri,
  id_token_signing_alg_values_supported,
  code_challenge_methods_supported,
  token_endpoint_auth_methods_supported,
  scopes_supported
}'
```

Check that:

- `issuer` exactly matches `RUSTFS_IDENTITY_OPENID_CONFIG_URL`
- `authorization_endpoint`, `token_endpoint`, and `jwks_uri` are present
- `code_challenge_methods_supported` includes `S256`
- `token_endpoint_auth_methods_supported` includes `client_secret_post`
- `scopes_supported` includes `openid`, `profile`, `email`, and any role scope you need

### 5.2 Validate RustFS Provider Visibility

```bash
curl -fsS "https://rustfs.example.com/rustfs/admin/v3/oidc/providers" | jq
```

The response should include the Authing provider unless `hide_from_ui` is enabled.

### 5.3 Test Browser Login

Open:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/default
```

Expected flow:

1. Browser redirects to Authing.
2. The user signs in.
3. Authing redirects to `/rustfs/admin/v3/oidc/callback/default?code=...&state=...`.
4. RustFS validates the ID token and issues STS credentials.
5. The browser lands on the RustFS Console and can use the expected permissions.

## 6. Troubleshooting

| Symptom | Common cause | Fix |
| --- | --- | --- |
| `/oidc/providers` does not show Authing | OIDC provider did not load, or RustFS was not restarted | Check environment variables and restart RustFS. |
| Authing reports redirect mismatch | Callback URL differs between Authing and RustFS | Use the exact `/rustfs/admin/v3/oidc/callback/default` URL. |
| RustFS reports missing `code` or `state` | Proxy dropped the query string | Preserve the full callback URL and query string. |
| Token exchange fails | Wrong client secret or unsupported token auth method | Confirm `client_secret_post` is allowed. |
| RustFS reports no `id_token` | Missing `openid` scope or non-OIDC OAuth flow | Include `openid` and use OIDC authorization code flow. |
| ID token verification fails | Issuer, audience, signing algorithm, or JWKS mismatch | Compare discovery metadata with RustFS config; prefer `RS256`. |
| Login succeeds but access is denied | No matching RustFS policy claim | Ensure `roles` or `groups` is in the ID token and equals a RustFS policy name. |
| Console redirects to an internal host | Missing `RUSTFS_BROWSER_REDIRECT_URL` or incorrect proxy headers | Set `RUSTFS_BROWSER_REDIRECT_URL` to the public browser origin. |
| Invalid or expired OIDC state | Callback reached a different RustFS node | Configure load-balancer session affinity for authorize and callback requests. |

## 7. Production Checklist

- [ ] RustFS and Authing use HTTPS.
- [ ] Authing redirect URL is exact, not a broad wildcard.
- [ ] `RUSTFS_BROWSER_REDIRECT_URL` is set to the public RustFS browser origin.
- [ ] `RUSTFS_IDENTITY_OPENID_REDIRECT_URI` matches the registered Authing callback URL.
- [ ] Authing emits role or group claims in the ID token.
- [ ] Claim values match RustFS policy names.
- [ ] `role_policy=consoleAdmin` is not used as a permanent production shortcut.
- [ ] The load balancer preserves query strings.
- [ ] OIDC authorize and callback requests have session affinity to the same RustFS node.
