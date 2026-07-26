# Keycloak OIDC Integration Runbook

This runbook describes how to connect the RustFS Console to Keycloak by using OpenID Connect Authorization Code Flow. The examples use the default RustFS provider id, `default`.

## 1. Integration Model

RustFS supports standard OpenID Connect for Console login:

- RustFS sends an authorization-code request with PKCE S256.
- Keycloak redirects back with `code` and `state`.
- RustFS exchanges the code at the token endpoint.
- RustFS requires an `id_token` and verifies signature, issuer, audience, expiry, and nonce.
- RustFS maps ID token claim values to local RustFS IAM policies.

RustFS does not call Keycloak Authorization Services for object or admin authorization. Authorization is handled by RustFS policies after claims are mapped.

## 2. Example Values

Replace these values for your environment:

| Value | Example | Notes |
| --- | --- | --- |
| Keycloak base URL | `https://keycloak.example.com` | Public Keycloak URL. |
| Realm | `rustfs` | Keycloak realm name. |
| Keycloak issuer | `https://keycloak.example.com/realms/rustfs` | RustFS `config_url`. |
| Discovery URL | `https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration` | Used to validate metadata. |
| Public RustFS browser origin | `https://rustfs.example.com` | The scheme and authority users open in the browser. |
| Provider id | `default` | This runbook uses the default provider. |
| RustFS callback URL | `https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default` | Register this exact URL in Keycloak. |
| Keycloak client id | `rustfs-console` | OIDC client used by RustFS. |
| Keycloak client secret | `<KEYCLOAK_CLIENT_SECRET>` | Confidential client secret. |
| RustFS scopes | `openid,profile,email` | Add custom scopes if they emit authorization claims. |
| RustFS groups claim | `groups` | Recommended flat array claim. |
| RustFS roles claim | `roles` | Optional flat array claim. |

## 3. Keycloak Configuration

### 3.1 Create or Select the Realm

1. Open the Keycloak Admin Console.
2. Create or select the `rustfs` realm.
3. Verify discovery:

```bash
curl -fsS "https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration" \
  | jq '.issuer,.authorization_endpoint,.token_endpoint,.jwks_uri'
```

The `issuer` should be:

```text
https://keycloak.example.com/realms/rustfs
```

### 3.2 Create the RustFS Client

In the Keycloak Admin Console:

1. Open `Clients` and create a client.
2. Set `Client type` to `OpenID Connect`.
3. Set `Client ID` to `rustfs-console`.
4. Enable `Client authentication`.
5. Enable `Standard flow`.
6. Disable unused flows such as `Implicit flow`, `Direct access grants`, and `Service accounts roles`.
7. Set `Valid redirect URIs` to:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default
```

8. Set `Web origins` to:

```text
https://rustfs.example.com
```

9. Set `Proof Key for Code Exchange Code Challenge Method` to `S256`.
10. Save and copy the client secret from `Credentials`.

RustFS submits the client secret in the token request body. Do not use a client policy that disables `client_secret_post`.

### 3.3 Map Groups or Roles to RustFS Policies

RustFS policy names are the final authorization source. Common built-in policies are:

| Policy | Purpose |
| --- | --- |
| `consoleAdmin` | Full Console, admin, KMS, and S3 access. |
| `readwrite` | S3 read/write access. |
| `readonly` | S3 read-only access. |
| `writeonly` | S3 write-only access. |
| `diagnostics` | Diagnostic admin access. |

Recommended production setup:

1. Create Keycloak groups such as `consoleAdmin` and `readonly`.
2. Add users to the groups.
3. Add a group membership mapper for the `rustfs-console` client.
4. Emit a flat top-level ID token claim named `groups`.
5. Keep group values equal to RustFS policy names.

### 3.4 Group Claim Mapper

Create a `Group Membership` mapper in the dedicated client scope:

| Mapper field | Value |
| --- | --- |
| Name | `rustfs-groups` |
| Token Claim Name | `groups` |
| Full group path | `Off` |
| Add to ID token | `On` |
| Add to access token | `On` |
| Add to userinfo | `On` |
| Multivalued | `On` |

Keep `Full group path` disabled. RustFS policy names cannot contain `/`, so `/consoleAdmin` will not map to the `consoleAdmin` policy.

### 3.5 Optional Role Claim Mapper

If the deployment uses Keycloak roles:

1. Assign realm or client roles such as `consoleAdmin`.
2. Add a `User Realm Role` or `User Client Role` mapper.
3. Emit a flat top-level claim named `roles`.
4. Set `RUSTFS_IDENTITY_OPENID_ROLES_CLAIM=roles`.

RustFS does not parse Keycloak's default nested `realm_access.roles` claim. Emit a flat `roles` array when role mapping is required.

## 4. RustFS Configuration

### 4.1 Environment Variables

Configure the provider and the public browser origin:

```bash
export RUSTFS_BROWSER_REDIRECT_URL="https://rustfs.example.com"

export RUSTFS_IDENTITY_OPENID_ENABLE=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="https://keycloak.example.com/realms/rustfs"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID="rustfs-console"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET="<KEYCLOAK_CLIENT_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES="openid,profile,email"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME="Keycloak"
export RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM="groups"
export RUSTFS_IDENTITY_OPENID_ROLES_CLAIM="roles"
export RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM="email"
export RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM="preferred_username"
```

If RustFS reaches Keycloak through an internal URL while tokens use a public issuer, configure both values:

```bash
export RUSTFS_IDENTITY_OPENID_CONFIG_URL="http://keycloak.keycloak.svc.cluster.local:8080/realms/rustfs/.well-known/openid-configuration"
export RUSTFS_IDENTITY_OPENID_ISSUER="https://keycloak.example.com/realms/rustfs"
```

Discovery and issuer-relative JWKS requests use the internal `CONFIG_URL` base. ID token issuer validation still uses `ISSUER`.
Use HTTPS with a trusted CA for the internal URL whenever possible. Discovery and JWKS define the token-signing trust root; use HTTP only on a network where DNS and traffic cannot be tampered with, because a compromised response can authorize forged tokens.

For short-lived connectivity testing only, you may temporarily add:

```bash
export RUSTFS_IDENTITY_OPENID_ROLE_POLICY="consoleAdmin"
```

Do not use the temporary `role_policy` shortcut as a permanent production authorization model.

Restart RustFS after changing OIDC settings.

### 4.2 Admin Config

If the deployment uses compatible admin configuration commands:

```bash
mc admin config set rustfs identity_openid \
  enable=on \
  config_url="https://keycloak.example.com/realms/rustfs" \
  client_id="rustfs-console" \
  client_secret="<KEYCLOAK_CLIENT_SECRET>" \
  scopes="openid,profile,email" \
  redirect_uri="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default" \
  redirect_uri_dynamic=off \
  display_name="Keycloak" \
  groups_claim="groups" \
  roles_claim="roles" \
  email_claim="email" \
  username_claim="preferred_username"

mc admin service restart rustfs
```

`RUSTFS_BROWSER_REDIRECT_URL` is a process environment variable, not an `identity_openid` provider key. Configure it in the RustFS service environment even when the provider is stored through admin config.

### 4.3 Named Provider

To use a provider id such as `keycloak`, register this callback URL in Keycloak:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/keycloak
```

Then suffix the provider-specific environment variables:

```bash
export RUSTFS_IDENTITY_OPENID_ENABLE_keycloak=on
export RUSTFS_IDENTITY_OPENID_CONFIG_URL_keycloak="https://keycloak.example.com/realms/rustfs"
export RUSTFS_IDENTITY_OPENID_CLIENT_ID_keycloak="rustfs-console"
export RUSTFS_IDENTITY_OPENID_CLIENT_SECRET_keycloak="<KEYCLOAK_CLIENT_SECRET>"
export RUSTFS_IDENTITY_OPENID_SCOPES_keycloak="openid,profile,email"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_keycloak="https://rustfs.example.com/rustfs/admin/v3/oidc/callback/keycloak"
export RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC_keycloak=off
export RUSTFS_IDENTITY_OPENID_DISPLAY_NAME_keycloak="Keycloak"
export RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM_keycloak="groups"
```

`RUSTFS_BROWSER_REDIRECT_URL` remains global and is not suffixed per provider.

### 4.4 Redirect URL Priority

RustFS builds browser-facing URLs with this priority:

1. Provider `redirect_uri`, when configured, is used for the OIDC callback URL sent to Keycloak.
2. `RUSTFS_BROWSER_REDIRECT_URL`, when configured, is used as the public origin for OIDC callback generation when no provider `redirect_uri` exists, and for Console success redirects and logout fallback redirects.
3. Request headers are used only when provider dynamic redirects are enabled and no browser redirect URL is configured.

For reverse-proxy or load-balancer deployments, set `RUSTFS_BROWSER_REDIRECT_URL` to avoid depending on `Host` and `X-Forwarded-Proto` for Console redirects. OIDC authorize and callback requests must still reach the same RustFS node because in-flight OIDC `state` is local to the node.

## 5. Validation

### 5.1 Validate Discovery

```bash
curl -fsS "https://keycloak.example.com/realms/rustfs/.well-known/openid-configuration" | jq '{
  issuer,
  authorization_endpoint,
  token_endpoint,
  jwks_uri,
  code_challenge_methods_supported,
  token_endpoint_auth_methods_supported
}'
```

Check that:

- `issuer` equals `RUSTFS_IDENTITY_OPENID_ISSUER` when set; otherwise it matches the issuer derived from `RUSTFS_IDENTITY_OPENID_CONFIG_URL`
- `authorization_endpoint`, `token_endpoint`, and `jwks_uri` are present
- `code_challenge_methods_supported` includes `S256`
- the token endpoint accepts client secret authentication compatible with request-body submission

### 5.2 Validate ID Token Claims

After a test login, decode the ID token and confirm:

- `iss` matches the Keycloak issuer
- `aud` includes the RustFS client id
- `email` and `preferred_username` are present when configured
- `groups` or `roles` contains RustFS policy names if fine-grained authorization is enabled

### 5.3 Test Browser Login

Open:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/authorize/default
```

Expected flow:

1. Browser redirects to Keycloak.
2. The user signs in.
3. Keycloak redirects to `/rustfs/admin/v3/oidc/callback/default?code=...&state=...`.
4. RustFS validates the ID token and issues STS credentials.
5. The browser lands on the RustFS Console and can use the expected permissions.

## 6. Troubleshooting

| Symptom | Common cause | Fix |
| --- | --- | --- |
| Keycloak reports `invalid redirect_uri` | Valid Redirect URIs does not match the RustFS callback URL | Use the exact callback URL and provider id. |
| Callback reports missing `code` or `state` | Proxy dropped the query string | Preserve the full callback URL and query string. |
| Token exchange fails | Client secret or client authentication policy mismatch | Confirm the client is confidential and accepts request-body secret auth. |
| RustFS reports no `id_token` | Missing `openid` scope or disabled Standard Flow | Include `openid` and enable Standard Flow. |
| ID token verification fails | Issuer, client id, audience, or JWKS mismatch | Compare discovery metadata and client settings. |
| Login succeeds but access is denied | No RustFS policy claim was mapped | Emit `groups` or `roles` as a flat ID token claim matching RustFS policy names. |
| Groups appear as `/consoleAdmin` | Keycloak `Full group path` is enabled | Disable `Full group path`. |
| Console redirects to an internal host | Missing `RUSTFS_BROWSER_REDIRECT_URL` or incorrect proxy headers | Set `RUSTFS_BROWSER_REDIRECT_URL` to the public browser origin. |
| Invalid or expired OIDC state | Callback reached a different RustFS node | Configure load-balancer session affinity for authorize and callback requests. |

## 7. Production Checklist

- [ ] Keycloak and RustFS use HTTPS.
- [ ] Keycloak Valid Redirect URIs uses exact callback URLs.
- [ ] `RUSTFS_BROWSER_REDIRECT_URL` is set to the public RustFS browser origin.
- [ ] `RUSTFS_IDENTITY_OPENID_REDIRECT_URI` matches the registered Keycloak callback URL.
- [ ] PKCE S256 is enabled or required.
- [ ] Users receive `groups` or `roles` claims that match RustFS policy names.
- [ ] `role_policy=consoleAdmin` is not used as a permanent production shortcut.
- [ ] The load balancer preserves query strings.
- [ ] OIDC authorize and callback requests have session affinity to the same RustFS node.
