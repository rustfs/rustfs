# OIDC Vendor Compatibility Checklist

Use this checklist when a vendor provides an OAuth or SSO document that is described as OIDC but does not clearly expose the standard OpenID Connect contract required by RustFS.

## 1. Discovery Metadata

RustFS expects provider metadata at:

```text
GET {issuer}/.well-known/openid-configuration
```

Ask the vendor to provide the discovery URL and confirm that it returns at least:

- `issuer`
- `authorization_endpoint`
- `token_endpoint`
- `jwks_uri`
- `response_types_supported`
- `subject_types_supported`
- `id_token_signing_alg_values_supported`

The returned `issuer` must exactly match the issuer configured in RustFS.

## 2. JWKS and Token Signature Verification

RustFS must verify the ID token signature. Ask the vendor to provide:

- `jwks_uri`
- supported signing algorithms, such as `RS256`
- key rotation behavior
- how the token `kid` maps to the JWKS key set

Without a verifiable ID token signature, the provider is not suitable for RustFS OIDC login.

## 3. Authorization Request Parameters

The provider must accept the standard authorization-code request parameters:

- `scope=openid profile email`
- `response_type=code`
- `client_id`
- `redirect_uri`
- `state`
- `nonce`
- `code_challenge`
- `code_challenge_method=S256`

If the vendor example omits `state`, `nonce`, or PKCE, confirm whether those parameters are supported.

## 4. Callback State

The provider must return the original `state` value in the callback:

```text
...?code=xxx&state=yyy
```

RustFS uses `state` for CSRF protection and to find the in-flight OIDC session. A callback that only returns `code` is not enough.

## 5. Token Response

The token endpoint response must be JSON and include at least:

- `access_token`
- `token_type`, usually `Bearer`
- `expires_in`
- `id_token`

RustFS requires `id_token`; an OAuth-only access token is not sufficient for Console OIDC login.

## 6. ID Token Claims

The ID token must contain standard claims that RustFS can verify:

- `iss`
- `sub`
- `aud`
- `exp`
- `iat`
- `nonce` when the authorization request includes `nonce`

Ask the vendor for a sample ID token payload and claim documentation.

## 7. UserInfo Endpoint

Standard OIDC UserInfo normally uses:

```text
GET /userinfo
Authorization: Bearer <access_token>
```

If the vendor only documents a private profile endpoint such as `/oidc/profile?access_token=...`, ask whether a standard `userinfo_endpoint` is available and returned in discovery.

## 8. Logout Endpoint

Standard RP-initiated logout is normally exposed through an `end_session_endpoint` in discovery. If the vendor only documents a private token removal endpoint, ask whether standard OIDC logout is available.

RustFS can still fall back to the Console login page when the provider does not advertise an end-session endpoint.

## 9. Authorization Claims

OIDC primarily authenticates the user. RustFS authorization is still based on RustFS policies. The provider must emit claims that can be mapped to RustFS policies, for example:

- `groups`
- `roles`
- `policy`
- another agreed flat array or string claim

Ask the vendor to confirm:

- whether group, role, or policy claims can be included in the ID token
- whether those claims can be included in UserInfo
- the exact claim names and value formats
- whether the claim values can match RustFS policy names such as `consoleAdmin`, `readwrite`, or `readonly`

If the provider only returns a user id or token validity result, it can authenticate the user but cannot by itself express RustFS authorization.

## 10. RustFS Redirect Requirements

RustFS browser-facing redirect behavior depends on these values:

- provider `redirect_uri`, when explicitly configured, is the callback URL sent to the provider
- `RUSTFS_BROWSER_REDIRECT_URL` is the public RustFS browser origin used for callback generation when no provider `redirect_uri` exists, and for Console success and logout fallback redirects
- dynamic request-header redirects are used only when no configured redirect source exists and dynamic redirects are enabled

Ask the vendor to register the exact callback URL, for example:

```text
https://rustfs.example.com/rustfs/admin/v3/oidc/callback/default
```

For load-balanced RustFS deployments, ensure authorize and callback requests reach the same RustFS node while the OIDC `state` is in flight.
