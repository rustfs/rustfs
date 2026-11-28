# [OpenID Connect](https://openid.net/specs/openid-connect-core-1_0.html) Library for Rust

[![crates.io](https://img.shields.io/crates/v/openidconnect.svg)](https://crates.io/crates/openidconnect)
[![docs.rs](https://docs.rs/openidconnect/badge.svg)](https://docs.rs/openidconnect)
[![Build Status](https://github.com/ramosbugs/openidconnect-rs/actions/workflows/main.yml/badge.svg)](https://github.com/ramosbugs/openidconnect-rs/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/ramosbugs/openidconnect-rs/branch/main/graph/badge.svg)](https://codecov.io/gh/ramosbugs/openidconnect-rs)

This library provides extensible, strongly-typed interfaces for the OpenID
Connect protocol, which can be used to authenticate users via
[Google](https://developers.google.com/identity/openid-connect/openid-connect),
[GitLab](https://docs.gitlab.com/ee/integration/openid_connect_provider.html),
[Microsoft](https://learn.microsoft.com/en-us/entra/identity-platform/v2-protocols-oidc),
and [many other providers](https://openid.net/certification/#OPENID-OP-P).

API documentation and examples are available on [docs.rs](https://docs.rs/openidconnect).

## Minimum Supported Rust Version (MSRV)

The MSRV for *4.1* and newer releases of this crate is Rust **1.88**.

The MSRV for *3.3* to *4.0* releases of this crate is Rust **1.65**.

The MSRV for *3.0* to *3.2* releases of this crate is Rust **1.57**.

The MSRV for *2.x* releases of this crate is Rust 1.45.

Since the 3.0.0 release, this crate maintains a policy of supporting
Rust releases going back at least 6 months. Changes that break compatibility with Rust releases
older than 6 months will no longer be considered SemVer breaking changes and will not result in a
new major version number for this crate. MSRV changes will coincide with minor version updates
and will not happen in patch releases.

## Standards

* [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html)
  * Supported features:
    * Relying Party flows: code, implicit, hybrid
    * Standard claims
    * UserInfo endpoint
    * RSA, HMAC, ECDSA (P-256/P-384 curves) and EdDSA (Ed25519 curve) ID token verification
  * Unsupported features:
    * Aggregated and distributed claims
    * Passing request parameters as JWTs
    * Verification of the `azp` claim (see [discussion](https://bitbucket.org/openid/connect/issues/973/))
    * ECDSA-based ID token verification using the P-521 curve
    * JSON Web Encryption (JWE)
* [OpenID Connect Discovery](https://openid.net/specs/openid-connect-discovery-1_0.html)
  * Supported features:
    * Provider Metadata
  * Unsupported features:
    * WebFinger
* [OpenID Connect Dynamic Client Registration](https://openid.net/specs/openid-connect-registration-1_0.html)
  * Supported features:
    * Client Metadata
    * Client Registration endpoint
  * Unsupported features:
    * Client Configuration endpoint
* [OpenID Connect RP-Initiated Logout](https://openid.net/specs/openid-connect-rpinitiated-1_0.html)
* [OAuth 2.0 Token Introspection](https://tools.ietf.org/html/rfc7662)
* [OAuth 2.0 Token Revocation](https://tools.ietf.org/html/rfc7009)
* [OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/rfc/rfc8628)
