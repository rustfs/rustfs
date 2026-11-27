# openidconnect rsa 0.10

This directory contains the published `openidconnect` 4.1.0 crate (crates.io SHA-256 `df0d098a59353679624d406825940139e2b2d554a52b64c9fb1b1bc42a11fe5c`) with the dependency moved from `rsa` 0.9.2 to `rsa` 0.10.0. Upstream 4.1.0 is still the latest release and still requires `rsa ^0.9.2`, which cargo-deny reports as `RUSTSEC-2023-0071` (Marvin timing sidechannel, patched in `rsa >= 0.10.0`). Tracking issue: [ramosbugs/openidconnect-rs#140](https://github.com/ramosbugs/openidconnect-rs/issues/140).

The patch keeps ID-token verification on the same JWS algorithms. PKCS#1 v1.5 and PSS padding now use `rsa`'s own SHA-2 types, while the hashed message bytes are still produced by this crate's `sha2` 0.10 dependency. JWK modulus and exponent values are decoded as unsigned big-endian integers with leading `0x00` octets removed, matching the previous `num-bigint` decode. RSA signing adapts the existing `rand` 0.8 generator to `rand_core` 0.10 because `rsa` 0.10 requires that trait for blinding and PSS salt. RustFS calls this crate for public-key ID-token verification and confidential-client secrets; it does not use the RSA private signing type on the request path.

No registry, git, or example files from the published crate are omitted except VCS metadata and the upstream lockfiles. One trailing-whitespace line in `UPGRADE.md` was removed so the tree passes whitespace checks. The workspace lockfile is authoritative. Remove this directory and the `openidconnect` Cargo patch when a released `openidconnect` depends on `rsa >= 0.10.0`.

The upstream MIT license is retained in `LICENSE`.
