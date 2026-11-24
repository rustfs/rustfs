# Hyper HTTP/2 Expect support

This directory contains the published Hyper 1.12.0 source with the four-file runtime backport of [hyperium/hyper#4217](https://github.com/hyperium/hyper/pull/4217), commit `6d363f774910ac069a830783c23e409c59371fb1`. The released source corresponds to upstream commit `7f67699ce32d717081deb7018b4da7d679277ac0` and crates.io SHA-256 `2c3e324da4c95177d6291d4c8730197c0d1822f8a9766814a4a44fa5ab797c9c`.

The patch sends one HTTP/2 100 Continue response when the service first waits for request body data. A final response without reading the body takes precedence. HTTP/1 behavior is preserved. Non-Expect traffic uses the existing path without a new allocation. No S3 handler or storage behavior is changed.

The upstream PR is not yet released. Its branch is based on Hyper 1.11.1, so pinning that branch would discard newer HTTP/1 framing fixes. The workspace Cargo patch retains 1.12.0 and applies uniformly to transitive Hyper users; this also affects internode and outbound clients, although the new behavior is server-side only.

Regression coverage lives in `rustfs/tests/http_expect_continue.rs` and runs as a normal RustFS integration test. Remove the Cargo patch and this directory when a released Hyper includes equivalent support; keep the regression tests. See [RustFS #9460](https://github.com/rustfs/rustfs/issues/9460).

The upstream MIT license is retained in `LICENSE`. Apart from this provenance document, upstream metadata and source files are preserved to keep the backport directly auditable.
