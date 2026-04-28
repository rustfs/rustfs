## Type of Change
- [ ] New Feature
- [ ] Bug Fix
- [ ] Documentation
- [ ] Performance Improvement
- [x] Test/CI
- [ ] Refactor
- [ ] Other:

## Related Issues
N/A

## Summary of Changes
Recent signer hardening in `origin/main` added explicit fallback/error paths around host-header resolution and signer-header error propagation, but those helper branches were still only covered indirectly. This PR adds focused regression tests for the newly introduced helper behavior without changing runtime logic.

In `crates/signer/src/utils.rs`, the new tests cover three edge cases in `try_get_host_addr`: preferring an explicit `Host` header when it intentionally differs from the URI authority, rejecting non-UTF-8 `Host` header values, and rejecting relative URIs that do not carry a host. These are the exact branches introduced by the signer hardening work and help ensure the higher-level `try_sign_v4` behavior continues to rest on a directly tested helper contract.

In `crates/ecstore/src/client/signer_error.rs`, the new tests verify that invalid UTF-8 signer-header failures keep the internal marker in the error chain and that unrelated I/O errors do not accidentally match that marker. That closes the remaining gap for the breaker/error-classification path added in the recent ILM remote-delete hardening.

## Checklist
- [x] I have read and followed the [CONTRIBUTING.md](CONTRIBUTING.md) guidelines
- [x] Passed `make pre-commit`
- [x] Added/updated necessary tests
- [ ] Documentation updated (if needed)
- [ ] CI/CD passed (if applicable)

## Impact
- [ ] Breaking change (compatibility)
- [ ] Requires doc/config/deployment update
- [x] Other impact:
This change is test-only and does not modify runtime behavior.

## Additional Notes
Validation used:

- `cargo test -p rustfs-signer try_get_host_addr_ --lib`
- `cargo test -p rustfs-ecstore signer_error --lib -- --nocapture`
- `make pre-commit`
