<!--
Pull Request Template for RustFS
-->

## Type of Change
- [ ] New Feature
- [x] Bug Fix
- [ ] Documentation
- [ ] Performance Improvement
- [x] Test/CI
- [ ] Refactor
- [ ] Other:

## Related Issues
#1301

## Summary of Changes
This PR fixes an issue where the `aws:SourceIp` condition key in IAM and Bucket policies was not being evaluated correctly.

Key changes include:
1.  **IP Propagation**: Threaded the client's `SocketAddr` through the request handling layers (server -> handlers -> auth) to ensure the actual remote address is available during authentication.
2.  **Correct IP Extraction**: Updated `auth.rs` to use `rustfs_utils::http::ip::get_source_ip_raw`. This ensures that we correctly identify the client IP, respecting standard headers like `X-Forwarded-For` when behind a proxy, aligning behavior with MinIO.
3.  **Policy Evaluation**: Updated `get_condition_values` to populate the `SourceIp` key in the condition context, enabling the policy engine to validate IP address restrictions.
4.  **Testing**: Added comprehensive unit/integration tests in `rustfs/src/auth.rs` covering:
    *   IAM User Policy evaluation with `aws:SourceIp`.
    *   Bucket Policy evaluation with `aws:SourceIp`.
    *   Verification of both allowed (matching IP) and denied (non-matching IP) scenarios.

## Checklist
- [x] I have read and followed the [CONTRIBUTING.md](CONTRIBUTING.md) guidelines
- [x] Passed `make pre-commit`
- [x] Added/updated necessary tests
- [ ] Documentation updated (if needed)
- [x] CI/CD passed (if applicable)

## Impact
- [ ] Breaking change (compatibility)
- [ ] Requires doc/config/deployment update
- [ ] Other impact:

## Additional Notes
The fix ensures that security policies relying on IP restrictions function as expected. Code formatting has been applied to all modified files.

---

Thank you for your contribution! Please ensure your PR follows the community standards ([CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)) and sign the CLA if this is your first contribution.
