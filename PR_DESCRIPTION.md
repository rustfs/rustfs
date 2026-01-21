## Type of Change
- [x] Bug Fix
- [ ] New Feature
- [ ] Documentation
- [ ] Performance Improvement
- [ ] Test/CI
- [ ] Refactor
- [ ] Other:

## Related Issues
<!-- List related Issue numbers, e.g. #123 -->

## Summary of Changes

This PR fixes two issues in the `list_object_versions` API implementation:

1. **max_keys field correction**: The `max_keys` field in the response was incorrectly set to the actual number of keys returned, instead of the requested maximum number of keys. According to the AWS S3 API specification, the `max_keys` field should represent the maximum number of keys that can be returned in the response (i.e., the value requested by the client), not the actual count of keys returned.

2. **is_truncated calculation improvement**: Improved the `is_truncated` calculation logic to check entries count against limit, which provides a more accurate indication of truncation. The previous logic only checked `get_objects.len()` which may be filtered (e.g., directories), so checking the raw entries count against limit helps ensure proper pagination.

These fixes improve S3 API compliance and help ensure proper pagination when `list_object_versions` is used for bucket cleanup operations.

## Checklist
- [x] I have read and followed the [CONTRIBUTING.md](CONTRIBUTING.md) guidelines
- [x] Passed `cargo fmt --all --check`
- [x] Passed `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] Added/updated necessary tests
- [ ] Documentation updated (if needed)
- [ ] CI/CD passed (if applicable)

## Impact
- [ ] Breaking change (compatibility)
- [ ] Requires doc/config/deployment update
- [x] Other impact: This is a bug fix that improves S3 API compliance. The change makes the response more accurate and aligned with AWS S3 behavior.

## Additional Notes

These fixes were identified while investigating S3 compatibility test failures. The `test_bucket_list_delimiter_basic` test passes, but there are teardown issues related to `list_object_versions` cleanup where the bucket cannot be deleted because it's not empty.

The fixes address:
- Correct `max_keys` field to reflect the requested maximum rather than the actual count
- Improved `is_truncated` calculation to better detect when there are more objects to list

However, the teardown issue may still persist as it could be related to other factors such as:
- Pagination logic in `list_object_versions` not correctly listing all objects
- Delete operations not properly handling objects returned from `list_object_versions`
- Test framework cleanup logic expecting different behavior

The changes are minimal and focused, ensuring better S3 API compliance. Further investigation may be needed to fully resolve the teardown issue.
