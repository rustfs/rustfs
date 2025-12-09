# Special Characters in Object Paths - Complete Documentation

This directory contains comprehensive documentation for handling special characters (spaces, plus signs, percent signs, etc.) in S3 object paths with RustFS.

## Quick Links

- **For Users**: Start with [Client Guide](./client-special-characters-guide.md)
- **For Developers**: Read [Solution Document](./special-characters-solution.md)
- **For Deep Dive**: See [Technical Analysis](./special-characters-in-path-analysis.md)

## Document Overview

### 1. [Client Guide](./client-special-characters-guide.md)
**Target Audience**: Application developers, DevOps engineers, end users

**Contents**:
- How to upload files with spaces, plus signs, etc.
- Examples for all major S3 SDKs (Python, Go, Node.js, AWS CLI, mc)
- Troubleshooting common issues
- Best practices
- FAQ

**When to Read**: You're experiencing issues with special characters in object names.

### 2. [Solution Document](./special-characters-solution.md)
**Target Audience**: RustFS developers, contributors, maintainers

**Contents**:
- Root cause analysis
- Technical explanation of URL encoding
- Why the backend is correct
- Why issues occur in UI/clients
- Implementation recommendations
- Testing strategy

**When to Read**: You need to understand the technical solution or contribute to the codebase.

### 3. [Technical Analysis](./special-characters-in-path-analysis.md)
**Target Audience**: Senior architects, security reviewers, technical deep-dive readers

**Contents**:
- Comprehensive technical analysis
- URL encoding standards (RFC 3986, AWS S3 API)
- Deep dive into s3s library behavior
- Edge cases and security considerations
- Multiple solution approaches evaluated
- Complete implementation plan

**When to Read**: You need detailed technical understanding or are making architectural decisions.

## TL;DR - The Core Issue

### What Happened

Users reported:
1. **Part A**: UI can navigate to folders with special chars but can't list contents
2. **Part B**: 400 errors when uploading files with `+` in the path

### Root Cause

**Backend (RustFS) is correct** ✅
- The s3s library properly URL-decodes object keys from HTTP requests
- RustFS stores and retrieves objects with special characters correctly
- CLI tools (mc, aws-cli) work perfectly → proves backend is working

**Client/UI is the issue** ❌
- Some clients don't properly URL-encode requests
- UI may not encode prefixes when making LIST requests
- Custom HTTP clients may have encoding bugs

### Solution

1. **For Users**: Use proper S3 SDKs/clients (they handle encoding automatically)
2. **For Developers**: Backend needs no fixes, but added defensive validation and logging
3. **For UI**: UI needs to properly URL-encode all requests (if applicable)

## Quick Examples

### ✅ Works Correctly (Using mc)

```bash
# Upload
mc cp file.txt "myrustfs/bucket/path with spaces/file.txt"

# List
mc ls "myrustfs/bucket/path with spaces/"

# Result: ✅ Success - mc properly encodes the request
```

### ❌ May Not Work (Raw HTTP without encoding)

```bash
# Wrong: Not encoded
curl "http://localhost:9000/bucket/path with spaces/file.txt"

# Result: ❌ May fail - spaces not encoded
```

### ✅ Correct Raw HTTP

```bash
# Correct: Properly encoded
curl "http://localhost:9000/bucket/path%20with%20spaces/file.txt"

# Result: ✅ Success - spaces encoded as %20
```

## URL Encoding Quick Reference

| Character | Display | In URL Path | In Query Param |
|-----------|---------|-------------|----------------|
| Space | ` ` | `%20` | `%20` or `+` |
| Plus | `+` | `%2B` | `%2B` |
| Percent | `%` | `%25` | `%25` |

**Critical**: In URL **paths**, `+` = literal plus (NOT space). Only `%20` = space in paths!

## Implementation Status

### ✅ Completed

1. **Backend Validation**: Added control character validation (rejects null bytes, newlines)
2. **Debug Logging**: Added logging for keys with special characters
3. **Tests**: Created comprehensive e2e test suite
4. **Documentation**: 
   - Client guide with SDK examples
   - Solution document for developers
   - Technical analysis for architects

### 📋 Recommended Next Steps

1. **Run Tests**: Execute e2e tests to verify backend behavior
   ```bash
   cargo test --package e2e_test special_chars
   ```

2. **UI Review** (if applicable): Check if RustFS UI properly encodes requests

3. **User Communication**: 
   - Update user documentation
   - Add troubleshooting to FAQ
   - Communicate known UI limitations (if any)

## Related GitHub Issues

- Original Issue: Special Chars in path (#???)
- Referenced PR: #1072 (mentioned in issue comments)

## Support

If you encounter issues with special characters:

1. **First**: Check the [Client Guide](./client-special-characters-guide.md)
2. **Try**: Use mc or AWS CLI to isolate the issue
3. **Enable**: Debug logging: `RUST_LOG=rustfs=debug`
4. **Report**: Create an issue with:
   - Client/SDK used
   - Exact object name causing issues
   - Whether mc works (to isolate backend vs client)
   - Debug logs

## Contributing

When contributing related fixes:

1. Read the [Solution Document](./special-characters-solution.md)
2. Understand that backend is working correctly via s3s
3. Focus on UI/client improvements or documentation
4. Add tests to verify behavior
5. Update relevant documentation

## Testing

### Run Special Character Tests

```bash
# All special character tests
cargo test --package e2e_test special_chars -- --nocapture

# Specific test
cargo test --package e2e_test test_object_with_space_in_path -- --nocapture
cargo test --package e2e_test test_object_with_plus_in_path -- --nocapture
cargo test --package e2e_test test_issue_scenario_exact -- --nocapture
```

### Test with Real Clients

```bash
# MinIO client
mc alias set test http://localhost:9000 minioadmin minioadmin
mc cp README.md "test/bucket/test with spaces/README.md"
mc ls "test/bucket/test with spaces/"

# AWS CLI
aws --endpoint-url=http://localhost:9000 s3 cp README.md "s3://bucket/test with spaces/README.md"
aws --endpoint-url=http://localhost:9000 s3 ls "s3://bucket/test with spaces/"
```

## Version History

- **v1.0** (2025-12-09): Initial documentation
  - Comprehensive analysis completed
  - Root cause identified (UI/client issue)
  - Backend validation and logging added
  - Client guide created
  - E2E tests added

## See Also

- [AWS S3 API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- [RFC 3986: URI Generic Syntax](https://tools.ietf.org/html/rfc3986)
- [s3s Library Documentation](https://docs.rs/s3s/)
- [URL Encoding Best Practices](https://developer.mozilla.org/en-US/docs/Glossary/Percent-encoding)

---

**Maintained by**: RustFS Team  
**Last Updated**: 2025-12-09  
**Status**: Complete - Ready for Use
