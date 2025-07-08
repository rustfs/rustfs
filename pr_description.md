## Summary

This PR modifies the GitHub Actions workflows to ensure that **version releases never get skipped** during CI/CD execution, addressing the issue where duplicate action detection could skip important release processes.

## Changes Made

### 🔧 Core Modifications

1. **Modified skip-duplicate-actions configuration**:
   - Added `skip_after_successful_duplicate: ${{ !startsWith(github.ref, 'refs/tags/') }}` parameter
   - This ensures tag pushes (version releases) are never skipped due to duplicate detection

2. **Updated workflow job conditions**:
   - **CI Workflow** (`ci.yml`): Modified `test-and-lint` and `e2e-tests` jobs
   - **Build Workflow** (`build.yml`): Modified `build-check`, `build-rustfs`, `build-gui`, `release`, and `upload-oss` jobs
   - All jobs now use condition: `startsWith(github.ref, 'refs/tags/') || needs.skip-check.outputs.should_skip != 'true'`

### 🎯 Problem Solved

- **Before**: Version releases could be skipped if there were concurrent workflows or duplicate actions
- **After**: Tag pushes always trigger complete CI/CD pipeline execution, ensuring:
  - ✅ Full test suite execution
  - ✅ Code quality checks (fmt, clippy)
  - ✅ Multi-platform builds (Linux, macOS, Windows)
  - ✅ GUI builds for releases
  - ✅ Release asset creation
  - ✅ OSS uploads

### 🚀 Benefits

1. **Release Quality Assurance**: Every version release undergoes complete validation
2. **Consistency**: No more uncertainty about whether release builds were properly tested
3. **Multi-platform Support**: Ensures all target platforms are built for every release
4. **Backward Compatibility**: Non-release workflows still benefit from duplicate skip optimization

## Testing

- [x] Workflow syntax validated
- [x] Logic conditions verified for both tag and non-tag scenarios
- [x] Maintains existing optimization for development builds
- [x] Follows project coding standards and commit conventions

## Related Issues

This resolves the concern about workflow skipping during version releases, ensuring complete CI/CD execution for all published versions.

## Checklist

- [x] Code follows project formatting standards
- [x] Commit message follows Conventional Commits format
- [x] Changes are backwards compatible
- [x] No breaking changes introduced
- [x] All workflow conditions properly tested

---

**Note**: This change only affects the execution logic for tag pushes (version releases). Regular development workflows continue to benefit from duplicate action skipping for efficiency.
