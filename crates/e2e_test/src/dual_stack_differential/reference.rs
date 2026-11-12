// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Which binary side A of the differential runs, and what it reports about itself.
//!
//! `RUSTFS_E2E_REFERENCE_BINARY` names a released `rustfs` binary to use as the reference. Unset,
//! side A is the binary under test on `RUSTFS_S3_STACK=legacy`. Set, it must resolve to an
//! executable file or the run fails: a reference that was asked for and silently replaced by the
//! binary under test would report an agreement with itself as agreement with the release.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(super) const ENV_REFERENCE_BINARY: &str = "RUSTFS_E2E_REFERENCE_BINARY";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SideA {
    /// Both sides are the binary under test; A runs `RUSTFS_S3_STACK=legacy`.
    SameBinary,
    /// A is this released binary, started without `RUSTFS_S3_STACK`.
    Reference(PathBuf),
}

impl SideA {
    pub(super) fn from_env() -> Result<Self, BoxError> {
        Self::resolve(std::env::var_os(ENV_REFERENCE_BINARY))
    }

    /// `None` is the only value that selects same-binary mode; every other value must validate.
    pub(super) fn resolve(value: Option<OsString>) -> Result<Self, BoxError> {
        let Some(value) = value else {
            return Ok(Self::SameBinary);
        };
        let path = PathBuf::from(value);
        validate_executable(&path)?;
        Ok(Self::Reference(path))
    }
}

fn validate_executable(path: &Path) -> Result<(), BoxError> {
    if path.as_os_str().is_empty() {
        return Err(format!("{ENV_REFERENCE_BINARY} is set but empty").into());
    }
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("{ENV_REFERENCE_BINARY} points to {}, which cannot be read: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{ENV_REFERENCE_BINARY} points to {}, which is not a file", path.display()).into());
    }
    if !is_executable(&metadata) {
        return Err(format!("{ENV_REFERENCE_BINARY} points to {}, which is not executable", path.display()).into());
    }
    Ok(())
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

/// What `<binary> --version` printed, trimmed. Fails when the binary does not run, exits
/// non-zero, or prints nothing, because an empty version would report nothing as observed.
pub(super) async fn binary_version(binary: &Path) -> Result<String, BoxError> {
    let output = tokio::process::Command::new(binary).arg("--version").output().await?;
    if !output.status.success() {
        return Err(format!("{} --version failed with {}", binary.display(), output.status).into());
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if version.is_empty() {
        return Err(format!("{} --version printed nothing", binary.display()).into());
    }
    Ok(version)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn script(dir: &Path, name: &str, body: &str, mode: u32) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).expect("write the stand-in");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        path
    }

    #[test]
    fn missing_path_is_an_error_not_a_fallback() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = SideA::resolve(Some(dir.path().join("absent").into_os_string())).expect_err("must fail closed");
        assert!(error.to_string().contains("cannot be read"), "{error}");
    }

    #[test]
    fn non_executable_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = script(dir.path(), "rustfs", "#!/bin/sh\necho v\n", 0o644);
        let error = SideA::resolve(Some(path.into_os_string())).expect_err("must fail closed");
        assert!(error.to_string().contains("not executable"), "{error}");
    }

    #[test]
    fn directory_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = SideA::resolve(Some(dir.path().as_os_str().to_owned())).expect_err("must fail closed");
        assert!(error.to_string().contains("not a file"), "{error}");
    }

    #[test]
    fn empty_value_is_an_error() {
        let error = SideA::resolve(Some(OsString::new())).expect_err("must fail closed");
        assert!(error.to_string().contains("empty"), "{error}");
    }

    #[test]
    fn unset_selects_same_binary_mode() {
        assert_eq!(SideA::resolve(None).expect("unset is valid"), SideA::SameBinary);
    }

    #[test]
    fn executable_file_selects_reference_mode() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = script(dir.path(), "rustfs", "#!/bin/sh\necho v\n", 0o755);
        assert_eq!(
            SideA::resolve(Some(path.clone().into_os_string())).expect("valid"),
            SideA::Reference(path)
        );
    }

    #[tokio::test]
    async fn version_output_is_captured_from_the_binary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = script(dir.path(), "rustfs", "#!/bin/sh\necho 'rustfs 1.0.4 (abc123)'\n", 0o755);
        assert_eq!(binary_version(&path).await.expect("version"), "rustfs 1.0.4 (abc123)");
    }

    #[tokio::test]
    async fn failing_or_silent_version_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let failing = script(dir.path(), "failing", "#!/bin/sh\nexit 3\n", 0o755);
        assert!(binary_version(&failing).await.is_err());
        let silent = script(dir.path(), "silent", "#!/bin/sh\nexit 0\n", 0o755);
        assert!(binary_version(&silent).await.is_err());
    }
}
