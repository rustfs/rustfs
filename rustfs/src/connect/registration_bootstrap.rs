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

use std::io;
use std::path::Path;
#[cfg(unix)]
use std::{
    fs::{self, File, OpenOptions},
    path::PathBuf,
    time::Duration,
};

use super::TokenError;
#[cfg(unix)]
use super::{ConnectClient, ConnectConfig, CredentialStore, IdentityStore, RegistrationToken};

#[cfg(unix)]
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, PartialEq, Eq)]
pub struct RegistrationBootstrapResult {
    pub device_uid: String,
    pub cluster_name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistrationBootstrapError {
    #[error("the Connect registration token file must be an owner-readable, owner-only regular file")]
    TokenFileSecurity,
    #[error("the Connect root CA file must be a trusted, non-shared-writable regular file")]
    RootCaFileSecurity,
    #[error("the Connect state path must be an explicit directory, not a symlink")]
    StateDirectorySecurity,
    #[error("failed to read protected Connect registration input")]
    Input(#[source] io::Error),
    #[error("Connect registration configuration is invalid")]
    Configuration,
    #[error("Connect registration exchange failed")]
    Exchange,
    #[error("Connect registration bootstrap requires Unix owner and permission guarantees")]
    PlatformSecurity,
    #[error(transparent)]
    Token(#[from] TokenError),
}

#[cfg(not(unix))]
pub async fn register_from_protected_input(
    endpoint: &str,
    root_ca_file: &Path,
    state_directory: &Path,
    token_file: Option<&Path>,
) -> Result<RegistrationBootstrapResult, RegistrationBootstrapError> {
    let _ = (endpoint, root_ca_file, state_directory, token_file);
    Err(RegistrationBootstrapError::PlatformSecurity)
}

#[cfg(unix)]
pub async fn register_from_protected_input(
    endpoint: &str,
    root_ca_file: &Path,
    state_directory: &Path,
    token_file: Option<&Path>,
) -> Result<RegistrationBootstrapResult, RegistrationBootstrapError> {
    let root_ca_pem = read_regular_file(root_ca_file, false)?;
    let client = ConnectClient::new(ConnectConfig {
        endpoint,
        root_ca_pem: &root_ca_pem,
        timeout: REQUEST_TIMEOUT,
    })
    .map_err(|_| RegistrationBootstrapError::Configuration)?;

    let token = match token_file {
        Some(path) => RegistrationToken::from_reader(open_regular_file(path, true)?),
        None => RegistrationToken::from_reader(io::stdin().lock()),
    }?;
    let state_directory = prepare_state_directory(state_directory)?;
    let cluster_name = format!("organizations/{}/clusters/{}", token.organization_uid, token.cluster_uid);
    let credential = client
        .register(
            &IdentityStore::new(state_directory.join("identity")),
            &CredentialStore::new(state_directory.join("credential")),
            &token,
        )
        .await
        .map_err(|_| RegistrationBootstrapError::Exchange)?;
    if credential.name != format!("{cluster_name}/clusterDevices/{}", credential.uid) {
        return Err(RegistrationBootstrapError::Exchange);
    }

    Ok(RegistrationBootstrapResult {
        device_uid: credential.uid,
        cluster_name,
    })
}

#[cfg(unix)]
fn prepare_state_directory(path: &Path) -> Result<PathBuf, RegistrationBootstrapError> {
    prepare_state_directory_with_sync(path, sync_directory)
}

#[cfg(unix)]
fn prepare_state_directory_with_sync(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> Result<PathBuf, RegistrationBootstrapError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(RegistrationBootstrapError::Input)?.join(path)
    };

    let mut directories = path.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    directories.reverse();
    for directory in &directories {
        ensure_directory(directory, directory == &path)?;
    }
    let store_directories = [path.join("identity"), path.join("credential")];
    for directory in &store_directories {
        ensure_directory(directory, true)?;
    }
    for directory in &directories {
        validate_directory(directory, directory == &path)?;
    }
    for directory in &store_directories {
        validate_directory(directory, true)?;
    }
    // Repeat the complete leaf-to-root sync chain even for existing entries. A retry after a
    // previous sync failure must not reach registration while an ancestor is still non-durable.
    for directory in store_directories.iter().chain(directories.iter().rev()) {
        sync(directory).map_err(RegistrationBootstrapError::Input)?;
    }

    Ok(path)
}

#[cfg(unix)]
fn ensure_directory(path: &Path, require_process_owner: bool) -> Result<(), RegistrationBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_directory(path, require_process_owner),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => validate_directory(path, true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => validate_directory(path, require_process_owner),
                Err(error) => Err(RegistrationBootstrapError::Input(error)),
            }
        }
        Err(error) => Err(RegistrationBootstrapError::Input(error)),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(unix)]
fn validate_directory(path: &Path, require_process_owner: bool) -> Result<(), RegistrationBootstrapError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let metadata = fs::symlink_metadata(path).map_err(RegistrationBootstrapError::Input)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RegistrationBootstrapError::StateDirectorySecurity);
    }
    let mode = metadata.permissions().mode() & 0o777;
    if !unix_directory_is_trusted(metadata.uid(), mode, process_uid(), require_process_owner) {
        return Err(RegistrationBootstrapError::StateDirectorySecurity);
    }
    Ok(())
}

#[cfg(unix)]
fn unix_directory_is_trusted(owner_uid: u32, mode: u32, process_uid: u32, require_process_owner: bool) -> bool {
    (owner_uid == process_uid || (!require_process_owner && owner_uid == 0)) && mode & 0o022 == 0
}

#[cfg(unix)]
// SAFETY: geteuid has no pointer arguments or caller preconditions.
#[allow(unsafe_code)]
fn process_uid() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn read_regular_file(path: &Path, owner_only: bool) -> Result<Vec<u8>, RegistrationBootstrapError> {
    let mut file = open_regular_file(path, owner_only)?;
    let mut contents = Vec::new();
    io::Read::read_to_end(&mut file, &mut contents).map_err(RegistrationBootstrapError::Input)?;
    Ok(contents)
}

#[cfg(unix)]
fn open_regular_file(path: &Path, owner_only: bool) -> Result<File, RegistrationBootstrapError> {
    let insecure = || {
        if owner_only {
            RegistrationBootstrapError::TokenFileSecurity
        } else {
            RegistrationBootstrapError::RootCaFileSecurity
        }
    };
    let initial = fs::symlink_metadata(path).map_err(RegistrationBootstrapError::Input)?;
    if initial.file_type().is_symlink() || !initial.is_file() {
        return Err(insecure());
    }

    let mut options = OpenOptions::new();
    options.read(true);
    use std::os::unix::fs::OpenOptionsExt as _;
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path).map_err(RegistrationBootstrapError::Input)?;
    let metadata = file.metadata().map_err(RegistrationBootstrapError::Input)?;
    if !metadata.is_file() {
        return Err(insecure());
    }
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let mode = metadata.permissions().mode() & 0o777;
    if owner_only {
        if metadata.uid() != process_uid() || mode & 0o400 == 0 || mode & 0o177 != 0 {
            return Err(RegistrationBootstrapError::TokenFileSecurity);
        }
    } else if !unix_ca_file_is_trusted(metadata.uid(), mode, process_uid()) {
        return Err(RegistrationBootstrapError::RootCaFileSecurity);
    }
    Ok(file)
}

#[cfg(unix)]
fn unix_ca_file_is_trusted(owner_uid: u32, mode: u32, process_uid: u32) -> bool {
    (owner_uid == process_uid || owner_uid == 0) && mode & 0o022 == 0
}

#[cfg(all(test, unix))]
mod tests {
    use std::cell::RefCell;
    use std::io;

    use super::{
        RegistrationBootstrapError, prepare_state_directory_with_sync, unix_ca_file_is_trusted, unix_directory_is_trusted,
    };

    #[test]
    fn unix_directory_policy_rejects_wrong_owners_and_writable_modes_only() {
        let process_uid = 501;

        assert!(unix_directory_is_trusted(process_uid, 0o700, process_uid, true));
        assert!(unix_directory_is_trusted(process_uid, 0o755, process_uid, true));
        assert!(unix_directory_is_trusted(0, 0o755, process_uid, false));
        assert!(!unix_directory_is_trusted(0, 0o755, process_uid, true));
        assert!(!unix_directory_is_trusted(process_uid + 1, 0o700, process_uid, false));
        assert!(!unix_directory_is_trusted(process_uid, 0o720, process_uid, true));
        assert!(!unix_directory_is_trusted(process_uid, 0o702, process_uid, true));
    }

    #[test]
    fn unix_ca_policy_accepts_only_process_or_root_owned_non_writable_files() {
        let process_uid = 501;

        assert!(unix_ca_file_is_trusted(process_uid, 0o600, process_uid));
        assert!(unix_ca_file_is_trusted(0, 0o644, process_uid));
        assert!(!unix_ca_file_is_trusted(process_uid + 1, 0o600, process_uid));
        assert!(!unix_ca_file_is_trusted(process_uid, 0o620, process_uid));
        assert!(!unix_ca_file_is_trusted(0, 0o646, process_uid));
    }

    #[test]
    fn state_directories_are_synced_leaf_first_and_sync_failures_propagate() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let state = temp.path().join("state");
        let observed = RefCell::new(Vec::new());

        let error = prepare_state_directory_with_sync(&state, |path| {
            observed.borrow_mut().push(path.to_path_buf());
            if path == temp.path() {
                return Err(io::Error::other("injected parent sync failure"));
            }
            Ok(())
        })
        .expect_err("parent sync failure must stop bootstrap preparation");

        assert!(matches!(error, RegistrationBootstrapError::Input(_)));
        assert_eq!(
            observed.into_inner(),
            vec![
                state.join("identity"),
                state.join("credential"),
                state,
                temp.path().to_path_buf(),
            ]
        );
    }
}

#[cfg(all(test, not(unix)))]
mod non_unix_tests {
    use std::path::Path;

    use super::{RegistrationBootstrapError, register_from_protected_input};

    #[tokio::test]
    async fn bootstrap_fails_closed_for_stdin_and_token_files_without_unix_guarantees() {
        let root = Path::new("unreadable-root-ca");
        let state = Path::new("registration-bootstrap-must-not-create-state");
        let token = Path::new("unreadable-token");
        assert!(!state.exists());

        for token_file in [None, Some(token)] {
            let error = register_from_protected_input("https://connect.invalid/agent/", root, state, token_file)
                .await
                .expect_err("non-Unix bootstrap must fail closed");
            assert!(matches!(error, RegistrationBootstrapError::PlatformSecurity));
        }
        assert!(!state.exists());
    }
}
