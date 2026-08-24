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
    io::Write as _,
    path::{Component, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use super::TokenError;
#[cfg(unix)]
use super::{ConnectClient, ConnectConfig, CredentialStore, IdentityStore, RegistrationToken};

#[cfg(unix)]
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(unix)]
const BOOTSTRAP_READY_FILE: &str = ".bootstrap-ready";
#[cfg(unix)]
const BOOTSTRAP_READY_CONTENTS: &[u8] = b"v1\n";
#[cfg(unix)]
static BOOTSTRAP_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    #[error("the Connect state directory parent must be pre-provisioned as a secure durable directory")]
    StateParentRequired,
    #[error("the Connect bootstrap readiness marker must be an owner-only regular file")]
    StateMarkerSecurity,
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

    let state_directory = prepare_state_directory(state_directory)?;
    let token = match token_file {
        Some(path) => RegistrationToken::from_reader(open_regular_file(path, true)?),
        None => RegistrationToken::from_reader(io::stdin().lock()),
    }?;
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
    sync: impl FnMut(&Path) -> io::Result<()>,
) -> Result<PathBuf, RegistrationBootstrapError> {
    prepare_state_directory_with_sync_and_missing_observer(path, sync, |_| {})
}

#[cfg(unix)]
fn prepare_state_directory_with_sync_and_missing_observer(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
    mut observed_missing: impl FnMut(&Path),
) -> Result<PathBuf, RegistrationBootstrapError> {
    if path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(RegistrationBootstrapError::StateDirectorySecurity);
    }
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(RegistrationBootstrapError::Input)?.join(path)
    };

    let state_parent = path.parent().ok_or(RegistrationBootstrapError::StateParentRequired)?;
    match fs::symlink_metadata(state_parent) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(RegistrationBootstrapError::StateParentRequired);
        }
        Err(error) => return Err(RegistrationBootstrapError::Input(error)),
    }

    let mut directories = state_parent.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    directories.reverse();
    for directory in &directories {
        validate_directory(directory, false)?;
    }

    let state_exists = match fs::symlink_metadata(&path) {
        Ok(_) => {
            validate_directory(&path, true)?;
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(RegistrationBootstrapError::Input(error)),
    };
    let store_directories = [path.join("identity"), path.join("credential")];
    let ready = path.join(BOOTSTRAP_READY_FILE);
    if state_exists && ready_marker_exists(&ready)? {
        for directory in &store_directories {
            validate_directory(directory, true)?;
        }
        return Ok(path);
    }

    ensure_directory(&path, true, &mut observed_missing)?;
    for directory in &store_directories {
        ensure_directory(directory, true, &mut observed_missing)?;
    }
    for directory in &directories {
        validate_directory(directory, false)?;
    }
    validate_directory(&path, true)?;
    for directory in &store_directories {
        validate_directory(directory, true)?;
    }

    // The caller-provisioned parent is the durability anchor. Without a ready
    // marker, repeat this complete commit sequence after every interruption.
    for directory in [
        store_directories[0].clone(),
        store_directories[1].clone(),
        path.clone(),
        state_parent.to_path_buf(),
    ] {
        sync(&directory).map_err(RegistrationBootstrapError::Input)?;
    }
    publish_ready_marker(&path, &ready)?;
    Ok(path)
}

#[cfg(unix)]
fn ready_marker_exists(path: &Path) -> Result<bool, RegistrationBootstrapError> {
    let initial = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(RegistrationBootstrapError::Input(error)),
    };
    if initial.file_type().is_symlink() || !initial.is_file() {
        return Err(RegistrationBootstrapError::StateMarkerSecurity);
    }

    let mut options = OpenOptions::new();
    options.read(true);
    use std::os::unix::fs::OpenOptionsExt as _;
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let mut file = options.open(path).map_err(RegistrationBootstrapError::Input)?;
    let metadata = file.metadata().map_err(RegistrationBootstrapError::Input)?;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_file() || !unix_ready_marker_is_trusted(metadata.uid(), mode, process_uid()) {
        return Err(RegistrationBootstrapError::StateMarkerSecurity);
    }

    let mut contents = Vec::with_capacity(BOOTSTRAP_READY_CONTENTS.len() + 1);
    io::Read::read_to_end(&mut io::Read::take(&mut file, (BOOTSTRAP_READY_CONTENTS.len() + 1) as u64), &mut contents)
        .map_err(RegistrationBootstrapError::Input)?;
    if contents != BOOTSTRAP_READY_CONTENTS {
        return Err(RegistrationBootstrapError::StateMarkerSecurity);
    }
    Ok(true)
}

#[cfg(unix)]
fn publish_ready_marker(state_directory: &Path, ready: &Path) -> Result<(), RegistrationBootstrapError> {
    publish_ready_marker_with_existing_observer(state_directory, ready, File::sync_all, || {})
}

#[cfg(unix)]
fn publish_ready_marker_with_existing_observer(
    state_directory: &Path,
    ready: &Path,
    mut sync_staging: impl FnMut(&File) -> io::Result<()>,
    mut existing_observer: impl FnMut(),
) -> Result<(), RegistrationBootstrapError> {
    let (staging_path, mut staging) = loop {
        let staging_path = state_directory.join(format!(
            "{BOOTSTRAP_READY_FILE}.{}.{}.tmp",
            std::process::id(),
            BOOTSTRAP_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
        match options.open(&staging_path) {
            Ok(file) => break (staging_path, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(RegistrationBootstrapError::Input(error)),
        }
    };

    let staged = (|| -> io::Result<()> {
        staging.write_all(BOOTSTRAP_READY_CONTENTS)?;
        use std::os::unix::fs::PermissionsExt as _;
        staging.set_permissions(fs::Permissions::from_mode(0o600))?;
        sync_staging(&staging)
    })();
    drop(staging);
    if let Err(error) = staged {
        let _ = fs::remove_file(&staging_path);
        return Err(RegistrationBootstrapError::Input(error));
    }

    // Hard-link publication is atomic and, unlike rename, cannot replace a
    // marker planted between validation and publication.
    let published = fs::hard_link(&staging_path, ready);
    if let Err(error) = fs::remove_file(&staging_path) {
        if published.is_ok() {
            let _ = fs::remove_file(ready);
        }
        return Err(RegistrationBootstrapError::Input(error));
    }
    match published {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            existing_observer();
            if ready_marker_exists(ready)? {
                Ok(())
            } else {
                Err(RegistrationBootstrapError::Input(io::Error::new(
                    io::ErrorKind::NotFound,
                    "bootstrap readiness marker disappeared during publication",
                )))
            }
        }
        Err(error) => Err(RegistrationBootstrapError::Input(error)),
    }
}

#[cfg(unix)]
fn ensure_directory(
    path: &Path,
    require_process_owner: bool,
    observed_missing: &mut impl FnMut(&Path),
) -> Result<(), RegistrationBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            validate_directory(path, require_process_owner)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            observed_missing(path);
            let mut builder = fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
            match builder.create(path) {
                Ok(()) => {
                    validate_directory(path, true)?;
                    Ok(())
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    validate_directory(path, require_process_owner)?;
                    Ok(())
                }
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
fn unix_ready_marker_is_trusted(owner_uid: u32, mode: u32, process_uid: u32) -> bool {
    owner_uid == process_uid && mode & 0o400 != 0 && mode & 0o177 == 0
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
    use std::cell::{Cell, RefCell};
    use std::fs::{self, File};
    use std::io;
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::path::Path;
    use std::sync::{Arc, Barrier};

    use super::{
        BOOTSTRAP_READY_CONTENTS, BOOTSTRAP_READY_FILE, RegistrationBootstrapError, prepare_state_directory_with_sync,
        prepare_state_directory_with_sync_and_missing_observer, publish_ready_marker_with_existing_observer, ready_marker_exists,
        sync_directory, unix_ca_file_is_trusted, unix_directory_is_trusted, unix_ready_marker_is_trusted,
    };

    fn secure_tempdir() -> tempfile::TempDir {
        let home = std::fs::canonicalize(std::env::var_os("HOME").expect("test requires a protected home directory"))
            .expect("test home directory must resolve without symlink components");
        tempfile::Builder::new()
            .prefix(".connect-registration-bootstrap-")
            .tempdir_in(home)
            .expect("temporary directory inside the protected home directory")
    }

    fn create_secure_state_tree(state: &Path) {
        for directory in [state.to_path_buf(), state.join("identity"), state.join("credential")] {
            fs::create_dir_all(&directory).expect("create state directory");
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).expect("secure state directory");
        }
    }

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
    fn unix_ready_marker_policy_requires_process_owner_and_owner_only_readability() {
        let process_uid = 501;

        assert!(unix_ready_marker_is_trusted(process_uid, 0o400, process_uid));
        assert!(unix_ready_marker_is_trusted(process_uid, 0o600, process_uid));
        assert!(!unix_ready_marker_is_trusted(process_uid + 1, 0o600, process_uid));
        assert!(!unix_ready_marker_is_trusted(process_uid, 0o200, process_uid));
        assert!(!unix_ready_marker_is_trusted(process_uid, 0o640, process_uid));
        assert!(!unix_ready_marker_is_trusted(process_uid, 0o602, process_uid));
    }

    #[test]
    fn new_state_syncs_managed_directories_and_preprovisioned_parent() {
        let temp = secure_tempdir();
        let state = temp.path().join("state");
        let observed = RefCell::new(Vec::new());

        let prepared = prepare_state_directory_with_sync(&state, |path| {
            assert!(path.is_dir(), "directory must exist before it is synced");
            observed.borrow_mut().push(path.to_path_buf());
            Ok(())
        })
        .expect("new state tree must become ready");

        assert_eq!(prepared, state);
        let observed = observed.into_inner();
        assert_eq!(
            observed,
            [
                state.join("identity"),
                state.join("credential"),
                state,
                temp.path().to_path_buf(),
            ],
            "new state trees must sync only managed directories and their durable parent"
        );
    }

    #[test]
    fn missing_state_parent_fails_before_creation_or_sync() {
        let temp = secure_tempdir();
        let missing_parent = temp.path().join("missing");
        let state = missing_parent.join("state");
        let calls = Cell::new(0);

        let error = prepare_state_directory_with_sync(&state, |_| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .expect_err("state parent must be provisioned before bootstrap");

        assert!(matches!(error, RegistrationBootstrapError::StateParentRequired));
        assert_eq!(calls.get(), 0);
        assert!(!missing_parent.exists());
        assert_eq!(
            error.to_string(),
            "the Connect state directory parent must be pre-provisioned as a secure durable directory"
        );
    }

    #[test]
    fn parent_components_fail_before_creation_or_sync_while_current_directory_is_allowed() {
        let temp = secure_tempdir();
        let absolute_parent = temp.path().join("secure/connect/..");

        for path in [absolute_parent.as_path(), Path::new("state/../other")] {
            let calls = Cell::new(0);
            let error = prepare_state_directory_with_sync(path, |_| {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .expect_err("parent components must fail before filesystem preparation");

            assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
            assert_eq!(calls.get(), 0);
        }
        assert!(!temp.path().join("secure").exists());

        let dotted_state = temp.path().join("./state");
        prepare_state_directory_with_sync(&dotted_state, |_| Ok(())).expect("current-directory component is safe");
        assert_eq!(
            fs::read(temp.path().join("state").join(BOOTSTRAP_READY_FILE)).expect("read ready marker"),
            BOOTSTRAP_READY_CONTENTS
        );
    }

    #[test]
    fn missing_marker_retries_all_managed_syncs_after_final_parent_failure() {
        let temp = secure_tempdir();
        let state = temp.path().join("state");
        create_secure_state_tree(&state);
        let ready = state.join(BOOTSTRAP_READY_FILE);
        let first_attempt = RefCell::new(Vec::new());

        let error = prepare_state_directory_with_sync(&state, |path| {
            first_attempt.borrow_mut().push(path.to_path_buf());
            if path == temp.path() {
                return Err(io::Error::other("injected final parent sync failure"));
            }
            Ok(())
        })
        .expect_err("final parent sync failure must stop preparation");

        assert!(matches!(error, RegistrationBootstrapError::Input(_)));
        assert_eq!(
            first_attempt.into_inner(),
            vec![
                state.join("identity"),
                state.join("credential"),
                state.clone(),
                temp.path().to_path_buf()
            ]
        );
        assert!(!ready.exists(), "failed durability preparation must not publish ready");

        let retry = RefCell::new(Vec::new());
        let prepared = prepare_state_directory_with_sync(&state, |path| {
            retry.borrow_mut().push(path.to_path_buf());
            Ok(())
        })
        .expect("retry must repeat durability preparation");

        assert_eq!(prepared, state);
        assert_eq!(
            retry.into_inner(),
            [
                state.join("identity"),
                state.join("credential"),
                state.clone(),
                temp.path().to_path_buf(),
            ],
            "an interrupted complete tree must resync only its managed path and direct parent"
        );
        assert_eq!(fs::read(&ready).expect("read ready marker"), BOOTSTRAP_READY_CONTENTS);

        let calls = Cell::new(0);
        let prepared = prepare_state_directory_with_sync(&state, |_| {
            calls.set(calls.get() + 1);
            Err(io::Error::other("ready trees must not sync"))
        })
        .expect("safe ready tree should bypass sync");
        assert_eq!(prepared, state);
        assert_eq!(calls.get(), 0);
    }

    #[test]
    fn every_pre_marker_sync_failure_retries_the_complete_commit() {
        let temp = secure_tempdir();

        for fail_at in 1..=4 {
            let state = temp.path().join(format!("state-{fail_at}"));
            create_secure_state_tree(&state);
            let expected = [
                state.join("identity"),
                state.join("credential"),
                state.clone(),
                temp.path().to_path_buf(),
            ];
            let calls = Cell::new(0);
            let error = prepare_state_directory_with_sync(&state, |path| {
                assert_eq!(path, &expected[calls.get()]);
                calls.set(calls.get() + 1);
                if calls.get() == fail_at {
                    return Err(io::Error::other("injected pre-marker sync failure"));
                }
                Ok(())
            })
            .expect_err("every managed durability failure must stop preparation");

            assert!(matches!(error, RegistrationBootstrapError::Input(_)));
            assert_eq!(calls.get(), fail_at);
            assert!(!state.join(BOOTSTRAP_READY_FILE).exists());

            let retry = RefCell::new(Vec::new());
            prepare_state_directory_with_sync(&state, |path| {
                retry.borrow_mut().push(path.to_path_buf());
                Ok(())
            })
            .expect("retry must repeat the complete durability commit");
            assert_eq!(retry.into_inner(), expected);
            assert!(ready_marker_exists(&state.join(BOOTSTRAP_READY_FILE)).expect("validate ready marker"));
        }
    }

    #[test]
    fn missing_directory_replacement_is_revalidated_after_create_race() {
        let temp = secure_tempdir();

        let target = temp.path().join("target");
        fs::create_dir(&target).expect("create symlink target");
        let linked_state = temp.path().join("linked-state");
        let error = prepare_state_directory_with_sync_and_missing_observer(
            &linked_state,
            |_| panic!("a raced symlink must fail before syncing"),
            |missing| {
                if missing == linked_state {
                    symlink(&target, missing).expect("replace missing state with symlink");
                }
            },
        )
        .expect_err("raced symlink must fail closed");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
        assert!(!target.join("identity").exists());
        assert!(!target.join("credential").exists());
        assert!(!target.join(BOOTSTRAP_READY_FILE).exists());

        let shared_state = temp.path().join("shared-state-race");
        let error = prepare_state_directory_with_sync_and_missing_observer(
            &shared_state,
            |_| panic!("a raced shared directory must fail before syncing"),
            |missing| {
                if missing == shared_state {
                    fs::create_dir(missing).expect("replace missing state with directory");
                    fs::set_permissions(missing, fs::Permissions::from_mode(0o770)).expect("share raced directory");
                }
            },
        )
        .expect_err("raced shared directory must fail closed");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
        assert!(!shared_state.join("identity").exists());
        assert!(!shared_state.join("credential").exists());
        assert!(!shared_state.join(BOOTSTRAP_READY_FILE).exists());
    }

    #[test]
    fn ready_marker_rejects_symlinks_shared_modes_non_files_and_invalid_contents() {
        let temp = secure_tempdir();

        for case in ["symlink", "shared", "directory", "contents"] {
            let state = temp.path().join(case);
            create_secure_state_tree(&state);
            let ready = state.join(BOOTSTRAP_READY_FILE);
            match case {
                "symlink" => {
                    let target = temp.path().join("marker-target");
                    fs::write(&target, BOOTSTRAP_READY_CONTENTS).expect("write marker target");
                    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).expect("secure marker target");
                    symlink(target, &ready).expect("create marker symlink");
                }
                "shared" => {
                    fs::write(&ready, BOOTSTRAP_READY_CONTENTS).expect("write shared marker");
                    fs::set_permissions(&ready, fs::Permissions::from_mode(0o640)).expect("share marker");
                }
                "directory" => fs::create_dir(&ready).expect("create marker directory"),
                "contents" => {
                    fs::write(&ready, b"not-ready\n").expect("write invalid marker");
                    fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).expect("secure invalid marker");
                }
                _ => unreachable!(),
            }

            let error = prepare_state_directory_with_sync(&state, |_| panic!("an unsafe marker must fail before syncing"))
                .expect_err("unsafe marker must fail closed");
            assert!(matches!(error, RegistrationBootstrapError::StateMarkerSecurity));
        }
    }

    #[test]
    fn marker_removed_after_publication_conflict_fails_closed() {
        let temp = secure_tempdir();
        let state = temp.path().join("state");
        create_secure_state_tree(&state);
        let ready = state.join(BOOTSTRAP_READY_FILE);
        fs::write(&ready, BOOTSTRAP_READY_CONTENTS).expect("write existing ready marker");
        fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).expect("secure existing ready marker");

        let error = publish_ready_marker_with_existing_observer(&state, &ready, File::sync_all, || {
            fs::remove_file(&ready).expect("remove marker after no-replace conflict");
        })
        .expect_err("a marker removed before validation must fail closed");

        assert!(matches!(
            error,
            RegistrationBootstrapError::Input(ref source) if source.kind() == io::ErrorKind::NotFound
        ));
        assert!(!ready.exists());
        assert!(
            fs::read_dir(&state)
                .expect("read state directory")
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
            "failed publication must not leave staging files"
        );
    }

    #[test]
    fn staging_sync_failure_does_not_publish_ready_marker_and_can_retry() {
        let temp = secure_tempdir();
        let state = temp.path().join("state");
        create_secure_state_tree(&state);
        let ready = state.join(BOOTSTRAP_READY_FILE);

        let error = publish_ready_marker_with_existing_observer(
            &state,
            &ready,
            |_| Err(io::Error::other("injected staging sync failure")),
            || {},
        )
        .expect_err("staging sync failure must stop publication");

        assert!(matches!(error, RegistrationBootstrapError::Input(ref source) if source.kind() == io::ErrorKind::Other));
        assert!(!ready.exists(), "staging sync failure must not publish the marker");
        assert!(
            fs::read_dir(&state)
                .expect("read state directory")
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
            "staging sync failure must not leave staging files"
        );

        publish_ready_marker_with_existing_observer(&state, &ready, File::sync_all, || {})
            .expect("retry should publish a durable marker");
        assert!(ready_marker_exists(&ready).expect("validate retried ready marker"));
    }

    #[test]
    fn ready_marker_does_not_bypass_ancestor_validation() {
        let temp = secure_tempdir();
        let ancestor = temp.path().join("ancestor");
        let state = ancestor.join("state");
        create_secure_state_tree(&state);
        prepare_state_directory_with_sync(&state, |_| Ok(())).expect("publish ready marker");

        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o770)).expect("share state ancestor");
        let error = prepare_state_directory_with_sync(&state, |_| panic!("unsafe ancestor must fail before syncing"))
            .expect_err("shared ancestor must fail even with ready marker");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700)).expect("restore state ancestor");

        let linked_ancestor = temp.path().join("linked-ancestor");
        symlink(&ancestor, &linked_ancestor).expect("create ancestor symlink");
        let error = prepare_state_directory_with_sync(&linked_ancestor.join("state"), |_| {
            panic!("symlink ancestor must fail before syncing")
        })
        .expect_err("symlink ancestor must fail even with ready marker");
        assert!(matches!(error, RegistrationBootstrapError::StateDirectorySecurity));
    }

    #[test]
    fn concurrent_preparation_publishes_one_safe_ready_marker() {
        let temp = secure_tempdir();
        let state_parent = temp.path().join("preprovisioned");
        fs::create_dir(&state_parent).expect("preprovision state parent");
        fs::set_permissions(&state_parent, fs::Permissions::from_mode(0o700)).expect("secure state parent");
        let state = state_parent.join("state");
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let handles = (0..2)
                .map(|_| {
                    let barrier = barrier.clone();
                    let state = state.clone();
                    scope.spawn(move || {
                        let first_sync = Cell::new(true);
                        prepare_state_directory_with_sync(&state, |path| {
                            if first_sync.replace(false) {
                                barrier.wait();
                            }
                            sync_directory(path)
                        })
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                let prepared = handle
                    .join()
                    .expect("preparation thread")
                    .expect("concurrent preparation succeeds");
                assert_eq!(prepared, state);
            }
        });

        assert!(ready_marker_exists(&state.join(BOOTSTRAP_READY_FILE)).expect("validate ready marker"));
        let staging = fs::read_dir(&state)
            .expect("read state directory")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"));
        assert!(!staging, "concurrent publication must not leave staging files");
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
