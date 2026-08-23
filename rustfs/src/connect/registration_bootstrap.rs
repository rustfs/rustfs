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

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::{ClientError, ConnectClient, ConnectConfig, CredentialStore, IdentityStore, RegistrationToken, TokenError};

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
    #[error("the Connect root CA file must be a regular file")]
    RootCaFileSecurity,
    #[error("the Connect state path must be an explicit directory, not a symlink")]
    StateDirectorySecurity,
    #[error("failed to read protected Connect registration input")]
    Input(#[source] io::Error),
    #[error("Connect returned a device outside the registration token's cluster scope")]
    CredentialScope,
    #[error(transparent)]
    Token(#[from] TokenError),
    #[error(transparent)]
    Client(#[from] ClientError),
}

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
    })?;

    let token = match token_file {
        Some(path) => RegistrationToken::from_reader(open_regular_file(path, true)?),
        None => RegistrationToken::from_reader(io::stdin().lock()),
    }?;
    prepare_state_directory(state_directory)?;
    let cluster_name = format!("organizations/{}/clusters/{}", token.organization_uid, token.cluster_uid);
    let credential = client
        .register(
            &IdentityStore::new(state_directory.join("identity")),
            &CredentialStore::new(state_directory.join("credential")),
            &token,
        )
        .await?;
    if credential.name != format!("{cluster_name}/clusterDevices/{}", credential.uid) {
        return Err(RegistrationBootstrapError::CredentialScope);
    }

    Ok(RegistrationBootstrapResult {
        device_uid: credential.uid,
        cluster_name,
    })
}

fn prepare_state_directory(path: &Path) -> Result<(), RegistrationBootstrapError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(RegistrationBootstrapError::StateDirectorySecurity)
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                builder.mode(0o700);
            }
            builder.create(path).map_err(RegistrationBootstrapError::Input)
        }
        Err(error) => Err(RegistrationBootstrapError::Input(error)),
    }
}

fn read_regular_file(path: &Path, owner_only: bool) -> Result<Vec<u8>, RegistrationBootstrapError> {
    let mut file = open_regular_file(path, owner_only)?;
    let mut contents = Vec::new();
    io::Read::read_to_end(&mut file, &mut contents).map_err(RegistrationBootstrapError::Input)?;
    Ok(contents)
}

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
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(RegistrationBootstrapError::Input)?;
    let metadata = file.metadata().map_err(RegistrationBootstrapError::Input)?;
    if !metadata.is_file() {
        return Err(insecure());
    }
    #[cfg(unix)]
    if owner_only {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o400 == 0 || mode & 0o177 != 0 {
            return Err(RegistrationBootstrapError::TokenFileSecurity);
        }
    }
    Ok(file)
}
