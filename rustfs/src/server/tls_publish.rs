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

//! Durable server-leaf publication. Trust and client identity remain owned by their original source.

use super::tls_material;
use rustfs_config::{DEFAULT_TLS_RELOAD_ENABLE, ENV_TLS_RELOAD_ENABLE, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustfs_tls_runtime::{ServerTlsMaterial, TlsMaterialSnapshot, TlsSource};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};
use uuid::Uuid;

pub(crate) const ENV_TLS_PUBLISH_DIR: &str = "RUSTFS_TLS_PUBLISH_DIR";
pub(crate) const SCHEMA: &str = "rustfs.tls-publish/v1alpha1";
pub(crate) const RESOURCE: &str = "server-default";
const MAX_CERT: usize = 64 * 1024;
const MAX_KEY: usize = 32 * 1024;
const MANIFEST: &str = "manifest.json";
const CURRENT: &str = "server-current";
const INITIALIZED: &str = "initialized";
const INITIALIZED_CONTENT: &[u8] = b"rustfs.tls-publish/v1alpha1\n";

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct OperationReads {
    certificates: usize,
    keys: usize,
    manifests: usize,
    manifest_decodes: usize,
    certificate_pem_traversals: usize,
    leaf_der_decodes: usize,
}

#[cfg(test)]
thread_local! {
    static OPERATION_READS: std::cell::Cell<OperationReads> = const { std::cell::Cell::new(OperationReads {
        certificates: 0, keys: 0, manifests: 0, manifest_decodes: 0,
        certificate_pem_traversals: 0, leaf_der_decodes: 0,
    }) };
}

#[cfg(test)]
fn count_operation(action: impl FnOnce(&mut OperationReads)) {
    OPERATION_READS.with(|counts| {
        let mut value = counts.get();
        action(&mut value);
        counts.set(value);
    });
}

fn certificate_pem_items(
    bytes: &[u8],
) -> impl Iterator<Item = Result<CertificateDer<'static>, rustls::pki_types::pem::Error>> + '_ {
    #[cfg(test)]
    count_operation(|counts| counts.certificate_pem_traversals += 1);
    CertificateDer::pem_slice_iter(bytes)
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum FailurePoint {
    None,
    AfterPointerRename,
    AfterPointerRenameUnreadable,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublishRequest {
    pub schema_version: String,
    pub resource_id: String,
    pub expected_target_revision: String,
    pub issuance_id: Uuid,
    pub certificate_pem: String,
    pub private_key_pem: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: String,
    issuance_id: Uuid,
    target_revision: Uuid,
    certificate_fingerprint: String,
    not_after_unix: i64,
    payload_digest: String,
}

// A disk operation pins one immutable generation and verifies its envelope once.
// This value is never retained across requests or reload ticks.
struct PublishedGeneration {
    manifest: Manifest,
    chain: Vec<CertificateDer<'static>>,
    leaf: LeafMetadata,
    key_pem: Vec<u8>,
}

struct LeafMetadata {
    fingerprint: String,
    not_after_unix: i64,
    names: std::collections::BTreeSet<String>,
    is_ca: bool,
}

#[derive(Serialize)]
pub(crate) struct CertificateState {
    pub schema_version: &'static str,
    pub resource_id: &'static str,
    pub target_revision: String,
    pub certificate_fingerprint: String,
    pub not_after_unix: i64,
    pub last_issuance_id: Option<Uuid>,
    pub observed_certificate_fingerprint: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct PublishReceipt {
    pub schema_version: &'static str,
    pub resource_id: &'static str,
    pub issuance_id: Uuid,
    pub target_revision: String,
    pub certificate_fingerprint: String,
    pub not_after_unix: i64,
    pub observed_certificate_fingerprint: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishError {
    #[error("certificate publication is disabled or not configured safely")]
    Disabled,
    #[error("invalid certificate publication request or source")]
    Invalid,
    #[error("certificate publication revision conflict")]
    Conflict,
    #[error("certificate publication is busy; retry the identical issuance")]
    Busy,
    #[error("certificate publication storage failed; retry the identical issuance")]
    Storage,
}

impl From<std::io::Error> for PublishError {
    fn from(_: std::io::Error) -> Self {
        Self::Storage
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn payload_digest(certificate: &[u8], key: &[u8]) -> String {
    let mut digest = Sha256::new();
    // Both inputs have bounded sizes; the fixed-width encoding survives architecture changes.
    digest.update((certificate.len() as u128).to_be_bytes());
    digest.update(certificate);
    digest.update((key.len() as u128).to_be_bytes());
    digest.update(key);
    digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_leaf(leaf: &[u8]) -> Result<LeafMetadata, PublishError> {
    #[cfg(test)]
    count_operation(|counts| counts.leaf_der_decodes += 1);
    let (remaining, parsed) = x509_parser::parse_x509_certificate(leaf).map_err(|_| PublishError::Invalid)?;
    if !remaining.is_empty() {
        return Err(PublishError::Invalid);
    }
    let mut names = std::collections::BTreeSet::new();
    if let Some(san) = parsed.subject_alternative_name().map_err(|_| PublishError::Invalid)? {
        for name in &san.value.general_names {
            match name {
                x509_parser::extensions::GeneralName::DNSName(name) => {
                    names.insert(format!("dns:{}", name.to_ascii_lowercase()));
                }
                x509_parser::extensions::GeneralName::IPAddress(bytes) => {
                    names.insert(format!("ip:{bytes:?}"));
                }
                _ => {}
            }
        }
    }
    Ok(LeafMetadata {
        fingerprint: hex_digest(leaf),
        not_after_unix: parsed.validity().not_after.timestamp(),
        names,
        is_ca: parsed
            .basic_constraints()
            .map_err(|_| PublishError::Invalid)?
            .is_some_and(|constraints| constraints.value.ca),
    })
}

fn snapshot_leaf(snapshot: &TlsMaterialSnapshot) -> Option<String> {
    match &snapshot.server {
        Some(ServerTlsMaterial::SingleCert { certs, .. }) => certs.first().map(|cert| hex_digest(cert.as_ref())),
        _ => None,
    }
}

fn observed(tls_path: &str, expected_chain: &[CertificateDer<'_>]) -> Option<String> {
    let snapshot = tls_material::observed_server_snapshot(tls_path)?;
    match &snapshot.server {
        Some(ServerTlsMaterial::SingleCert { certs, .. }) if certs == expected_chain => snapshot_leaf(&snapshot),
        _ => None,
    }
}

fn open_regular(path: &Path, writable: bool, create: bool) -> Result<File, PublishError> {
    let mut options = OpenOptions::new();
    options.read(true).write(writable).create(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(PublishError::Invalid);
    }
    Ok(file)
}

fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>, PublishError> {
    let file = open_regular(path, false, false)?;
    #[cfg(test)]
    count_operation(|counts| match path.file_name().and_then(|name| name.to_str()) {
        Some(RUSTFS_TLS_CERT) => counts.certificates += 1,
        Some(RUSTFS_TLS_KEY) => counts.keys += 1,
        Some(MANIFEST) => counts.manifests += 1,
        _ => {}
    });
    let mut bytes = Vec::new();
    file.take(u64::try_from(limit).map_err(|_| PublishError::Invalid)? + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(PublishError::Invalid);
    }
    Ok(bytes)
}

fn validate_root(root: &Path) -> Result<(), PublishError> {
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(PublishError::Disabled);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 || metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(PublishError::Disabled);
        }
    }
    #[cfg(not(unix))]
    {
        Err(PublishError::Disabled)
    }
    #[cfg(unix)]
    {
        Ok(())
    }
}

fn configured_root() -> Result<Option<PathBuf>, PublishError> {
    let root = rustfs_utils::get_env_opt_str(ENV_TLS_PUBLISH_DIR).map(PathBuf::from);
    if let Some(root) = &root {
        validate_root(root)?;
    }
    Ok(root)
}

fn single_default_source(tls_path: &str) -> Result<(), PublishError> {
    if fs::symlink_metadata(Path::new(tls_path).join("..data")).is_ok() {
        return Err(PublishError::Disabled);
    }
    for entry in fs::read_dir(tls_path)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with('.') && entry.path().is_dir() {
            return Err(PublishError::Invalid);
        }
    }
    Ok(())
}

fn current_generation(root: &Path) -> Result<Option<PublishedGeneration>, PublishError> {
    let initialized = match fs::symlink_metadata(root.join(INITIALIZED)) {
        Ok(_) => {
            if read_limited(&root.join(INITIALIZED), 128)? != INITIALIZED_CONTENT {
                return Err(PublishError::Invalid);
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let pointer = root.join(CURRENT);
    match fs::symlink_metadata(&pointer) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return if initialized { Err(PublishError::Invalid) } else { Ok(None) };
        }
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.file_type().is_symlink() => return Err(PublishError::Invalid),
        _ => {}
    }
    let target = fs::read_link(pointer)?;
    let mut components = target.components();
    if components.next() != Some(Component::Normal("generations".as_ref())) {
        return Err(PublishError::Invalid);
    }
    let Some(Component::Normal(version)) = components.next() else {
        return Err(PublishError::Invalid);
    };
    let id = version
        .to_str()
        .and_then(|text| Uuid::parse_str(text).ok())
        .ok_or(PublishError::Invalid)?;
    if components.next().is_some() || version != id.to_string().as_str() {
        return Err(PublishError::Invalid);
    }
    let generations = root.join("generations");
    if fs::symlink_metadata(&generations)?.file_type().is_symlink() {
        return Err(PublishError::Invalid);
    }
    let directory = root.join(target);
    if !fs::symlink_metadata(&directory)?.is_dir() {
        return Err(PublishError::Invalid);
    }
    PublishedGeneration::load(directory, id).map(Some)
}

fn validate_source_separation(root: &Path, tls_path: &str) -> Result<(), PublishError> {
    let root = fs::canonicalize(root)?;
    let source = fs::canonicalize(tls_path)?;
    if root.starts_with(&source) || source.starts_with(&root) {
        return Err(PublishError::Disabled);
    }
    Ok(())
}

impl PublishedGeneration {
    fn into_snapshot(self, source: &rustfs_tls_runtime::TlsSourceSnapshot) -> Result<TlsMaterialSnapshot, PublishError> {
        TlsMaterialSnapshot::load_from_snapshot_with_server_identity(source, self.chain, &self.key_pem)
            .map_err(|_| PublishError::Invalid)
    }

    fn load(directory: PathBuf, revision: Uuid) -> Result<Self, PublishError> {
        let manifest_bytes = read_limited(&directory.join(MANIFEST), 4096)?;
        #[cfg(test)]
        count_operation(|counts| counts.manifest_decodes += 1);
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|_| PublishError::Invalid)?;
        let certificate = read_limited(&directory.join(RUSTFS_TLS_CERT), MAX_CERT)?;
        let key = read_limited(&directory.join(RUSTFS_TLS_KEY), MAX_KEY)?;
        let chain = certificate_pem_items(&certificate)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PublishError::Invalid)?;
        let leaf = parse_leaf(chain.first().ok_or(PublishError::Invalid)?.as_ref())?;
        if manifest.schema_version != SCHEMA
            || manifest.issuance_id.is_nil()
            || manifest.target_revision.is_nil()
            || manifest.target_revision != revision
            || manifest.certificate_fingerprint != leaf.fingerprint
            || manifest.not_after_unix != leaf.not_after_unix
            || manifest.payload_digest != payload_digest(&certificate, &key)
        {
            return Err(PublishError::Invalid);
        }
        Ok(Self {
            manifest,
            chain,
            leaf,
            key_pem: key,
        })
    }
}

#[cfg(test)]
fn current_directory(root: &Path) -> Result<Option<PathBuf>, PublishError> {
    current_generation(root).map(|generation| {
        generation.map(|generation| root.join("generations").join(generation.manifest.target_revision.to_string()))
    })
}

pub(crate) fn configured_server_snapshot(
    tls_path: &str,
    source: &rustfs_tls_runtime::TlsSourceSnapshot,
) -> Result<Option<TlsMaterialSnapshot>, PublishError> {
    let Some(root) = configured_root()? else {
        return Ok(None);
    };
    validate_source_separation(&root, tls_path)?;
    single_default_source(tls_path)?;
    current_generation(&root)?
        .map(|generation| generation.into_snapshot(source))
        .transpose()
}

fn state_at(root: &Path, tls_path: &str) -> Result<CertificateState, PublishError> {
    let generation = current_generation(root)?;
    state_from_generation(generation.as_ref(), tls_path)
}

fn state_from_generation(generation: Option<&PublishedGeneration>, tls_path: &str) -> Result<CertificateState, PublishError> {
    if let Some(generation) = generation {
        return Ok(CertificateState {
            schema_version: SCHEMA,
            resource_id: RESOURCE,
            target_revision: generation.manifest.target_revision.to_string(),
            certificate_fingerprint: generation.leaf.fingerprint.clone(),
            not_after_unix: generation.leaf.not_after_unix,
            last_issuance_id: Some(generation.manifest.issuance_id),
            observed_certificate_fingerprint: observed(tls_path, &generation.chain),
        });
    }
    let snapshot = tls_material::observed_server_snapshot(tls_path).ok_or(PublishError::Disabled)?;
    let Some(ServerTlsMaterial::SingleCert { certs, .. }) = &snapshot.server else {
        return Err(PublishError::Invalid);
    };
    let leaf = parse_leaf(certs.first().ok_or(PublishError::Invalid)?.as_ref())?;
    Ok(CertificateState {
        schema_version: SCHEMA,
        resource_id: RESOURCE,
        target_revision: format!("bootstrap-{}", leaf.fingerprint),
        certificate_fingerprint: leaf.fingerprint,
        not_after_unix: leaf.not_after_unix,
        last_issuance_id: None,
        observed_certificate_fingerprint: observed(tls_path, certs),
    })
}

pub(crate) async fn certificate_state(tls_path: String) -> Result<CertificateState, PublishError> {
    tokio::task::spawn_blocking(move || {
        let root = configured_root()?.ok_or(PublishError::Disabled)?;
        validate_source_separation(&root, &tls_path)?;
        single_default_source(&tls_path)?;
        state_at(&root, &tls_path)
    })
    .await
    .map_err(|_| PublishError::Storage)?
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), PublishError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn receipt(state: CertificateState, issuance_id: Uuid) -> PublishReceipt {
    PublishReceipt {
        schema_version: SCHEMA,
        resource_id: RESOURCE,
        issuance_id,
        target_revision: state.target_revision,
        certificate_fingerprint: state.certificate_fingerprint,
        not_after_unix: state.not_after_unix,
        observed_certificate_fingerprint: state.observed_certificate_fingerprint,
    }
}

fn mark_initialized(root: &Path) -> Result<(), PublishError> {
    let marker = root.join(INITIALIZED);
    if fs::symlink_metadata(&marker).is_ok() {
        if read_limited(&marker, 128)? != INITIALIZED_CONTENT {
            return Err(PublishError::Invalid);
        }
    } else {
        // Partial writes must remain unreferenced so an identical issuance can recover after I/O failure.
        let temporary = root.join(format!(".initialized-{}", Uuid::new_v4()));
        write_new(&temporary, INITIALIZED_CONTENT)?;
        fs::rename(temporary, &marker)?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

fn publish_at(root: &Path, tls_path: &str, request: PublishRequest) -> Result<PublishReceipt, PublishError> {
    publish_at_inner(
        root,
        tls_path,
        request,
        #[cfg(test)]
        FailurePoint::None,
    )
}

fn publish_at_inner(
    root: &Path,
    tls_path: &str,
    request: PublishRequest,
    #[cfg(test)] failure: FailurePoint,
) -> Result<PublishReceipt, PublishError> {
    validate_root(root)?;
    validate_source_separation(root, tls_path)?;
    single_default_source(tls_path)?;
    if request.schema_version != SCHEMA
        || request.resource_id != RESOURCE
        || request.issuance_id.is_nil()
        || request.certificate_pem.is_empty()
        || request.certificate_pem.len() > MAX_CERT
        || request.private_key_pem.is_empty()
        || request.private_key_pem.len() > MAX_KEY
        || request.expected_target_revision.len() > 128
    {
        return Err(PublishError::Invalid);
    }
    // OS lock is shared by processes using this local volume. Captured CAS tokens are never refreshed on retry.
    let lock = open_regular(&root.join("publisher.lock"), true, true)?;
    lock.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => PublishError::Busy,
        std::fs::TryLockError::Error(_) => PublishError::Storage,
    })?;
    let current = current_generation(root)?;
    let state = state_from_generation(current.as_ref(), tls_path)?;
    let digest = payload_digest(request.certificate_pem.as_bytes(), request.private_key_pem.as_bytes());
    if state.last_issuance_id == Some(request.issuance_id) {
        let generation = current.as_ref().ok_or(PublishError::Invalid)?;
        if generation.manifest.payload_digest != digest {
            return Err(PublishError::Conflict);
        }
        // A prior response may have been lost after rename or directory sync.
        File::open(root)?.sync_all()?;
        mark_initialized(root)?;
        return Ok(receipt(state, request.issuance_id));
    }
    if request.expected_target_revision != state.target_revision {
        return Err(PublishError::Conflict);
    }
    let accepted = tls_material::observed_server_snapshot(tls_path).ok_or(PublishError::Disabled)?;
    let Some(ServerTlsMaterial::SingleCert { certs, .. }) = &accepted.server else {
        return Err(PublishError::Invalid);
    };
    let old_names = if let Some(generation) = current.as_ref() {
        generation.leaf.names.clone()
    } else {
        if snapshot_leaf(&accepted).as_ref() != Some(&state.certificate_fingerprint) {
            return Err(PublishError::Conflict);
        }
        parse_leaf(certs.first().ok_or(PublishError::Invalid)?.as_ref())?.names
    };
    let new_chain = certificate_pem_items(request.certificate_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PublishError::Invalid)?;
    let new_leaf = parse_leaf(new_chain.first().ok_or(PublishError::Invalid)?.as_ref())?;
    if new_leaf.is_ca || !old_names.is_subset(&new_leaf.names) {
        return Err(PublishError::Invalid);
    }
    let generations = root.join("generations");
    if !generations.exists() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&generations)?;
        File::open(root)?.sync_all()?;
    }
    if !fs::symlink_metadata(&generations)?.is_dir() {
        return Err(PublishError::Invalid);
    }
    let revision = Uuid::new_v4();
    let directory = generations.join(revision.to_string());
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&directory)?;
    let mut pointer_may_reference = false;
    let result = (|| {
        write_new(&directory.join(RUSTFS_TLS_CERT), request.certificate_pem.as_bytes())?;
        write_new(&directory.join(RUSTFS_TLS_KEY), request.private_key_pem.as_bytes())?;
        let source = TlsSource::from_directory(tls_path);
        let candidate = TlsMaterialSnapshot::load_from_snapshot_with_server_directory(
            &source.snapshot().map_err(|_| PublishError::Invalid)?,
            Some(&directory),
        )
        .map_err(|_| PublishError::Invalid)?;
        if !matches!(&candidate.server, Some(ServerTlsMaterial::SingleCert { certs, .. }) if certs == &new_chain) {
            return Err(PublishError::Invalid);
        }
        tls_material::validate_candidate_replacement(&accepted, &candidate).map_err(|_| PublishError::Invalid)?;
        tls_material::validate_required_material_files(
            &crate::startup_tls_material::required_startup_material_files(tls_path),
            &candidate,
        )
        .map_err(|_| PublishError::Invalid)?;
        let prepared = tls_material::PreparedServerTls::from_snapshot(&candidate)
            .map_err(|_| PublishError::Invalid)?
            .ok_or(PublishError::Invalid)?;
        prepared.tcp_acceptor().map_err(|_| PublishError::Invalid)?;
        #[cfg(feature = "http3")]
        prepared.http3_config().map_err(|_| PublishError::Invalid)?;
        let ledger = Manifest {
            schema_version: SCHEMA.into(),
            issuance_id: request.issuance_id,
            target_revision: revision,
            certificate_fingerprint: new_leaf.fingerprint.clone(),
            not_after_unix: new_leaf.not_after_unix,
            payload_digest: digest,
        };
        write_new(
            &directory.join(MANIFEST),
            &serde_json::to_vec(&ledger).map_err(|_| PublishError::Invalid)?,
        )?;
        File::open(&directory)?.sync_all()?;
        File::open(&generations)?.sync_all()?;
        let temporary = root.join(format!(".pointer-{revision}"));
        #[cfg(unix)]
        std::os::unix::fs::symlink(Path::new("generations").join(revision.to_string()), &temporary)?;
        #[cfg(not(unix))]
        let () = Err::<(), _>(PublishError::Disabled)?;
        // A rename error can itself have an uncertain outcome on a failing filesystem.
        pointer_may_reference = true;
        fs::rename(&temporary, root.join(CURRENT))?;
        #[cfg(test)]
        {
            if matches!(failure, FailurePoint::AfterPointerRenameUnreadable) {
                fs::remove_file(root.join(CURRENT))?;
                write_new(&root.join(CURRENT), b"unreadable pointer")?;
            }
            if !matches!(failure, FailurePoint::None) {
                return Err(PublishError::Storage);
            }
        }
        File::open(root)?.sync_all()?;
        mark_initialized(root)?;
        let installed = PublishedGeneration {
            manifest: ledger,
            chain: new_chain,
            leaf: new_leaf,
            key_pem: request.private_key_pem.into_bytes(),
        };
        Ok(receipt(state_from_generation(Some(&installed), tls_path)?, request.issuance_id))
    })();
    // Observation errors must never convert an uncertain publication into deletion of serving material.
    if result.is_err() && !pointer_may_reference {
        let _ = fs::remove_dir_all(&directory);
    }
    result
}

pub(crate) async fn publish(tls_path: String, request: PublishRequest) -> Result<PublishReceipt, PublishError> {
    if !rustfs_utils::get_env_bool(ENV_TLS_RELOAD_ENABLE, DEFAULT_TLS_RELOAD_ENABLE) {
        return Err(PublishError::Disabled);
    }
    if rustfs_utils::get_env_bool(rustfs_config::ENV_TRUST_LEAF_CERT_AS_CA, rustfs_config::DEFAULT_TRUST_LEAF_CERT_AS_CA) {
        return Err(PublishError::Disabled);
    }
    let root = configured_root()?.ok_or(PublishError::Disabled)?;
    let notification_path = tls_path.clone();
    let result = tokio::task::spawn_blocking(move || publish_at(&root, &tls_path, request))
        .await
        .map_err(|_| PublishError::Storage)?;
    tls_material::notify_server_publication(&notification_path);
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn pair(subject: &str) -> rcgen::CertifiedKey<rcgen::KeyPair> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        rcgen::generate_simple_self_signed(vec![subject.into()]).expect("certificate")
    }

    fn build_request(expected: &str, certificate: &rcgen::CertifiedKey<rcgen::KeyPair>) -> PublishRequest {
        PublishRequest {
            schema_version: SCHEMA.into(),
            resource_id: RESOURCE.into(),
            expected_target_revision: expected.into(),
            issuance_id: Uuid::new_v4(),
            certificate_pem: certificate.cert.pem(),
            private_key_pem: certificate.signing_key.serialize_pem(),
        }
    }

    fn clone_request(request: &PublishRequest) -> PublishRequest {
        PublishRequest {
            schema_version: request.schema_version.clone(),
            resource_id: request.resource_id.clone(),
            expected_target_revision: request.expected_target_revision.clone(),
            issuance_id: request.issuance_id,
            certificate_pem: request.certificate_pem.clone(),
            private_key_pem: request.private_key_pem.clone(),
        }
    }

    fn measure_operation<T>(operation: impl FnOnce() -> T) -> (T, OperationReads) {
        OPERATION_READS.with(|counts| counts.set(OperationReads::default()));
        let result = operation();
        (result, OPERATION_READS.with(std::cell::Cell::get))
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tls_publication_rejects_missing_startup_ca_source() {
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (rustfs_config::ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
                (rustfs_config::ENV_TRUST_SYSTEM_CA, Some("0")),
                (rustfs_config::ENV_MTLS_CLIENT_CERT, None),
                (rustfs_config::ENV_MTLS_CLIENT_KEY, None),
                (rustfs_config::ENV_RUSTFS_EXTRA_CA_CERT, None),
                (ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let previous = crate::startup_runtime_sources::current_outbound_tls_state().await;
                let previous_files = crate::startup_tls_material::replace_startup_material_files(None);
                let base = tempfile::TempDir::new().expect("base");
                let root = tempfile::TempDir::new().expect("publication root");
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("permissions");
                let original = pair("server.example");
                fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("certificate");
                fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("key");
                fs::write(base.path().join(rustfs_config::RUSTFS_PUBLIC_CERT), pair("public-ca.example").cert.pem())
                    .expect("public trust");
                fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), pair("fallback-ca.example").cert.pem())
                    .expect("fallback trust");
                let path = base.path().to_str().expect("path");
                let mut config = crate::config::Config::new("127.0.0.1:0", Vec::new());
                config.tls_path = Some(path.into());
                crate::startup_tls_material::init_outbound_tls_material(&config)
                    .await
                    .expect("real startup publication");
                fs::remove_file(base.path().join(rustfs_config::RUSTFS_CA_CERT)).expect("remove startup CA before listener");
                let initial = tls_material::load_tls_material(path).await.expect("HTTP initial snapshot");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared TLS")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let task = tls_material::spawn_reload_loop(
                    path.into(),
                    holder,
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload enabled");
                let state = state_at(root.path(), path).expect("valid bootstrap CAS state");
                let result = publish_at(root.path(), path, build_request(&state.target_revision, &pair("server.example")));
                let installed = current_directory(root.path()).expect("read durable pointer");
                task.abort();
                let _ = task.await;
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while tls_material::observed_server_snapshot(path).is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("reload group exits before restoring startup state");
                crate::startup_runtime_sources::publish_outbound_tls_state(
                    rustfs_tls_runtime::TlsGeneration(
                        crate::startup_runtime_sources::current_outbound_tls_generation().saturating_add(1),
                    ),
                    &rustfs_tls_runtime::OutboundTlsMaterial {
                        root_ca_pem: previous.root_ca_pem.unwrap_or_default(),
                        mtls_identity: previous.mtls_identity,
                    },
                )
                .await;
                crate::startup_tls_material::replace_startup_material_files(previous_files);
                assert!(
                    matches!(result, Err(PublishError::Invalid)),
                    "source loss must reject an otherwise valid renewal"
                );
                assert!(installed.is_none(), "rejected trust must not acquire a durable publication pointer");
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn publication_operation_reads_are_bounded() {
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let base = tempfile::TempDir::new().expect("base");
                let root = tempfile::TempDir::new().expect("publication root");
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("permissions");
                let original = pair("server.example");
                fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("certificate");
                fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("key");
                let path = base.path().to_str().expect("path");
                let initial = TlsMaterialSnapshot::load(&TlsSource::from_directory(base.path()))
                    .await
                    .expect("initial");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared TLS")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let task = tls_material::spawn_reload_loop(
                    path.into(),
                    holder,
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload task");
                let bootstrap = state_at(root.path(), path).expect("bootstrap");
                let request = build_request(&bootstrap.target_revision, &pair("server.example"));
                let retry = clone_request(&request);
                publish_at(root.path(), path, request).expect("first publication");
                let (state, get_counts) = measure_operation(|| state_at(root.path(), path).expect("state"));
                let (_, retry_counts) = measure_operation(|| publish_at(root.path(), path, retry).expect("retry"));
                let (_, reload_counts) = measure_operation(|| {
                    let generation = current_generation(root.path()).expect("generation").expect("published");
                    generation
                        .into_snapshot(&TlsSource::from_directory(base.path()).snapshot().expect("source"))
                        .expect("runtime snapshot")
                });
                let (_, publish_counts) = measure_operation(|| {
                    publish_at(root.path(), path, build_request(&state.target_revision, &pair("server.example")))
                        .expect("next publication")
                });
                println!("GET={get_counts:?}; RETRY={retry_counts:?}; PUBLISH={publish_counts:?}");
                println!("RELOAD={reload_counts:?}");
                assert_eq!(
                    reload_counts,
                    OperationReads {
                        certificates: 1,
                        keys: 1,
                        manifests: 1,
                        manifest_decodes: 1,
                        certificate_pem_traversals: 1,
                        leaf_der_decodes: 1
                    }
                );
                assert_eq!(
                    get_counts,
                    OperationReads {
                        certificates: 1,
                        keys: 1,
                        manifests: 1,
                        manifest_decodes: 1,
                        certificate_pem_traversals: 1,
                        leaf_der_decodes: 1
                    }
                );
                assert_eq!(
                    retry_counts,
                    OperationReads {
                        certificates: 1,
                        keys: 1,
                        manifests: 1,
                        manifest_decodes: 1,
                        certificate_pem_traversals: 1,
                        leaf_der_decodes: 1
                    }
                );
                assert_eq!(
                    publish_counts,
                    OperationReads {
                        certificates: 1,
                        keys: 1,
                        manifests: 1,
                        manifest_decodes: 1,
                        certificate_pem_traversals: 2,
                        leaf_der_decodes: 2
                    }
                );
                task.abort();
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn publication_preserves_trust_rejects_stale_and_recovers_lost_receipt() {
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let base = tempfile::TempDir::new().expect("base");
                let root = tempfile::TempDir::new().expect("publication root");
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("root permissions");
                let original = pair("server.example");
                fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("original certificate");
                fs::rename(base.path().join(RUSTFS_TLS_CERT), base.path().join("seed-certificate.pem"))
                    .expect("symlink seed backing");
                std::os::unix::fs::symlink("seed-certificate.pem", base.path().join(RUSTFS_TLS_CERT))
                    .expect("Certbot-style seed symlink");
                fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("original key");
                let old_ca = pair("ca-old.example");
                fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), old_ca.cert.pem()).expect("trust file");
                let path = base.path().to_str().expect("base path");
                let initial = TlsMaterialSnapshot::load(&TlsSource::from_directory(base.path()))
                    .await
                    .expect("initial");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared TLS")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let task = tls_material::spawn_reload_loop(
                    path.into(),
                    holder,
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload task");
                let bootstrap = state_at(root.path(), path).expect("bootstrap state");
                let candidate = pair("server.example");
                let request = build_request(&bootstrap.target_revision, &candidate);
                let retry = clone_request(&request);
                let mutated = clone_request(&request);
                let receipt = publish_at(root.path(), path, request).expect("durable receipt");
                assert_ne!(receipt.target_revision, bootstrap.target_revision);
                assert_eq!(receipt.certificate_fingerprint, hex_digest(candidate.cert.der().as_ref()));
                assert_eq!(
                    fs::read(base.path().join(RUSTFS_TLS_CERT)).expect("base certificate"),
                    original.cert.pem().as_bytes()
                );
                let new_ca = pair("ca-new.example");
                fs::write(base.path().join(rustfs_config::RUSTFS_CA_CERT), new_ca.cert.pem())
                    .expect("independent trust rotation");
                let directory = current_directory(root.path()).expect("pointer").expect("generation");
                let snapshot = TlsMaterialSnapshot::load_from_snapshot_with_server_directory(
                    &TlsSource::from_directory(base.path()).snapshot().expect("source"),
                    Some(&directory),
                )
                .expect("overlay snapshot");
                assert_eq!(snapshot.outbound.root_ca_pem, new_ca.cert.pem().as_bytes());
                assert_eq!(snapshot_leaf(&snapshot), Some(receipt.certificate_fingerprint.clone()));
                let retry_receipt = publish_at(root.path(), path, retry).expect("lost receipt recovery");
                assert_eq!(retry_receipt.target_revision, receipt.target_revision);
                let mut mutated = mutated;
                mutated.private_key_pem = pair("server.example").signing_key.serialize_pem();
                assert!(matches!(publish_at(root.path(), path, mutated), Err(PublishError::Conflict)));
                assert!(matches!(
                    publish_at(root.path(), path, build_request(&bootstrap.target_revision, &pair("server.example"))),
                    Err(PublishError::Conflict)
                ));
                assert_eq!(
                    state_at(root.path(), path).expect("accepted state").target_revision,
                    receipt.target_revision
                );
                let expanded = rcgen::generate_simple_self_signed(vec!["server.example".into(), "extra.example".into()])
                    .expect("expanded names");
                let expanded_receipt = publish_at(root.path(), path, build_request(&receipt.target_revision, &expanded))
                    .expect("pending expanded generation");
                assert!(
                    matches!(
                        publish_at(root.path(), path, build_request(&expanded_receipt.target_revision, &candidate)),
                        Err(PublishError::Invalid)
                    ),
                    "a later publication must preserve names of the durable generation even before it is observed"
                );
                let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
                let one = build_request(&expanded_receipt.target_revision, &expanded);
                let two = build_request(&expanded_receipt.target_revision, &expanded);
                let root_path = root.path();
                let (one, two) = std::thread::scope(|scope| {
                    let barrier_one = barrier.clone();
                    let first = scope.spawn(move || {
                        barrier_one.wait();
                        publish_at(root_path, path, one)
                    });
                    let second = scope.spawn(|| {
                        barrier.wait();
                        publish_at(root_path, path, two)
                    });
                    (first.join().expect("first publisher"), second.join().expect("second publisher"))
                });
                assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1, "one concurrent CAS winner");
                assert!(
                    matches!(one, Err(PublishError::Conflict | PublishError::Busy))
                        || matches!(two, Err(PublishError::Conflict | PublishError::Busy))
                );
                let latest = state_at(root.path(), path).expect("concurrent winner");
                let interrupted = build_request(&latest.target_revision, &expanded);
                let retry = clone_request(&interrupted);
                assert!(
                    matches!(
                        publish_at_inner(root.path(), path, interrupted, FailurePoint::AfterPointerRename),
                        Err(PublishError::Storage)
                    ),
                    "failure after rename has an unknown durable outcome"
                );
                let recovered =
                    publish_at(root.path(), path, retry).expect("same issuance recovers and syncs the installed generation");
                assert_ne!(recovered.target_revision, latest.target_revision);
                assert!(
                    current_directory(root.path())
                        .expect("installed generation survives error")
                        .is_some()
                );
                let interrupted = build_request(&recovered.target_revision, &expanded);
                let before = fs::read_dir(root.path().join("generations")).expect("generations").count();
                assert!(matches!(
                    publish_at_inner(root.path(), path, interrupted, FailurePoint::AfterPointerRenameUnreadable),
                    Err(PublishError::Storage)
                ));
                let after = fs::read_dir(root.path().join("generations"))
                    .expect("generations after unreadable pointer")
                    .count();
                assert_eq!(after, before + 1, "an uncertain pointer must not delete the new generation");
                fs::remove_file(root.path().join(CURRENT)).expect("simulate accepted pointer loss");
                assert!(
                    matches!(current_directory(root.path()), Err(PublishError::Invalid)),
                    "an acknowledged publisher must never silently revert to bootstrap after pointer loss"
                );
                task.abort();
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn expired_generation_can_renew_and_next_operation_detects_corruption() {
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let base = tempfile::TempDir::new().expect("base");
                let root = tempfile::TempDir::new().expect("publication root");
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("permissions");
                let original = pair("server.example");
                fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("certificate");
                fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("key");
                let path = base.path().to_str().expect("path");
                let initial = TlsMaterialSnapshot::load(&TlsSource::from_directory(base.path()))
                    .await
                    .expect("initial");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let task = tls_material::spawn_reload_loop(
                    path.into(),
                    holder,
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload task");
                let expired_key = rcgen::KeyPair::generate().expect("expired key");
                let mut params = rcgen::CertificateParams::new(vec!["server.example".into(), "expired-required.example".into()])
                    .expect("parameters");
                params.not_before = rcgen::date_time_ymd(2000, 1, 1);
                params.not_after = rcgen::date_time_ymd(2001, 1, 1);
                let expired = params.self_signed(&expired_key).expect("expired certificate");
                let certificate = expired.pem();
                let key = expired_key.serialize_pem();
                let revision = Uuid::new_v4();
                let generations = root.path().join("generations");
                fs::create_dir(&generations).expect("generations");
                let directory = generations.join(revision.to_string());
                fs::create_dir(&directory).expect("generation");
                write_new(&directory.join(RUSTFS_TLS_CERT), certificate.as_bytes()).expect("expired cert");
                write_new(&directory.join(RUSTFS_TLS_KEY), key.as_bytes()).expect("expired key");
                let leaf = parse_leaf(expired.der().as_ref()).expect("expired metadata");
                let manifest = Manifest {
                    schema_version: SCHEMA.into(),
                    issuance_id: Uuid::new_v4(),
                    target_revision: revision,
                    certificate_fingerprint: leaf.fingerprint,
                    not_after_unix: leaf.not_after_unix,
                    payload_digest: payload_digest(certificate.as_bytes(), key.as_bytes()),
                };
                write_new(&directory.join(MANIFEST), &serde_json::to_vec(&manifest).expect("manifest")).expect("manifest file");
                std::os::unix::fs::symlink(Path::new("generations").join(revision.to_string()), root.path().join(CURRENT))
                    .expect("pointer");
                mark_initialized(root.path()).expect("initialized");
                let state = state_at(root.path(), path).expect("expired visible head remains readable");
                assert!(state.not_after_unix < 1_000_000_000);
                assert!(
                    current_generation(root.path())
                        .expect("expired generation")
                        .expect("present")
                        .into_snapshot(&TlsSource::from_directory(base.path()).snapshot().expect("source"))
                        .is_err(),
                    "reading an expired ledger must never bypass serving validity checks"
                );
                assert!(
                    matches!(
                        publish_at(root.path(), path, build_request(&state.target_revision, &pair("server.example"))),
                        Err(PublishError::Invalid)
                    ),
                    "expired durable SAN coverage must survive renewal"
                );
                let renewed =
                    rcgen::generate_simple_self_signed(vec!["server.example".into(), "expired-required.example".into()])
                        .expect("renewal");
                let request = build_request(&state.target_revision, &renewed);
                let retry = clone_request(&request);
                let receipt = publish_at(root.path(), path, request).expect("renew expired generation");
                assert_ne!(receipt.target_revision, state.target_revision);
                let generation = current_generation(root.path()).expect("published").expect("generation");
                assert_eq!(
                    state_at(root.path(), path).expect("first independent read").target_revision,
                    receipt.target_revision
                );
                let directory = root
                    .path()
                    .join("generations")
                    .join(generation.manifest.target_revision.to_string());
                fs::write(directory.join(RUSTFS_TLS_KEY), "corrupted key").expect("corrupt after read");
                let snapshot = generation
                    .into_snapshot(&TlsSource::from_directory(base.path()).snapshot().expect("source"))
                    .expect("the pinned operation consumes its already verified material without a second file read");
                assert_eq!(snapshot_leaf(&snapshot), Some(receipt.certificate_fingerprint.clone()));
                assert!(
                    matches!(state_at(root.path(), path), Err(PublishError::Invalid)),
                    "the next operation must revalidate disk"
                );
                assert!(
                    matches!(publish_at(root.path(), path, retry), Err(PublishError::Invalid)),
                    "idempotent retry must not bypass disk corruption"
                );
                task.abort();
            },
        )
        .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn invalid_candidate_never_changes_pointer() {
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (ENV_TLS_PUBLISH_DIR, None),
            ],
            async {
                let base = tempfile::TempDir::new().expect("base");
                let root = tempfile::TempDir::new().expect("publication root");
                fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("root permissions");
                let original = pair("server.example");
                fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("certificate");
                fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("key");
                let path = base.path().to_str().expect("base path");
                let initial = TlsMaterialSnapshot::load(&TlsSource::from_directory(base.path()))
                    .await
                    .expect("initial");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared TLS")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let task = tls_material::spawn_reload_loop(
                    path.into(),
                    holder,
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload task");
                let expected = state_at(root.path(), path).expect("state").target_revision;
                assert!(matches!(
                    publish_at(root.path(), path, build_request(&expected, &pair("wrong.example"))),
                    Err(PublishError::Invalid)
                ));
                let valid = pair("server.example");
                let mut mismatch = build_request(&expected, &valid);
                mismatch.private_key_pem = pair("server.example").signing_key.serialize_pem();
                assert!(matches!(publish_at(root.path(), path, mismatch), Err(PublishError::Invalid)));
                let mut ca_params = rcgen::CertificateParams::new(vec!["server.example".into()]).expect("CA params");
                ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
                let ca_key = rcgen::KeyPair::generate().expect("CA key");
                let ca_cert = ca_params.self_signed(&ca_key).expect("CA certificate");
                let mut ca_candidate = build_request(&expected, &valid);
                ca_candidate.certificate_pem = ca_cert.pem();
                ca_candidate.private_key_pem = ca_key.serialize_pem();
                assert!(
                    matches!(publish_at(root.path(), path, ca_candidate), Err(PublishError::Invalid)),
                    "a CA certificate must not become the serving leaf"
                );
                let mut oversized = build_request(&expected, &valid);
                oversized.certificate_pem = "x".repeat(MAX_CERT + 1);
                assert!(matches!(publish_at(root.path(), path, oversized), Err(PublishError::Invalid)));
                assert!(current_directory(root.path()).expect("pointer absent").is_none());
                assert_eq!(state_at(root.path(), path).expect("unchanged").target_revision, expected);
                task.abort();
            },
        )
        .await;
    }

    #[test]
    fn corrupt_or_escaping_pointer_is_rejected() {
        let root = tempfile::TempDir::new().expect("root");
        std::os::unix::fs::symlink("../outside", root.path().join(CURRENT)).expect("pointer");
        assert!(matches!(current_directory(root.path()), Err(PublishError::Invalid)));
        fs::remove_file(root.path().join(CURRENT)).expect("remove pointer");
        fs::write(root.path().join(CURRENT), "generations/foo").expect("regular pointer");
        assert!(matches!(current_directory(root.path()), Err(PublishError::Invalid)));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn publication_is_observed_by_live_tls_handshakes() {
        let base = tempfile::TempDir::new().expect("base");
        let root = tempfile::TempDir::new().expect("publication root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("root permissions");
        let original = pair("server.example");
        let renewed = pair("server.example");
        fs::write(base.path().join(RUSTFS_TLS_CERT), original.cert.pem()).expect("certificate");
        fs::write(base.path().join(RUSTFS_TLS_KEY), original.signing_key.serialize_pem()).expect("key");
        temp_env::async_with_vars(
            [
                (ENV_TLS_RELOAD_ENABLE, Some("1")),
                (ENV_TLS_PUBLISH_DIR, root.path().to_str()),
                (rustfs_config::ENV_SERVER_MTLS_ENABLE, Some("0")),
                (rustfs_config::ENV_TRUST_SYSTEM_CA, Some("0")),
                (rustfs_config::ENV_TRUST_LEAF_CERT_AS_CA, Some("0")),
                (rustfs_config::ENV_RUSTFS_TLS_PATH, Some("ignored-env-source")),
            ],
            async {
                let path = base.path().to_str().expect("path");
                let initial = tls_material::load_tls_material(path).await.expect("initial snapshot");
                let prepared = tls_material::PreparedServerTls::from_snapshot(&initial)
                    .expect("prepared TLS")
                    .expect("server");
                let holder = prepared.tcp_acceptor().expect("acceptor");
                let registration = tls_material::spawn_reload_loop(
                    path.into(),
                    holder.clone(),
                    initial,
                    prepared,
                    #[cfg(feature = "http3")]
                    None,
                )
                .expect("registration")
                .expect("reload task");
                assert_eq!(
                    tls_material::active_server_tls_path(),
                    Some(path.to_string()),
                    "registered CLI/listener source wins over environment"
                );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
                let address = listener.local_addr().expect("address");
                let server = tokio::spawn(async move {
                    for _ in 0..2 {
                        let (socket, _) = listener.accept().await.expect("TCP connection");
                        let _tls = holder.get().accept(socket).await.expect("TLS handshake");
                    }
                });
                let mut roots = rustls::RootCertStore::empty();
                roots.add(original.cert.der().clone()).expect("bootstrap anchor");
                roots.add(renewed.cert.der().clone()).expect("renewal anchor");
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
                async fn handshake(connector: &tokio_rustls::TlsConnector, address: std::net::SocketAddr) -> String {
                    let socket = tokio::net::TcpStream::connect(address).await.expect("connect");
                    let stream = connector
                        .connect(rustls::pki_types::ServerName::try_from("server.example").expect("SNI"), socket)
                        .await
                        .expect("verified handshake");
                    hex_digest(stream.get_ref().1.peer_certificates().expect("peer chain")[0].as_ref())
                }
                assert_eq!(handshake(&connector, address).await, hex_digest(original.cert.der().as_ref()));
                let bootstrap = certificate_state(path.into()).await.expect("bootstrap state");
                let receipt = publish(path.into(), build_request(&bootstrap.target_revision, &renewed))
                    .await
                    .expect("publish");
                let expected = hex_digest(renewed.cert.der().as_ref());
                assert_eq!(receipt.certificate_fingerprint, expected);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        if certificate_state(path.into())
                            .await
                            .expect("state")
                            .observed_certificate_fingerprint
                            == Some(expected.clone())
                        {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("publication observed by serving group");
                assert_eq!(handshake(&connector, address).await, expected);
                let mut expected_chain = vec![renewed.cert.der().clone()];
                assert_eq!(observed(path, &expected_chain), Some(expected.clone()));
                expected_chain.push(original.cert.der().clone());
                assert!(
                    observed(path, &expected_chain).is_none(),
                    "matching leaf alone must not acknowledge a different serving chain"
                );
                server.await.expect("TLS server");
                registration.abort();
            },
        )
        .await;
    }
}
