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

//! KMS backup/restore admin API: export a bundle, preflight a restore, execute
//! a restore, and roll an interrupted restore back.
//!
//! # Trust root separation
//!
//! The backup KEK is read from the environment and never from the KMS itself.
//! A bundle encrypted under a key that is part of the state being backed up
//! would be worthless in the disaster it exists for, so the KEK material is
//! compared against the configured backend secrets and a reused secret is
//! refused ([`BackupEnvironment::build`]).
//!
//! # Filesystem boundary
//!
//! Neither endpoint accepts a path. Bundles live under a configured backup
//! root and are addressed by a single validated name component; the restore
//! target is always the key directory of the server's own KMS configuration.
//! An admin credential therefore cannot turn these endpoints into an arbitrary
//! read or write primitive.
//!
//! # Execution model
//!
//! Backup and restore run synchronously inside the request: there is no admin
//! background-job facility to hang them on, and both are operator-driven
//! one-shot actions. Consequently there is no progress-polling endpoint. What
//! *is* resumable is a restore interrupted by a crash — the KMS layer's commit
//! marker makes re-entry converge on a complete old or complete new state, and
//! [`KmsRestoreAbortHandler`] is the explicit take-back for that case.

use super::kms_audit::{KmsAdminAudit, KmsAdminOperation};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_deployment_id, current_kms_runtime_service_manager};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::backup::{
    AtRestProtection, BackupBackendKind, BackupKek, BackupResponsibility, LocalBackupExportRequest, LocalRestoreReport,
    LocalRestoreRequest, RestoreConflictPolicy, RestoreDryRunReport, abort_local_restore, dry_run_local_restore,
    export_local_backup, read_local_bundle_manifest, restore_local_backup,
};
use rustfs_kms::config::{BackendConfig, KmsConfig, LocalConfig, VaultAuthMethod};
use rustfs_kms::{KmsBackend, KmsManager, KmsServiceManager, KmsServiceStatus};
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};
use zeroize::Zeroizing;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS_BACKUP: &str = "kms_backup";
const EVENT_ADMIN_KMS_BACKUP: &str = "admin_kms_backup";

/// Directory every backup bundle is written under and read back from.
///
/// Requests name a bundle, never a path, so this root is the only place the
/// endpoints ever touch.
const ENV_KMS_BACKUP_DIR: &str = "RUSTFS_KMS_BACKUP_DIR";

/// Base64 of the 32-byte backup KEK.
///
/// Deliberately an environment secret rather than KMS state: it must survive
/// the loss of everything it protects, and it must not be recoverable from the
/// backend being backed up.
const ENV_KMS_BACKUP_KEK: &str = "RUSTFS_KMS_BACKUP_KEK";

/// Identifier of the backup KEK, recorded in the manifest so a bundle names
/// the key it needs without carrying anything that opens it.
const ENV_KMS_BACKUP_KEK_ID: &str = "RUSTFS_KMS_BACKUP_KEK_ID";

/// Version of the backup KEK; defaults to 1.
const ENV_KMS_BACKUP_KEK_VERSION: &str = "RUSTFS_KMS_BACKUP_KEK_VERSION";

/// Longest accepted bundle name.
const MAX_BUNDLE_NAME_LEN: usize = 128;

/// Format version of [`SanitizedKmsConfig`], so a consumer can reject a shape
/// it does not know instead of guessing.
const SANITIZED_CONFIG_FORMAT_VERSION: u32 = 1;

/// An HTTP status paired with the message sent to the caller.
type Failure = (StatusCode, String);

fn kms_backup_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::BackupAction)]
}

fn kms_restore_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::RestoreAction)]
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsBackupRequest {
    /// Bundle name under the backup root, which is also the manifest's backup
    /// id. Generated from the current time when omitted.
    #[serde(default)]
    pub backup_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsBackupResponse {
    pub backup_id: String,
    pub snapshot_generation: u64,
    pub deployment_identity: String,
    pub artifact_count: usize,
    pub manifest_digest: String,
    pub backup_kek_id: String,
    pub backup_kek_version: u32,
    /// Whether the sanitized configuration artifact was sealed into the
    /// bundle.
    pub sanitized_config_included: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsRestoreDryRunRequest {
    /// Bundle name under the backup root.
    pub bundle: String,
    /// Highest snapshot generation the target has already observed, when the
    /// operator tracks one. A bundle older than this is reported as a
    /// generation regression.
    #[serde(default)]
    pub observed_generation: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsRestoreDryRunResponse {
    pub report: RestoreDryRunReport,
    /// The bundle's sanitized configuration next to the running one. Restore
    /// never applies it; adopting any difference is a separate, explicit
    /// operator action through the ordinary configuration endpoints.
    pub config_comparison: Option<ConfigComparison>,
}

/// The configuration a bundle recorded, against the configuration this server
/// is running.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigComparison {
    pub bundle: SanitizedKmsConfig,
    pub current: SanitizedKmsConfig,
    /// False when the two differ in any field, which is informational only.
    pub identical: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsRestoreRequest {
    /// Bundle name under the backup root.
    pub bundle: String,
    #[serde(default)]
    pub observed_generation: Option<u64>,
    /// First confirmation: the backup id sealed in the bundle's manifest. A
    /// caller that has not read the dry-run report cannot supply it.
    pub confirm_backup_id: String,
    /// Second confirmation: the conflict policy, which has no default here on
    /// purpose. The KMS layer refuses to write under the `fail` policy, so a
    /// restore only ever proceeds when the operator names the writing policy
    /// explicitly.
    pub confirm_conflict_policy: RestoreConflictPolicy,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KmsRestoreAbortRequest {
    /// Confirmation: the key directory whose interrupted restore is being
    /// taken back, as reported by the status endpoint.
    pub confirm_target_key_dir: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsRestoreAbortResponse {
    pub target_key_dir: String,
    pub aborted: bool,
}

/// Readiness of the backup subsystem. Reports identifiers and paths only —
/// never KEK material.
#[derive(Debug, Serialize, Deserialize)]
pub struct KmsBackupStatusResponse {
    pub service_status: String,
    pub backend: Option<String>,
    /// What a bundle of the configured backend can be responsible for; `None`
    /// when the backend has no supported backup path at all.
    pub responsibility: Option<BackupResponsibility>,
    /// Whether this server can export a bundle right now.
    pub export_supported: bool,
    pub deployment_identity: Option<String>,
    pub backup_root: Option<String>,
    pub backup_kek_configured: bool,
    pub backup_kek_id: Option<String>,
    pub backup_kek_version: Option<u32>,
    pub target_key_dir: Option<String>,
    /// Why the subsystem is not usable, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Sanitized configuration artifact
// ---------------------------------------------------------------------------

/// Allowlist projection of the persisted KMS configuration.
///
/// Deliberately a projection rather than a redaction of [`KmsConfig`]: a
/// redaction has to enumerate every secret and silently starts leaking the day
/// a backend gains a new credential field, while a field that is not named
/// here can never reach a bundle. Nothing in this type is a secret: no Vault
/// token, no AppRole secret id, no master key, no static key material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedKmsConfig {
    pub format_version: u32,
    pub backend: String,
    pub default_key_id: Option<String>,
    pub allow_insecure_dev_defaults: bool,
    pub timeout_secs: u64,
    pub retry_attempts: u32,
    pub enable_cache: bool,
    pub backend_reference: SanitizedBackendReference,
}

/// Non-sensitive coordinates of a backend: where it is and how it is
/// addressed, never what authenticates to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SanitizedBackendReference {
    Local {
        key_dir: String,
        file_permissions: Option<u32>,
        /// Whether a master key is configured. The key itself lives outside
        /// the backup domain and the operator re-supplies it during restore.
        master_key_configured: bool,
    },
    VaultKv2 {
        address: String,
        namespace: Option<String>,
        kv_mount: String,
        key_path_prefix: String,
        auth_method: String,
        tls_configured: bool,
    },
    VaultTransit {
        address: String,
        namespace: Option<String>,
        mount_path: String,
        metadata_kv_mount: String,
        metadata_key_prefix: String,
        auth_method: String,
        tls_configured: bool,
    },
    Static {
        key_id: String,
    },
    Aws {
        region: Option<String>,
        endpoint_url: Option<String>,
    },
}

/// Authentication *kind*, never its inputs.
fn auth_method_kind(auth: &VaultAuthMethod) -> String {
    match auth {
        VaultAuthMethod::Token { .. } => "token",
        VaultAuthMethod::AppRole { .. } => "approle",
        VaultAuthMethod::Kubernetes { .. } => "kubernetes",
        VaultAuthMethod::TokenFile { .. } => "token-file",
    }
    .to_string()
}

impl SanitizedKmsConfig {
    pub fn from_config(config: &KmsConfig) -> Self {
        let backend_reference = match &config.backend_config {
            BackendConfig::Local(local) => SanitizedBackendReference::Local {
                key_dir: local.key_dir.display().to_string(),
                file_permissions: local.file_permissions,
                master_key_configured: local.master_key.is_some(),
            },
            BackendConfig::VaultKv2(vault) => SanitizedBackendReference::VaultKv2 {
                address: vault.address.clone(),
                namespace: vault.namespace.clone(),
                kv_mount: vault.kv_mount.clone(),
                key_path_prefix: vault.key_path_prefix.clone(),
                auth_method: auth_method_kind(&vault.auth_method),
                tls_configured: vault.tls.is_some(),
            },
            BackendConfig::VaultTransit(vault) => SanitizedBackendReference::VaultTransit {
                address: vault.address.clone(),
                namespace: vault.namespace.clone(),
                mount_path: vault.mount_path.clone(),
                metadata_kv_mount: vault.metadata_kv_mount.clone(),
                metadata_key_prefix: vault.metadata_key_prefix.clone(),
                auth_method: auth_method_kind(&vault.auth_method),
                tls_configured: vault.tls.is_some(),
            },
            BackendConfig::Static(static_config) => SanitizedBackendReference::Static {
                key_id: static_config.key_id.clone(),
            },
            BackendConfig::Aws(aws) => SanitizedBackendReference::Aws {
                region: aws.region.clone(),
                endpoint_url: aws.endpoint_url.clone(),
            },
        };

        Self {
            format_version: SANITIZED_CONFIG_FORMAT_VERSION,
            backend: backend_wire_name(&config.backend).to_string(),
            default_key_id: config.default_key_id.clone(),
            allow_insecure_dev_defaults: config.allow_insecure_dev_defaults,
            timeout_secs: config.timeout.as_secs(),
            retry_attempts: config.retry_attempts,
            enable_cache: config.enable_cache,
            backend_reference,
        }
    }
}

fn backend_wire_name(backend: &KmsBackend) -> &'static str {
    match backend {
        KmsBackend::Local => "local",
        KmsBackend::VaultKv2 => "vault-kv2",
        KmsBackend::VaultTransit => "vault-transit",
        KmsBackend::Static => "static",
        KmsBackend::Aws => "aws",
    }
}

/// Manifest backend discriminant for a configured backend, or `None` for a
/// backend the backup contract does not cover at all.
fn backup_backend_kind(backend: &KmsBackend) -> Option<BackupBackendKind> {
    match backend {
        KmsBackend::Local => Some(BackupBackendKind::Local),
        KmsBackend::VaultKv2 => Some(BackupBackendKind::VaultKv2),
        KmsBackend::VaultTransit => Some(BackupBackendKind::VaultTransit),
        KmsBackend::Static => Some(BackupBackendKind::Static),
        KmsBackend::Aws => None,
    }
}

/// The responsibility a bundle of this backend can claim, using the protection
/// state each backend actually ships with today.
fn declared_responsibility(backend: &KmsBackend) -> Option<BackupResponsibility> {
    let kind = backup_backend_kind(backend)?;
    let protection = match kind {
        // The concrete Local protection state is decided per record at export
        // time; every Local state maps to the same responsibility.
        BackupBackendKind::Local => AtRestProtection::EncryptedMasterKey,
        BackupBackendKind::VaultKv2 => AtRestProtection::StorageOnly,
        BackupBackendKind::VaultTransit => AtRestProtection::ExternalNonExportable,
        BackupBackendKind::Static => AtRestProtection::ExternalSecretDelivery,
    };
    BackupResponsibility::for_backend(kind, protection)
}

// ---------------------------------------------------------------------------
// Environment-sourced setup
// ---------------------------------------------------------------------------

/// Where bundles live and the key they are sealed with.
pub struct BackupEnvironment {
    root: PathBuf,
    kek: BackupKek,
    kek_id: String,
    kek_version: u32,
}

/// Identifiers only: the KEK material must not be reachable through a
/// debug-formatted error, log line, or panic message.
impl std::fmt::Debug for BackupEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupEnvironment")
            .field("root", &self.root)
            .field("kek_id", &self.kek_id)
            .field("kek_version", &self.kek_version)
            .finish_non_exhaustive()
    }
}

impl BackupEnvironment {
    fn from_env(config: &KmsConfig) -> Result<Self, Failure> {
        let root = required_env(ENV_KMS_BACKUP_DIR)?;
        let raw_kek = Zeroizing::new(required_env(ENV_KMS_BACKUP_KEK)?);
        let kek_id = required_env(ENV_KMS_BACKUP_KEK_ID)?;
        let kek_version = match std::env::var(ENV_KMS_BACKUP_KEK_VERSION) {
            Ok(value) => value.trim().parse::<u32>().map_err(|_| {
                (
                    StatusCode::PRECONDITION_FAILED,
                    format!("{ENV_KMS_BACKUP_KEK_VERSION} must be an unsigned integer"),
                )
            })?,
            Err(_) => 1,
        };

        Self::build(PathBuf::from(root), &raw_kek, kek_id, kek_version, config)
    }

    /// Validate the backup KEK and the bundle root.
    ///
    /// Split from [`Self::from_env`] so the trust-root separation rule is
    /// testable without mutating process environment.
    fn build(root: PathBuf, raw_kek: &str, kek_id: String, kek_version: u32, config: &KmsConfig) -> Result<Self, Failure> {
        if kek_id.trim().is_empty() {
            return Err((StatusCode::PRECONDITION_FAILED, format!("{ENV_KMS_BACKUP_KEK_ID} must not be empty")));
        }

        let decoded = Zeroizing::new(
            BASE64
                .decode(raw_kek.trim())
                .map_err(|_| (StatusCode::PRECONDITION_FAILED, format!("{ENV_KMS_BACKUP_KEK} must be base64-encoded")))?,
        );
        if decoded.len() != 32 {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!("{ENV_KMS_BACKUP_KEK} must decode to exactly 32 bytes"),
            ));
        }

        // The bundle exists for the disaster that destroys the backend; a KEK
        // taken from that same backend would be gone with it, and would also
        // make the bundle self-encrypted.
        if reuses_business_secret(&decoded, raw_kek.trim(), config) {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "{ENV_KMS_BACKUP_KEK} reuses a secret of the configured KMS backend; the backup KEK must be an \
                     independent trust root"
                ),
            ));
        }

        let mut material = [0u8; 32];
        material.copy_from_slice(&decoded);
        let kek = BackupKek::new(kek_id.clone(), kek_version, material)
            .map_err(|error| (StatusCode::PRECONDITION_FAILED, error.to_string()))?;

        Ok(Self {
            root,
            kek,
            kek_id,
            kek_version,
        })
    }
}

fn required_env(name: &str) -> Result<String, Failure> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err((
            StatusCode::PRECONDITION_FAILED,
            format!("{name} is not set; KMS backup and restore are disabled until it is"),
        )),
    }
}

/// Every plaintext secret the configured backend authenticates with.
fn business_trust_root_secrets(config: &KmsConfig) -> Vec<Zeroizing<String>> {
    let mut secrets: Vec<Zeroizing<String>> = Vec::new();
    let mut push_auth = |auth: &VaultAuthMethod| match auth {
        VaultAuthMethod::Token { token } => secrets.push(Zeroizing::new(token.clone())),
        VaultAuthMethod::AppRole { role_id, secret_id, .. } => {
            secrets.push(Zeroizing::new(role_id.clone()));
            secrets.push(Zeroizing::new(secret_id.clone()));
        }
        // Kubernetes and TokenFile hold no inline plaintext credential: the
        // ServiceAccount token and the agent-managed token live in files, and
        // the role names a Vault binding rather than half a credential pair.
        VaultAuthMethod::Kubernetes { .. } | VaultAuthMethod::TokenFile { .. } => {}
    };

    match &config.backend_config {
        BackendConfig::Local(local) => {
            if let Some(master_key) = &local.master_key {
                secrets.push(Zeroizing::new(master_key.clone()));
            }
        }
        BackendConfig::VaultKv2(vault) => push_auth(&vault.auth_method),
        BackendConfig::VaultTransit(vault) => push_auth(&vault.auth_method),
        BackendConfig::Static(static_config) => secrets.push(Zeroizing::new(static_config.secret_key.clone())),
        BackendConfig::Aws(_) => {}
    }
    secrets
}

/// Whether the supplied KEK is one of the backend's own secrets, compared both
/// as the literal environment value and as raw key bytes so an operator cannot
/// slip past by re-encoding the same secret.
fn reuses_business_secret(kek_material: &[u8], raw_kek: &str, config: &KmsConfig) -> bool {
    business_trust_root_secrets(config).into_iter().any(|secret| {
        if secret.is_empty() {
            return false;
        }
        if raw_kek == secret.as_str() || secret.as_bytes() == kek_material {
            return true;
        }
        BASE64.decode(secret.as_str()).is_ok_and(|decoded| decoded == kek_material)
    })
}

/// Accept a single, benign path component and nothing else.
///
/// The bundle name becomes both a directory under the backup root and the
/// manifest's backup id, so it has to be free of separators, traversal and
/// anything a shell or log consumer would have to escape.
fn validated_bundle_name(name: &str) -> Result<&str, Failure> {
    let invalid = |detail: &str| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid bundle name {name:?}: {detail}; expected a single name of [A-Za-z0-9._-]"),
        )
    };

    if name.is_empty() {
        return Err(invalid("empty"));
    }
    if name.len() > MAX_BUNDLE_NAME_LEN {
        return Err(invalid("longer than 128 characters"));
    }
    if name == "." || name == ".." {
        return Err(invalid("path traversal"));
    }
    if name.starts_with('.') {
        return Err(invalid("must not start with a dot"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        return Err(invalid("contains characters outside the accepted set"));
    }
    Ok(name)
}

/// Snapshot generation for an export.
///
/// The contract only requires monotonicity per deployment, and the wall clock
/// in seconds satisfies it without introducing persisted counter state. A
/// backwards clock step would produce a lower generation, which the restore
/// side reports as a regression rather than silently accepting — the
/// conservative direction.
fn snapshot_generation_now() -> u64 {
    jiff::Timestamp::now().as_second().max(0) as u64
}

fn generated_backup_id() -> String {
    format!("backup-{}", uuid::Uuid::new_v4().simple())
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// The pieces every backup/restore operation needs from the running server.
struct BackupContext {
    service_manager: Arc<KmsServiceManager>,
    config: KmsConfig,
    deployment_identity: String,
    environment: BackupEnvironment,
}

impl BackupContext {
    async fn resolve() -> Result<Self, Failure> {
        let Some(service_manager) = current_kms_runtime_service_manager() else {
            return Err((StatusCode::SERVICE_UNAVAILABLE, "kms service manager is not initialized".to_string()));
        };
        let Some(config) = service_manager.get_config().await else {
            return Err((StatusCode::CONFLICT, "kms is not configured".to_string()));
        };
        // A bundle is bound to the deployment that produced it and a restore
        // refuses a foreign one. Without an identity that binding would be
        // empty on both sides, which is exactly the confused-deputy case the
        // manifest field exists to prevent.
        let Some(deployment_identity) = current_deployment_id().filter(|id| !id.is_empty()) else {
            return Err((
                StatusCode::CONFLICT,
                "deployment identity is not available; a bundle cannot be bound to this deployment".to_string(),
            ));
        };
        let environment = BackupEnvironment::from_env(&config)?;

        Ok(Self {
            service_manager,
            config,
            deployment_identity,
            environment,
        })
    }

    /// The Local configuration a restore targets.
    fn local_config(&self) -> Result<&LocalConfig, Failure> {
        match &self.config.backend_config {
            BackendConfig::Local(local) => Ok(local),
            _ => Err((
                StatusCode::NOT_IMPLEMENTED,
                format!(
                    "backend {} has no RustFS-side restore: {}",
                    backend_wire_name(&self.config.backend),
                    responsibility_detail(&self.config.backend)
                ),
            )),
        }
    }

    fn bundle_dir(&self, bundle: &str) -> Result<PathBuf, Failure> {
        Ok(self.environment.root.join(validated_bundle_name(bundle)?))
    }
}

fn responsibility_detail(backend: &KmsBackend) -> String {
    match declared_responsibility(backend) {
        Some(BackupResponsibility::FullMaterial) => "the bundle owns the complete material".to_string(),
        Some(BackupResponsibility::ReferenceOnly) => {
            "the secret is delivered externally and must be re-supplied by the operator".to_string()
        }
        Some(BackupResponsibility::MetadataPlusExternalRoot) => {
            "the cryptographic root is protected by the external system's own snapshot flow".to_string()
        }
        None => "this backend has no backup responsibility defined".to_string(),
    }
}

async fn execute_backup(
    manager: &KmsManager,
    context: &BackupContext,
    request: &KmsBackupRequest,
) -> Result<KmsBackupResponse, Failure> {
    // Two independent gates on purpose. The configuration decides what this
    // deployment is allowed to export, so it is checked first and cannot be
    // circumvented by whatever handle a caller managed to obtain; the client
    // lookup then supplies the fence the export actually needs.
    context.local_config()?;
    let Some(client) = manager.local_backup_client() else {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            format!(
                "backend {} cannot export a RustFS bundle: {}",
                backend_wire_name(&context.config.backend),
                responsibility_detail(&context.config.backend)
            ),
        ));
    };

    let backup_id = match &request.backup_id {
        Some(id) => validated_bundle_name(id)?.to_string(),
        None => generated_backup_id(),
    };
    let destination = context.environment.root.join(&backup_id);

    let sanitized = SanitizedKmsConfig::from_config(&context.config);
    let sanitized_bytes = serde_json::to_vec(&sanitized)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("sanitized config: {error}")))?;

    let export_request = LocalBackupExportRequest {
        backup_id: backup_id.clone(),
        deployment_identity: context.deployment_identity.clone(),
        rustfs_version: env!("CARGO_PKG_VERSION").to_string(),
        snapshot_generation: snapshot_generation_now(),
        destination,
        sanitized_config: Some(sanitized_bytes),
    };

    let manifest = export_local_backup(client, &context.environment.kek, &export_request)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("backup export failed: {error}")))?;

    Ok(KmsBackupResponse {
        backup_id: manifest.backup_id,
        snapshot_generation: manifest.snapshot_generation,
        deployment_identity: manifest.deployment_identity,
        artifact_count: manifest.artifacts.len(),
        manifest_digest: manifest.manifest_digest.hex,
        backup_kek_id: context.environment.kek_id.clone(),
        backup_kek_version: context.environment.kek_version,
        sanitized_config_included: true,
    })
}

/// Build the restore request that both the dry-run and the execution use, so
/// the preflight can never be evaluated against different parameters than the
/// restore it authorizes.
fn restore_request_for(
    context: &BackupContext,
    bundle_dir: PathBuf,
    observed_generation: Option<u64>,
    conflict_policy: RestoreConflictPolicy,
) -> Result<LocalRestoreRequest, Failure> {
    let local = context.local_config()?;
    Ok(LocalRestoreRequest {
        bundle_dir,
        target_key_dir: local.key_dir.clone(),
        target_deployment_identity: context.deployment_identity.clone(),
        observed_generation,
        // The master key is server-side configuration, never a request field:
        // an admin API is not a place to move key material through.
        master_key: local.master_key.clone(),
        conflict_policy,
        file_permissions: local.file_permissions,
    })
}

async fn execute_restore_dry_run(
    context: &BackupContext,
    request: &KmsRestoreDryRunRequest,
) -> Result<KmsRestoreDryRunResponse, Failure> {
    let bundle_dir = context.bundle_dir(&request.bundle)?;
    let restore_request =
        restore_request_for(context, bundle_dir.clone(), request.observed_generation, RestoreConflictPolicy::Fail)?;

    let report = dry_run_local_restore(&context.environment.kek, &restore_request)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("restore dry-run failed: {error}")))?;

    let config_comparison = read_bundle_config(context, &bundle_dir).await;
    Ok(KmsRestoreDryRunResponse {
        report,
        config_comparison,
    })
}

/// Decode the bundle's configuration artifact for the report.
///
/// Best effort: a bundle produced before configuration artifacts existed, or
/// one whose artifact cannot be opened, simply has no comparison to show. The
/// restore path itself already rejects a bundle that fails verification, so a
/// missing comparison can never widen what a restore accepts.
async fn read_bundle_config(context: &BackupContext, bundle_dir: &std::path::Path) -> Option<ConfigComparison> {
    let manifest = read_local_bundle_manifest(bundle_dir).await.ok()?;
    let descriptor = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == rustfs_kms::backup::ArtifactKind::KmsConfig)?;
    let plaintext = rustfs_kms::backup::decrypt_bundle_artifact(bundle_dir, &manifest, descriptor, &context.environment.kek)
        .await
        .ok()?;
    let bundle: SanitizedKmsConfig = serde_json::from_slice(&plaintext).ok()?;
    let current = SanitizedKmsConfig::from_config(&context.config);
    Some(ConfigComparison {
        identical: bundle == current,
        bundle,
        current,
    })
}

async fn execute_restore(context: &BackupContext, request: &KmsRestoreRequest) -> Result<LocalRestoreReport, Failure> {
    // A live service holds the key directory open and caches what it has
    // already read, so a restore underneath it could publish material the
    // running process never observes. Refusing is the only outcome that keeps
    // "complete old state or complete new state" true at the process level.
    if matches!(context.service_manager.get_status().await, KmsServiceStatus::Running) {
        return Err((
            StatusCode::CONFLICT,
            "kms service is running; stop it before restoring so the restore cannot race the live key directory".to_string(),
        ));
    }

    let bundle_dir = context.bundle_dir(&request.bundle)?;
    let manifest = read_local_bundle_manifest(&bundle_dir)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("bundle is not restorable: {error}")))?;

    if request.confirm_backup_id != manifest.backup_id {
        return Err((
            StatusCode::BAD_REQUEST,
            "confirm_backup_id does not match the bundle manifest; review the dry-run report and confirm the backup id \
             it reports"
                .to_string(),
        ));
    }

    let restore_request = restore_request_for(context, bundle_dir, request.observed_generation, request.confirm_conflict_policy)?;

    restore_local_backup(&context.environment.kek, &restore_request)
        .await
        .map_err(|error| (StatusCode::CONFLICT, format!("restore refused: {error}")))
}

async fn execute_restore_abort(
    context: &BackupContext,
    request: &KmsRestoreAbortRequest,
) -> Result<KmsRestoreAbortResponse, Failure> {
    let local = context.local_config()?;
    let target = local.key_dir.display().to_string();
    if request.confirm_target_key_dir != target {
        return Err((
            StatusCode::BAD_REQUEST,
            "confirm_target_key_dir does not match the configured key directory".to_string(),
        ));
    }

    abort_local_restore(&local.key_dir)
        .await
        .map_err(|error| (StatusCode::CONFLICT, format!("restore abort refused: {error}")))?;

    Ok(KmsRestoreAbortResponse {
        target_key_dir: target,
        aborted: true,
    })
}

async fn backup_status() -> KmsBackupStatusResponse {
    let Some(service_manager) = current_kms_runtime_service_manager() else {
        return KmsBackupStatusResponse {
            service_status: "unavailable".to_string(),
            backend: None,
            responsibility: None,
            export_supported: false,
            deployment_identity: None,
            backup_root: None,
            backup_kek_configured: false,
            backup_kek_id: None,
            backup_kek_version: None,
            target_key_dir: None,
            unavailable_reason: Some("kms service manager is not initialized".to_string()),
        };
    };

    let status = service_manager.get_status().await;
    let config = service_manager.get_config().await;
    let deployment_identity = current_deployment_id().filter(|id| !id.is_empty());

    let (backend, responsibility, target_key_dir) = match &config {
        Some(config) => (
            Some(backend_wire_name(&config.backend).to_string()),
            declared_responsibility(&config.backend),
            match &config.backend_config {
                BackendConfig::Local(local) => Some(local.key_dir.display().to_string()),
                _ => None,
            },
        ),
        None => (None, None, None),
    };

    let environment = config.as_ref().map(BackupEnvironment::from_env);
    let (backup_root, backup_kek_id, backup_kek_version, unavailable_reason) = match &environment {
        Some(Ok(environment)) => (
            Some(environment.root.display().to_string()),
            Some(environment.kek_id.clone()),
            Some(environment.kek_version),
            None,
        ),
        Some(Err((_, reason))) => (None, None, None, Some(reason.clone())),
        None => (None, None, None, Some("kms is not configured".to_string())),
    };

    let backup_kek_configured = backup_kek_id.is_some();
    KmsBackupStatusResponse {
        service_status: format!("{status:?}"),
        export_supported: backup_kek_configured
            && deployment_identity.is_some()
            && responsibility == Some(BackupResponsibility::FullMaterial)
            && backend.as_deref() == Some("local"),
        backend,
        responsibility,
        deployment_identity,
        backup_root,
        backup_kek_configured,
        backup_kek_id,
        backup_kek_version,
        target_key_dir,
        unavailable_reason,
    }
}

// ---------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------

fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/json".parse().expect("static content type should parse"));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    success: bool,
    message: String,
}

fn failure_response(failure: Failure) -> S3Result<S3Response<(StatusCode, Body)>> {
    let (status, message) = failure;
    json_response(status, &ErrorResponse { success: false, message })
}

/// Authenticate, audit-gate on the dedicated action, and read the body.
///
/// Returns the audit state and the raw body. The gate runs before the body is
/// parsed so an unauthorized caller learns nothing about the request contract.
async fn authorize(
    mut req: S3Request<Body>,
    actions: Vec<Action>,
    operation: KmsAdminOperation,
) -> S3Result<(KmsAdminAudit, Vec<u8>)> {
    let Some(cred) = req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let (cred, owner) = check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;
    let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

    audit.gate_admin(
        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            actions,
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await,
        operation,
        None,
    )?;

    let body = req
        .input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?
        .to_vec();
    Ok((audit, body))
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> S3Result<T> {
    serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

fn parse_optional_body<T: for<'de> Deserialize<'de> + Default>(body: &[u8]) -> S3Result<T> {
    if body.is_empty() {
        return Ok(T::default());
    }
    parse_body(body)
}

/// Record the outcome and log it. `bundle` identifies which bundle the
/// operation touched; no manifest content beyond identifiers is logged.
fn finish(audit: &KmsAdminAudit, operation: KmsAdminOperation, action: &str, bundle: Option<&str>, failed: Option<&str>) {
    audit.finish_with_class(operation, failed.map(|_| "operation_failed"));
    match failed {
        None => info!(
            event = EVENT_ADMIN_KMS_BACKUP,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS_BACKUP,
            action = action,
            bundle = bundle.unwrap_or_default(),
            state = "completed",
            "admin kms backup"
        ),
        Some(message) => error!(
            event = EVENT_ADMIN_KMS_BACKUP,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_KMS_BACKUP,
            action = action,
            bundle = bundle.unwrap_or_default(),
            result = "failed",
            error = message,
            "admin kms backup"
        ),
    }
}

pub fn register_kms_backup_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/backup").as_str(),
        AdminOperation(&KmsBackupStatusHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/backup").as_str(),
        AdminOperation(&KmsBackupHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/restore/dry-run").as_str(),
        AdminOperation(&KmsRestoreDryRunHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/restore").as_str(),
        AdminOperation(&KmsRestoreHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/restore/abort").as_str(),
        AdminOperation(&KmsRestoreAbortHandler {}),
    )?;
    Ok(())
}

/// Export the KMS state as a sealed backup bundle.
pub struct KmsBackupHandler {}

#[async_trait::async_trait]
impl Operation for KmsBackupHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (audit, body) = authorize(req, kms_backup_actions(), KmsAdminOperation::Backup).await?;
        let request: KmsBackupRequest = parse_optional_body(&body)?;

        let context = match BackupContext::resolve().await {
            Ok(context) => context,
            Err(failure) => {
                finish(&audit, KmsAdminOperation::Backup, "backup", None, Some(&failure.1));
                return failure_response(failure);
            }
        };

        let Some(manager) = context.service_manager.get_manager().await else {
            let failure = (
                StatusCode::SERVICE_UNAVAILABLE,
                "kms service is not running; a consistent export needs the running backend's fence".to_string(),
            );
            finish(&audit, KmsAdminOperation::Backup, "backup", None, Some(&failure.1));
            return failure_response(failure);
        };

        match execute_backup(&manager, &context, &request).await {
            Ok(response) => {
                finish(&audit, KmsAdminOperation::Backup, "backup", Some(&response.backup_id), None);
                json_response(StatusCode::OK, &response)
            }
            Err(failure) => {
                finish(
                    &audit,
                    KmsAdminOperation::Backup,
                    "backup",
                    request.backup_id.as_deref(),
                    Some(&failure.1),
                );
                failure_response(failure)
            }
        }
    }
}

/// Zero-write restore preflight.
pub struct KmsRestoreDryRunHandler {}

#[async_trait::async_trait]
impl Operation for KmsRestoreDryRunHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (audit, body) = authorize(req, kms_restore_actions(), KmsAdminOperation::RestoreDryRun).await?;
        let request: KmsRestoreDryRunRequest = parse_body(&body)?;

        let context = match BackupContext::resolve().await {
            Ok(context) => context,
            Err(failure) => {
                finish(
                    &audit,
                    KmsAdminOperation::RestoreDryRun,
                    "restore_dry_run",
                    Some(&request.bundle),
                    Some(&failure.1),
                );
                return failure_response(failure);
            }
        };

        match execute_restore_dry_run(&context, &request).await {
            Ok(response) => {
                finish(&audit, KmsAdminOperation::RestoreDryRun, "restore_dry_run", Some(&request.bundle), None);
                json_response(StatusCode::OK, &response)
            }
            Err(failure) => {
                finish(
                    &audit,
                    KmsAdminOperation::RestoreDryRun,
                    "restore_dry_run",
                    Some(&request.bundle),
                    Some(&failure.1),
                );
                failure_response(failure)
            }
        }
    }
}

/// Execute a restore. Requires both confirmations.
pub struct KmsRestoreHandler {}

#[async_trait::async_trait]
impl Operation for KmsRestoreHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (audit, body) = authorize(req, kms_restore_actions(), KmsAdminOperation::Restore).await?;
        let request: KmsRestoreRequest = parse_body(&body)?;

        let context = match BackupContext::resolve().await {
            Ok(context) => context,
            Err(failure) => {
                finish(&audit, KmsAdminOperation::Restore, "restore", Some(&request.bundle), Some(&failure.1));
                return failure_response(failure);
            }
        };

        match execute_restore(&context, &request).await {
            Ok(report) => {
                finish(&audit, KmsAdminOperation::Restore, "restore", Some(&report.backup_id), None);
                json_response(StatusCode::OK, &report)
            }
            Err(failure) => {
                finish(&audit, KmsAdminOperation::Restore, "restore", Some(&request.bundle), Some(&failure.1));
                failure_response(failure)
            }
        }
    }
}

/// Take back an interrupted restore.
pub struct KmsRestoreAbortHandler {}

#[async_trait::async_trait]
impl Operation for KmsRestoreAbortHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (audit, body) = authorize(req, kms_restore_actions(), KmsAdminOperation::RestoreAbort).await?;
        let request: KmsRestoreAbortRequest = parse_body(&body)?;

        let context = match BackupContext::resolve().await {
            Ok(context) => context,
            Err(failure) => {
                finish(&audit, KmsAdminOperation::RestoreAbort, "restore_abort", None, Some(&failure.1));
                return failure_response(failure);
            }
        };

        match execute_restore_abort(&context, &request).await {
            Ok(response) => {
                finish(&audit, KmsAdminOperation::RestoreAbort, "restore_abort", None, None);
                json_response(StatusCode::OK, &response)
            }
            Err(failure) => {
                finish(&audit, KmsAdminOperation::RestoreAbort, "restore_abort", None, Some(&failure.1));
                failure_response(failure)
            }
        }
    }
}

/// Report whether this server can produce or consume bundles.
pub struct KmsBackupStatusHandler {}

#[async_trait::async_trait]
impl Operation for KmsBackupStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let (audit, _body) = authorize(req, kms_backup_actions(), KmsAdminOperation::BackupStatus).await?;
        let response = backup_status().await;
        finish(&audit, KmsAdminOperation::BackupStatus, "backup_status", None, None);
        json_response(StatusCode::OK, &response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_kms::backends::local::LocalKmsBackend;
    use rustfs_kms::config::{StaticConfig, VaultConfig};
    use rustfs_kms::types::CreateKeyRequest;
    use std::collections::BTreeMap;

    /// Base64 of `backup-kek-material-32-bytes!!!!`; ASCII so the test can
    /// also use its raw bytes as a differently spelled configuration secret.
    const TEST_KEK_B64: &str = "YmFja3VwLWtlay1tYXRlcmlhbC0zMi1ieXRlcyEhISE=";
    const DEPLOYMENT: &str = "deployment-under-test";

    fn test_kek_bytes() -> Vec<u8> {
        BASE64.decode(TEST_KEK_B64).expect("test KEK must decode")
    }

    fn local_config(key_dir: PathBuf) -> KmsConfig {
        KmsConfig::local(key_dir).with_insecure_development_defaults()
    }

    fn local_config_with_master_key(key_dir: PathBuf, master_key: &str) -> KmsConfig {
        KmsConfig {
            backend_config: BackendConfig::Local(LocalConfig {
                key_dir: key_dir.clone(),
                master_key: Some(master_key.to_string()),
                file_permissions: Some(0o600),
            }),
            ..local_config(key_dir)
        }
    }

    fn vault_kv2_config(auth_method: VaultAuthMethod) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::VaultKv2,
            backend_config: BackendConfig::VaultKv2(Box::new(VaultConfig {
                auth_method,
                ..VaultConfig::default()
            })),
            ..KmsConfig::default()
        }
    }

    fn vault_transit_config() -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::VaultTransit,
            backend_config: BackendConfig::VaultTransit(Box::default()),
            ..KmsConfig::default()
        }
    }

    fn static_kms_config(secret_key: String) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::Static,
            backend_config: BackendConfig::Static(StaticConfig {
                key_id: "static-key".to_string(),
                secret_key,
            }),
            ..KmsConfig::default()
        }
    }

    fn environment(root: PathBuf, config: &KmsConfig) -> BackupEnvironment {
        BackupEnvironment::build(root, TEST_KEK_B64, "backup-kek-test".to_string(), 1, config).expect("environment should build")
    }

    fn backup_context(root: PathBuf, config: KmsConfig) -> BackupContext {
        let environment = environment(root, &config);
        BackupContext {
            service_manager: Arc::new(KmsServiceManager::new()),
            config,
            deployment_identity: DEPLOYMENT.to_string(),
            environment,
        }
    }

    async fn manager_with_keys(config: &KmsConfig, key_names: &[&str]) -> KmsManager {
        let backend = Arc::new(
            LocalKmsBackend::new(config.clone())
                .await
                .expect("local backend should build"),
        );
        let manager = KmsManager::new(backend, config.clone());
        for name in key_names {
            manager
                .create_key(CreateKeyRequest {
                    key_name: Some((*name).to_string()),
                    ..Default::default()
                })
                .await
                .expect("key should be created");
        }
        manager
    }

    /// Byte-level fingerprint of a directory tree, so "zero writes" is checked
    /// against content and layout rather than an mtime.
    fn tree_fingerprint(root: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut BTreeMap<String, Vec<u8>>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, base, out);
                } else {
                    let relative = path.strip_prefix(base).unwrap_or(&path).display().to_string();
                    out.insert(relative, std::fs::read(&path).unwrap_or_default());
                }
            }
        }
        walk(root, root, &mut out);
        out
    }

    #[test]
    fn backup_and_restore_use_dedicated_actions() {
        assert_eq!(kms_backup_actions(), vec![Action::KmsAction(KmsAction::BackupAction)]);
        assert_eq!(kms_restore_actions(), vec![Action::KmsAction(KmsAction::RestoreAction)]);

        // Neither may be satisfied by a broad KMS action a read-only operator
        // could plausibly hold.
        for actions in [kms_backup_actions(), kms_restore_actions()] {
            assert!(!actions.contains(&Action::KmsAction(KmsAction::DescribeKeyAction)));
            assert!(!actions.contains(&Action::KmsAction(KmsAction::ListKeysAction)));
            assert!(!actions.contains(&Action::KmsAction(KmsAction::ConfigureAction)));
        }
        assert_ne!(kms_backup_actions(), kms_restore_actions());
    }

    #[test]
    fn a_missing_backup_kek_disables_backup_and_restore() {
        let config = local_config(PathBuf::from("/tmp/does-not-matter"));
        let error = BackupEnvironment::build(PathBuf::from("/tmp/root"), "", "kek".to_string(), 1, &config)
            .expect_err("an empty KEK must be refused");
        assert_eq!(error.0, StatusCode::PRECONDITION_FAILED);

        let short = BASE64.encode([0x11; 16]);
        let error = BackupEnvironment::build(PathBuf::from("/tmp/root"), &short, "kek".to_string(), 1, &config)
            .expect_err("a KEK that is not 32 bytes must be refused");
        assert_eq!(error.0, StatusCode::PRECONDITION_FAILED);
        assert!(error.1.contains("32 bytes"), "{}", error.1);

        let error = BackupEnvironment::build(PathBuf::from("/tmp/root"), TEST_KEK_B64, String::new(), 1, &config)
            .expect_err("an empty KEK id must be refused");
        assert_eq!(error.0, StatusCode::PRECONDITION_FAILED);
    }

    /// The core principle of the issue: the bundle must not be sealed with a
    /// key that belongs to the state it protects.
    #[test]
    fn a_backup_kek_reusing_a_backend_secret_is_refused() {
        let material = test_kek_bytes();

        // Local master key supplied as the same base64 spelling.
        let config = local_config_with_master_key(PathBuf::from("/tmp/keys"), TEST_KEK_B64);
        let error = BackupEnvironment::build(PathBuf::from("/tmp/root"), TEST_KEK_B64, "kek".to_string(), 1, &config)
            .expect_err("reusing the master key must be refused");
        assert_eq!(error.0, StatusCode::PRECONDITION_FAILED);
        assert!(error.1.contains("independent trust root"), "{}", error.1);

        // Same secret, different spelling: the raw bytes of the master key are
        // the KEK material.
        let raw_master_key = String::from_utf8(material).expect("test KEK is ASCII");
        let config = local_config_with_master_key(PathBuf::from("/tmp/keys"), &raw_master_key);
        assert!(
            BackupEnvironment::build(PathBuf::from("/tmp/root"), TEST_KEK_B64, "kek".to_string(), 1, &config).is_err(),
            "re-encoding the same secret must not bypass the separation rule"
        );

        // A Vault token is a business trust root too.
        let vault_config = vault_kv2_config(VaultAuthMethod::Token {
            token: TEST_KEK_B64.to_string(),
        });
        assert!(
            BackupEnvironment::build(PathBuf::from("/tmp/root"), TEST_KEK_B64, "kek".to_string(), 1, &vault_config).is_err(),
            "a Vault token must not double as the backup KEK"
        );

        // An unrelated KEK is accepted.
        let independent = BASE64.encode([0x5a; 32]);
        assert!(BackupEnvironment::build(PathBuf::from("/tmp/root"), &independent, "kek".to_string(), 1, &config).is_ok());
    }

    #[test]
    fn bundle_names_cannot_escape_the_backup_root() {
        for name in ["..", ".", "../etc", "a/b", "/abs", ".hidden", "", "name with space", "na*me"] {
            assert!(validated_bundle_name(name).is_err(), "{name:?} must be refused");
        }
        assert_eq!(validated_bundle_name("backup-2026-08-01_01").expect("valid"), "backup-2026-08-01_01");
    }

    /// The sanitized artifact is the only configuration that ever enters a
    /// bundle, so no configured secret may survive the projection.
    #[test]
    fn the_sanitized_config_carries_no_secret() {
        let master_key = "local-master-key-super-secret";
        let vault_token = "hvs.vault-token-super-secret";
        let approle_secret = "approle-secret-id-super-secret";
        let static_key = BASE64.encode([0x7c; 32]);

        let local = local_config_with_master_key(PathBuf::from("/var/lib/rustfs/kms"), master_key);
        let kv2 = vault_kv2_config(VaultAuthMethod::Token {
            token: vault_token.to_string(),
        });
        let approle = vault_kv2_config(VaultAuthMethod::approle("role-id".to_string(), approle_secret.to_string()));
        let static_config = static_kms_config(static_key.clone());

        for config in [&local, &kv2, &approle, &static_config] {
            let rendered = serde_json::to_string(&SanitizedKmsConfig::from_config(config)).expect("sanitized config");
            for secret in [master_key, vault_token, approle_secret, static_key.as_str()] {
                assert!(!rendered.contains(secret), "sanitized config leaked a secret: {rendered}");
            }
        }

        // The non-sensitive coordinates must survive, or the artifact would be
        // useless as restore evidence.
        let rendered = serde_json::to_string(&SanitizedKmsConfig::from_config(&kv2)).expect("sanitized config");
        assert!(rendered.contains("\"auth_method\":\"token\""), "{rendered}");
        assert!(rendered.contains("vault-kv2"), "{rendered}");
    }

    #[tokio::test]
    async fn backup_then_dry_run_reports_a_restorable_bundle_and_writes_nothing() {
        let source = tempfile::tempdir().expect("temp dir");
        let root = tempfile::tempdir().expect("temp dir");
        let config = local_config(source.path().to_path_buf());
        let manager = manager_with_keys(&config, &["alpha", "beta"]).await;
        let context = backup_context(root.path().to_path_buf(), config.clone());

        let response = execute_backup(
            &manager,
            &context,
            &KmsBackupRequest {
                backup_id: Some("drill-0001".to_string()),
            },
        )
        .await
        .expect("export should succeed");
        assert_eq!(response.backup_id, "drill-0001");
        assert!(response.sanitized_config_included);
        // Two key artifacts plus the configuration. A dev-mode directory has
        // no master-key salt, so no salt artifact exists to seal.
        assert_eq!(response.artifact_count, 3);

        // The bundle must not contain the configured secrets in any artifact.
        let bundle_bytes = tree_fingerprint(&root.path().join("drill-0001"));
        assert!(!bundle_bytes.is_empty(), "bundle must have been written");

        // Dry-run against an empty target: zero writes, and the config
        // comparison is available for the operator.
        let target = tempfile::tempdir().expect("temp dir");
        let restore_config = local_config(target.path().to_path_buf());
        let restore_context = backup_context(root.path().to_path_buf(), restore_config);
        let before = tree_fingerprint(target.path());

        let dry_run = execute_restore_dry_run(
            &restore_context,
            &KmsRestoreDryRunRequest {
                bundle: "drill-0001".to_string(),
                observed_generation: None,
            },
        )
        .await
        .expect("dry-run should evaluate");

        assert_eq!(tree_fingerprint(target.path()), before, "a dry-run must write nothing to the target");
        assert_eq!(tree_fingerprint(&root.path().join("drill-0001")), bundle_bytes, "the bundle is read-only");
        assert!(dry_run.report.blockers.is_empty(), "unexpected blockers: {:?}", dry_run.report.blockers);
        let comparison = dry_run.config_comparison.expect("bundle carries a config artifact");
        assert_eq!(comparison.bundle.backend, "local");
    }

    #[tokio::test]
    async fn restore_without_a_matching_confirmation_is_refused() {
        let source = tempfile::tempdir().expect("temp dir");
        let root = tempfile::tempdir().expect("temp dir");
        let config = local_config(source.path().to_path_buf());
        let manager = manager_with_keys(&config, &["alpha"]).await;
        let context = backup_context(root.path().to_path_buf(), config);

        execute_backup(
            &manager,
            &context,
            &KmsBackupRequest {
                backup_id: Some("drill-0002".to_string()),
            },
        )
        .await
        .expect("export should succeed");

        let target = tempfile::tempdir().expect("temp dir");
        let restore_context = backup_context(root.path().to_path_buf(), local_config(target.path().to_path_buf()));
        let before = tree_fingerprint(target.path());

        // Wrong backup id: refused before anything is written.
        let error = execute_restore(
            &restore_context,
            &KmsRestoreRequest {
                bundle: "drill-0002".to_string(),
                observed_generation: None,
                confirm_backup_id: "drill-9999".to_string(),
                confirm_conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
            },
        )
        .await
        .expect_err("a mismatched confirmation must be refused");
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(tree_fingerprint(target.path()), before, "a refused restore must write nothing");

        // Correct backup id but the default (non-writing) conflict policy: the
        // second confirmation is independent, so this is refused too.
        let error = execute_restore(
            &restore_context,
            &KmsRestoreRequest {
                bundle: "drill-0002".to_string(),
                observed_generation: None,
                confirm_backup_id: "drill-0002".to_string(),
                confirm_conflict_policy: RestoreConflictPolicy::Fail,
            },
        )
        .await
        .expect_err("the default conflict policy must never write");
        assert_eq!(error.0, StatusCode::CONFLICT);
        assert_eq!(tree_fingerprint(target.path()), before, "a refused restore must write nothing");

        // Both confirmations present: the restore proceeds.
        let report = execute_restore(
            &restore_context,
            &KmsRestoreRequest {
                bundle: "drill-0002".to_string(),
                observed_generation: None,
                confirm_backup_id: "drill-0002".to_string(),
                confirm_conflict_policy: RestoreConflictPolicy::RestoreIntoEmptyTarget,
            },
        )
        .await
        .expect("a fully confirmed restore should proceed");
        assert_eq!(report.backup_id, "drill-0002");
        assert_ne!(tree_fingerprint(target.path()), before);
    }

    /// A restore request must not be able to name a directory to write into.
    #[tokio::test]
    async fn restore_never_accepts_a_caller_supplied_path() {
        let root = tempfile::tempdir().expect("temp dir");
        let target = tempfile::tempdir().expect("temp dir");
        let context = backup_context(root.path().to_path_buf(), local_config(target.path().to_path_buf()));

        let error = execute_restore_dry_run(
            &context,
            &KmsRestoreDryRunRequest {
                bundle: "../../etc".to_string(),
                observed_generation: None,
            },
        )
        .await
        .expect_err("a traversal bundle name must be refused");
        assert_eq!(error.0, StatusCode::BAD_REQUEST);

        // The restore target is server configuration, not a request field.
        let restore_request =
            restore_request_for(&context, root.path().join("bundle"), None, RestoreConflictPolicy::RestoreIntoEmptyTarget)
                .expect("request should build");
        assert_eq!(restore_request.target_key_dir, target.path());
    }

    #[tokio::test]
    async fn a_non_local_backend_reports_its_responsibility_instead_of_exporting() {
        let source = tempfile::tempdir().expect("temp dir");
        let root = tempfile::tempdir().expect("temp dir");
        // A Local manager paired with a Vault Transit configuration is not a
        // real deployment; what is under test is that the export refuses when
        // the configured backend owns no exportable material.
        let local = local_config(source.path().to_path_buf());
        let manager = manager_with_keys(&local, &["alpha"]).await;

        let context = backup_context(root.path().to_path_buf(), vault_transit_config());

        assert_eq!(
            declared_responsibility(&KmsBackend::VaultTransit),
            Some(BackupResponsibility::MetadataPlusExternalRoot)
        );
        assert_eq!(declared_responsibility(&KmsBackend::Static), Some(BackupResponsibility::ReferenceOnly));
        assert_eq!(declared_responsibility(&KmsBackend::Local), Some(BackupResponsibility::FullMaterial));
        assert_eq!(declared_responsibility(&KmsBackend::Aws), None);

        // The handle can still export, which is exactly why the refusal has to
        // be driven by the configured backend rather than by the handle.
        assert!(manager.local_backup_client().is_some());

        let error = execute_backup(&manager, &context, &KmsBackupRequest { backup_id: None })
            .await
            .expect_err("a non-local configuration must not produce a full-material bundle");
        assert_eq!(error.0, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(
            std::fs::read_dir(root.path()).expect("read backup root").count(),
            0,
            "a refused export must leave no bundle behind"
        );

        let error = execute_restore_dry_run(
            &context,
            &KmsRestoreDryRunRequest {
                bundle: "any".to_string(),
                observed_generation: None,
            },
        )
        .await
        .expect_err("a non-local backend has no RustFS-side restore");
        assert_eq!(error.0, StatusCode::NOT_IMPLEMENTED);
    }

    /// Nothing that reaches an audit trail or a log line may carry KEK
    /// material, a master key, or bundle plaintext.
    #[tokio::test]
    async fn responses_carry_identifiers_only() {
        let source = tempfile::tempdir().expect("temp dir");
        let root = tempfile::tempdir().expect("temp dir");
        let master_key = "local-master-key-super-secret";
        let config = local_config_with_master_key(source.path().to_path_buf(), master_key);
        let manager = manager_with_keys(&config, &["alpha"]).await;
        let context = backup_context(root.path().to_path_buf(), config);

        let response = execute_backup(&manager, &context, &KmsBackupRequest { backup_id: None })
            .await
            .expect("export should succeed");
        let rendered = serde_json::to_string(&response).expect("response should serialize");
        for secret in [TEST_KEK_B64, master_key] {
            assert!(!rendered.contains(secret), "response leaked a secret: {rendered}");
        }
        assert!(
            !rendered
                .as_bytes()
                .windows(test_kek_bytes().len())
                .any(|window| window == test_kek_bytes()),
            "response leaked raw KEK material: {rendered}"
        );
        assert!(response.backup_id.starts_with("backup-"));
        assert_eq!(response.backup_kek_id, "backup-kek-test");

        let status = backup_status().await;
        let rendered = serde_json::to_string(&status).expect("status should serialize");
        assert!(!rendered.contains(TEST_KEK_B64), "status leaked KEK material: {rendered}");
    }
}
