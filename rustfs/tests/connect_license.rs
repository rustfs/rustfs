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

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use base64_simd::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer as _, SigningKey};
use rustfs::connect::{
    LICENSE_DOMAIN_SEPARATION_TAG, LicenseArtifactStatus, LicenseClaims, LicenseVerificationContext, apply_license_artifact,
    inspect_installed_license, verify_license_artifact,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

const ORGANIZATION: &str = "organizations/018cc251-f400-7000-8000-000000000003";
const DEPLOYMENT: &str = "organizations/018cc251-f400-7000-8000-000000000003/clusters/018cc251-f400-7000-8000-000000000004";
const OTHER_DEPLOYMENT: &str = "organizations/018cc251-f400-7000-8000-000000000003/clusters/018cc251-f400-7000-8000-000000000005";
const OTHER_ORGANIZATION: &str = "organizations/018cc251-f400-7000-8000-000000000006";
const OTHER_ORGANIZATION_DEPLOYMENT: &str =
    "organizations/018cc251-f400-7000-8000-000000000006/clusters/018cc251-f400-7000-8000-000000000007";
const ISSUER: &str = "test-connect-issuer";
const AUDIENCE: &str = "test-rustfs-cluster";
const SERVICE: &str = "SUPPORT";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FrozenVector {
    public_key: String,
    key_id: String,
    connect_payload: String,
    connect_artifact: String,
    current_artifact: String,
}

fn vector() -> FrozenVector {
    serde_json::from_str(include_str!("fixtures/connect-license-ed25519-vector.json"))
        .expect("frozen Connect license vector must decode")
}

fn public_key(vector: &FrozenVector) -> [u8; 32] {
    URL_SAFE_NO_PAD
        .decode_to_vec(vector.public_key.as_bytes())
        .expect("fixture public key must decode")
        .try_into()
        .expect("fixture public key must contain 32 bytes")
}

fn context(vector: &FrozenVector, now_unix: i64, deployment: &str) -> LicenseVerificationContext {
    LicenseVerificationContext::new(
        public_key(vector),
        vector.key_id.clone(),
        ISSUER.to_owned(),
        AUDIENCE.to_owned(),
        ORGANIZATION.to_owned(),
        deployment.to_owned(),
        SERVICE.to_owned(),
        now_unix,
    )
    .expect("fixture context must be valid")
}

fn write_artifact(directory: &Path, name: &str, artifact: &str) -> PathBuf {
    let path = directory.join(name);
    fs::write(&path, artifact).expect("write artifact fixture");
    path
}

#[test]
fn downloaded_and_offline_files_share_the_frozen_connect_verifier_and_store() {
    let vector = vector();
    let temporary = TempDir::new().expect("create temp directory");
    let artifact = write_artifact(temporary.path(), "license.json", &vector.connect_artifact);
    let evaluation_time = 1_894_665_600; // 2030-01-15T00:00:00Z
    let context = context(&vector, evaluation_time, DEPLOYMENT);

    let verified = verify_license_artifact(&artifact, &temporary.path().join("verify"), &context)
        .expect("frozen Connect artifact must verify");
    assert_eq!(verified.status, LicenseArtifactStatus::Valid);
    assert!(!verified.installed);
    assert_eq!(verified.license.as_ref().map(|claims| claims.sequence), Some(2));

    let downloaded = apply_license_artifact(&artifact, &temporary.path().join("downloaded"), &context)
        .expect("downloaded artifact must install");
    let offline = apply_license_artifact(&artifact, &temporary.path().join("offline"), &context)
        .expect("hand-carried artifact must install through the same function");
    assert_eq!(downloaded.license, offline.license);
    assert!(downloaded.installed && offline.installed);

    let shown = inspect_installed_license(&temporary.path().join("downloaded"), &context)
        .expect("installed license must remain verifiable");
    assert_eq!(shown.license, downloaded.license);
    assert_eq!(
        vector.connect_payload,
        serde_json::to_string(shown.license.as_ref().expect("claims")).unwrap()
    );
}

#[test]
fn tamper_foreign_scope_and_expiry_are_structured_and_preserve_installed_state() {
    let vector = vector();
    let temporary = TempDir::new().expect("create temp directory");
    let artifact = write_artifact(temporary.path(), "license.json", &vector.connect_artifact);
    let state = temporary.path().join("state");
    let valid = context(&vector, 1_894_665_600, DEPLOYMENT);
    apply_license_artifact(&artifact, &state, &valid).expect("install baseline license");

    let mut envelope: Value = serde_json::from_str(&vector.connect_artifact).expect("decode envelope");
    let payload = URL_SAFE_NO_PAD
        .decode_to_vec(envelope["payload"].as_str().expect("payload").as_bytes())
        .expect("decode payload");
    let mut claims: Value = serde_json::from_slice(&payload).expect("decode claims");
    claims["deployment"] = Value::String(OTHER_DEPLOYMENT.to_owned());
    envelope["payload"] = Value::String(URL_SAFE_NO_PAD.encode_to_string(serde_json::to_vec(&claims).unwrap()));
    let tampered = write_artifact(temporary.path(), "tampered.json", &serde_json::to_string(&envelope).unwrap());
    let error = apply_license_artifact(&tampered, &state, &valid).expect_err("tampered artifact must fail");
    assert_eq!(error.status, LicenseArtifactStatus::InvalidSignature);

    let foreign = context(&vector, 1_894_665_600, OTHER_DEPLOYMENT);
    let error = verify_license_artifact(&artifact, &temporary.path().join("foreign"), &foreign)
        .expect_err("valid signature for another deployment must fail");
    assert_eq!(error.status, LicenseArtifactStatus::WrongScope);

    let expired = context(&vector, 1_896_134_400, DEPLOYMENT); // 2030-02-01T00:00:00Z
    let error = apply_license_artifact(&artifact, &state, &expired).expect_err("expiry is exclusive");
    assert_eq!(error.status, LicenseArtifactStatus::Expired);
    assert_eq!(
        error.license.as_ref().map(|claims| claims.license_uid.as_str()),
        Some("018cc251-f400-7000-8000-000000000002")
    );

    let shown = inspect_installed_license(&state, &valid).expect("failed imports must preserve the installed license");
    assert_eq!(shown.license.as_ref().map(|claims| claims.sequence), Some(2));
}

#[test]
fn sequence_and_grant_supersession_fail_closed() {
    let signing_key = SigningKey::from_bytes(&[42; 32]);
    let verifying_key = signing_key.verifying_key().to_bytes();
    let key_id = hex_simd::encode_to_string(Sha256::digest(verifying_key), hex_simd::AsciiCase::Lower);
    let context = LicenseVerificationContext::new(
        verifying_key,
        key_id.clone(),
        ISSUER.to_owned(),
        AUDIENCE.to_owned(),
        ORGANIZATION.to_owned(),
        DEPLOYMENT.to_owned(),
        SERVICE.to_owned(),
        1_800_000_000,
    )
    .expect("test context must be valid");
    let temporary = TempDir::new().expect("create temp directory");
    let state = temporary.path().join("state");

    let sequence_two = signed_artifact(
        &signing_key,
        claims(&key_id, 2, "018cc251-f400-7000-8000-000000000002", "018cc251-f400-7000-8000-000000000001"),
    );
    let sequence_one = signed_artifact(
        &signing_key,
        claims(&key_id, 1, "018cc251-f400-7000-8000-000000000005", "018cc251-f400-7000-8000-000000000001"),
    );
    let conflicting_two = signed_artifact(
        &signing_key,
        claims(&key_id, 2, "018cc251-f400-7000-8000-000000000006", "018cc251-f400-7000-8000-000000000001"),
    );
    let sequence_three = signed_artifact(
        &signing_key,
        claims(&key_id, 3, "018cc251-f400-7000-8000-000000000007", "018cc251-f400-7000-8000-000000000001"),
    );
    let different_grant = signed_artifact(
        &signing_key,
        claims(&key_id, 4, "018cc251-f400-7000-8000-000000000009", "018cc251-f400-7000-8000-000000000008"),
    );

    let paths: Vec<PathBuf> = [sequence_two, sequence_one, conflicting_two, sequence_three, different_grant]
        .into_iter()
        .enumerate()
        .map(|(index, artifact)| write_artifact(temporary.path(), &format!("{index}.json"), &artifact))
        .collect();
    apply_license_artifact(&paths[0], &state, &context).expect("install sequence two");
    let repeat = apply_license_artifact(&paths[0], &state, &context).expect("exact repeat must be idempotent");
    assert!(repeat.idempotent);
    assert_eq!(
        apply_license_artifact(&paths[1], &state, &context).unwrap_err().status,
        LicenseArtifactStatus::Rollback
    );
    assert_eq!(
        apply_license_artifact(&paths[2], &state, &context).unwrap_err().status,
        LicenseArtifactStatus::SequenceConflict
    );
    apply_license_artifact(&paths[3], &state, &context).expect("higher sequence for the same grant must replace");
    assert_eq!(
        apply_license_artifact(&paths[4], &state, &context).unwrap_err().status,
        LicenseArtifactStatus::SupersessionRequired
    );
    assert_eq!(
        inspect_installed_license(&state, &context)
            .expect("installed sequence must survive rejected supersession")
            .license
            .map(|claims| claims.sequence),
        Some(3)
    );
}

#[test]
fn trust_identity_and_business_scope_are_all_pinned() {
    let vector = vector();
    let temporary = TempDir::new().expect("create temp directory");
    let artifact = write_artifact(temporary.path(), "license.json", &vector.connect_artifact);
    let mismatches = [
        ("other-issuer", AUDIENCE, ORGANIZATION, DEPLOYMENT, SERVICE),
        (ISSUER, "other-audience", ORGANIZATION, DEPLOYMENT, SERVICE),
        (ISSUER, AUDIENCE, OTHER_ORGANIZATION, OTHER_ORGANIZATION_DEPLOYMENT, SERVICE),
        (ISSUER, AUDIENCE, ORGANIZATION, OTHER_DEPLOYMENT, SERVICE),
        (ISSUER, AUDIENCE, ORGANIZATION, DEPLOYMENT, "OTHER_SERVICE"),
    ];
    for (issuer, audience, organization, deployment, service) in mismatches {
        let context = LicenseVerificationContext::new(
            public_key(&vector),
            vector.key_id.clone(),
            issuer.to_owned(),
            audience.to_owned(),
            organization.to_owned(),
            deployment.to_owned(),
            service.to_owned(),
            1_894_665_600,
        )
        .expect("mismatched local scope must still be well formed");
        assert_eq!(
            verify_license_artifact(&artifact, temporary.path(), &context)
                .expect_err("foreign identity or scope must fail")
                .status,
            LicenseArtifactStatus::WrongScope
        );
    }

    let foreign_key = SigningKey::from_bytes(&[7; 32]);
    let foreign_key_id =
        hex_simd::encode_to_string(Sha256::digest(foreign_key.verifying_key().to_bytes()), hex_simd::AsciiCase::Lower);
    let foreign_artifact = write_artifact(
        temporary.path(),
        "foreign-key.json",
        &signed_artifact(
            &foreign_key,
            claims(
                &foreign_key_id,
                2,
                "018cc251-f400-7000-8000-000000000008",
                "018cc251-f400-7000-8000-000000000009",
            ),
        ),
    );
    assert_eq!(
        verify_license_artifact(&foreign_artifact, temporary.path(), &context(&vector, 1_894_665_600, DEPLOYMENT))
            .expect_err("a valid signature from an untrusted key must fail")
            .status,
        LicenseArtifactStatus::UntrustedKey
    );
}

#[test]
fn rustfs_cli_runs_verify_import_and_show_with_structured_json() {
    let vector = vector();
    let temporary = TempDir::new().expect("create temp directory");
    let artifact = write_artifact(temporary.path(), "license.json", &vector.current_artifact);
    let public_key = write_artifact(temporary.path(), "license.pub", &vector.public_key);
    let state = temporary.path().join("state");

    let verify = cli(&artifact, &public_key, &state, &vector.key_id, "verify");
    assert!(verify.status.success(), "verify failed: {}", String::from_utf8_lossy(&verify.stderr));
    let verify_json: Value = serde_json::from_slice(&verify.stdout).expect("verify output must be JSON");
    assert_eq!(verify_json["status"], "VALID");
    assert_eq!(verify_json["installed"], false);

    let import = cli(&artifact, &public_key, &state, &vector.key_id, "import");
    assert!(import.status.success(), "import failed: {}", String::from_utf8_lossy(&import.stderr));
    let import_json: Value = serde_json::from_slice(&import.stdout).expect("import output must be JSON");
    assert_eq!(import_json["status"], "VALID");
    assert_eq!(import_json["installed"], true);

    let show = cli(&artifact, &public_key, &state, &vector.key_id, "show");
    assert!(show.status.success(), "show failed: {}", String::from_utf8_lossy(&show.stderr));
    let show_json: Value = serde_json::from_slice(&show.stdout).expect("show output must be JSON");
    assert_eq!(show_json["status"], "VALID");
    assert_eq!(show_json["license"]["serviceCode"], SERVICE);

    let foreign = cli_for_deployment(
        &artifact,
        &public_key,
        &temporary.path().join("foreign"),
        &vector.key_id,
        "verify",
        OTHER_DEPLOYMENT,
    );
    assert!(!foreign.status.success(), "foreign deployment must fail");
    let foreign_json: Value = serde_json::from_slice(&foreign.stdout).expect("failure output must be JSON");
    assert_eq!(foreign_json["status"], "WRONG_SCOPE");
    assert_eq!(foreign_json["installed"], false);
}

fn claims(key_id: &str, sequence: u64, license_uid: &str, grant_uid: &str) -> LicenseClaims {
    LicenseClaims {
        purpose: "RUSTFS_CONNECT_SERVICE_LICENSE".to_owned(),
        schema: "rustfs.connect.serviceLicense/1".to_owned(),
        algorithm: "Ed25519".to_owned(),
        key_id: key_id.to_owned(),
        license_uid: license_uid.to_owned(),
        grant_uid: grant_uid.to_owned(),
        sequence,
        issuer: ISSUER.to_owned(),
        audience: AUDIENCE.to_owned(),
        organization: ORGANIZATION.to_owned(),
        deployment: DEPLOYMENT.to_owned(),
        plan_code: "TEST_SUPPORT".to_owned(),
        policy_revision: "test-policy-1".to_owned(),
        service_code: SERVICE.to_owned(),
        issue_time: "2026-01-01T00:00:00Z".to_owned(),
        not_before: "2026-01-01T00:00:00Z".to_owned(),
        expire_time: "2099-01-01T00:00:00Z".to_owned(),
    }
}

fn signed_artifact(signing_key: &SigningKey, claims: LicenseClaims) -> String {
    let payload = serde_json::to_vec(&claims).expect("encode claims");
    let mut signed = Vec::from(LICENSE_DOMAIN_SEPARATION_TAG.as_bytes());
    signed.push(0);
    signed.extend_from_slice(&payload);
    json!({
        "payload": URL_SAFE_NO_PAD.encode_to_string(payload),
        "signature": URL_SAFE_NO_PAD.encode_to_string(signing_key.sign(&signed).to_bytes()),
    })
    .to_string()
}

fn cli(artifact: &Path, public_key: &Path, state: &Path, key_id: &str, operation: &str) -> std::process::Output {
    cli_for_deployment(artifact, public_key, state, key_id, operation, DEPLOYMENT)
}

fn cli_for_deployment(
    artifact: &Path,
    public_key: &Path,
    state: &Path,
    key_id: &str,
    operation: &str,
    deployment: &str,
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs-cli"));
    command.args(["connect", "license", operation]);
    if operation != "show" {
        command.arg("--artifact").arg(artifact);
    }
    command
        .arg("--state-dir")
        .arg(state)
        .arg("--public-key-file")
        .arg(public_key)
        .args([
            "--key-id",
            key_id,
            "--issuer",
            ISSUER,
            "--audience",
            AUDIENCE,
            "--organization",
            ORGANIZATION,
        ])
        .arg("--deployment")
        .arg(deployment)
        .args(["--service-code", SERVICE])
        .output()
        .expect("run rustfs-cli")
}
