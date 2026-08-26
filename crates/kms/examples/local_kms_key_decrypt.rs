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

use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_kms::{LocalConfig, backends::local::LocalKmsClient};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

const LOCAL_KMS_MASTER_KEY_ENV: &str = "RUSTFS_KMS_LOCAL_MASTER_KEY";

fn usage(program: &str) -> String {
    format!(
        "Usage: {program} <local-kms-key-file>\n\
         Reads {LOCAL_KMS_MASTER_KEY_ENV} when the key file is encrypted.\n\
         Writes only the base64-encoded 32-byte key to stdout."
    )
}

fn resolve_key_file(path: &Path) -> Result<(PathBuf, String), String> {
    let canonical = std::fs::canonicalize(path).map_err(|error| format!("cannot open Local KMS key file: {error}"))?;
    if canonical.extension().and_then(|extension| extension.to_str()) != Some("key") {
        return Err("Local KMS key file must have a .key extension".to_string());
    }
    let key_dir = canonical
        .parent()
        .ok_or_else(|| "Local KMS key file must have a parent directory".to_string())?
        .to_path_buf();
    let key_id = canonical
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .ok_or_else(|| "Local KMS key file name must contain a valid UTF-8 key ID".to_string())?
        .to_string();
    Ok((key_dir, key_id))
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args();
    let program = args.next().unwrap_or_else(|| "local_kms_key_decrypt".to_string());
    let Some(key_file) = args.next() else {
        return Err(usage(&program));
    };
    if args.next().is_some() {
        return Err(usage(&program));
    }

    let (key_dir, key_id) = resolve_key_file(Path::new(&key_file))?;
    let master_key = std::env::var(LOCAL_KMS_MASTER_KEY_ENV).ok().filter(|value| !value.is_empty());
    let client = LocalKmsClient::new_for_key_export(LocalConfig {
        key_dir,
        master_key,
        file_permissions: Some(0o600),
    })
    .await
    .map_err(|error| error.to_string())?;
    let key_material = client
        .decrypt_key_material_for_export(&key_id)
        .await
        .map_err(|error| error.to_string())?;
    let encoded = Zeroizing::new(BASE64_STANDARD.encode_to_string(key_material.as_ref()));

    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{}", encoded.as_str()).map_err(|error| format!("failed to write decrypted key: {error}"))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        let _ = writeln!(io::stderr().lock(), "local_kms_key_decrypt: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_key_file_extracts_directory_and_key_id() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let key_file = directory.path().join("migration-key.key");
        std::fs::write(&key_file, b"{}").expect("create key file");

        let (key_dir, key_id) = resolve_key_file(&key_file).expect("resolve key file");

        assert_eq!(key_dir, directory.path().canonicalize().expect("canonical directory"));
        assert_eq!(key_id, "migration-key");
    }

    #[test]
    fn resolve_key_file_rejects_non_key_extension() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let key_file = directory.path().join("migration-key.json");
        std::fs::write(&key_file, b"{}").expect("create key file");

        let error = resolve_key_file(&key_file).expect_err("non-key file must be rejected");

        assert!(error.contains(".key"));
    }
}
