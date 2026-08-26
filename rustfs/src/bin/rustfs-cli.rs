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

//! Offline and diagnostic RustFS command-line entry point.
//!
//! This binary shares RustFS's existing subcommand dispatcher and provides the
//! documented entry point for offline tooling such as `inspect bucket-meta`.

use std::fs;
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustfs::connect::offline::{
    BundleContext, BundleError, BundleReceipt, OfflineEnrollment, OfflineKeyStore, collect_offline_diagnostics,
    write_offline_bundle,
};
use tokio_util::sync::CancellationToken;

/// Owner read/write only. The response names the key being enrolled and the
/// challenge it answers; neither belongs to anyone else on the machine.
#[cfg(unix)]
const RESPONSE_MODE: u32 = 0o600;
const OFFLINE_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);

const USAGE: &str = "\
Usage: rustfs-cli connect offline enroll --challenge <path|-> --output <path> [--key-dir <path>]
       rustfs-cli connect offline bundle --state-dir <path> --device-name <name> --output <path> [--key-dir <path>]

Answers a Connect offline enrolment challenge without a network. Reads the
challenge from a file or from stdin when the path is `-`, verifies it against the
enrolment root compiled into this binary, mints the key being enrolled on first
use, and writes the signed response.

Builds a deterministic support bundle from the stopped runtime's persisted
inventory and bounded host diagnostics. The bundle command never uploads.

No secret is ever accepted on the command line.
";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();

    // Offline operations are handled before the server dispatcher so they can
    // never enter the networked server runtime. Bundle collection creates only
    // a current-thread runtime for its timeout and SIGINT cancellation.
    if matches!(
        arguments.first().map(String::as_str),
        Some("connect") if matches!(arguments.get(1).map(String::as_str), Some("offline"))
    ) {
        return match run_offline(&arguments[2..]) {
            Ok(()) => ExitCode::SUCCESS,
            Err(message) => {
                eprintln!("rustfs-cli: {message}");
                ExitCode::FAILURE
            }
        };
    }

    rustfs::startup_entrypoint::run_process();

    ExitCode::SUCCESS
}

fn run_offline(arguments: &[String]) -> Result<(), String> {
    match arguments.first().map(String::as_str) {
        Some("enroll") => enroll(&arguments[1..]),
        Some("bundle") => bundle(&arguments[1..]),
        Some(other) => Err(format!("unknown offline subcommand `{other}`\n\n{USAGE}")),
        None => Err(format!("missing offline subcommand\n\n{USAGE}")),
    }
}

fn bundle(arguments: &[String]) -> Result<(), String> {
    if !cfg!(target_os = "linux") {
        return Err(BundleError::UnsupportedPlatform.to_string());
    }

    let mut state_directory: Option<String> = None;
    let mut device_name: Option<String> = None;
    let mut output_path: Option<String> = None;
    let mut key_directory: Option<String> = None;

    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        let take_value = |name: &str| -> Result<String, String> {
            arguments
                .get(index + 1)
                .cloned()
                .ok_or_else(|| format!("`{name}` needs a value\n\n{USAGE}"))
        };

        match flag {
            "--state-dir" => state_directory = Some(take_value("--state-dir")?),
            "--device-name" => device_name = Some(take_value("--device-name")?),
            "--output" => output_path = Some(take_value("--output")?),
            "--key-dir" => key_directory = Some(take_value("--key-dir")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => return Err(format!("unknown option `{other}`\n\n{USAGE}")),
        }

        index += 2;
    }

    let state_directory = state_directory.ok_or_else(|| format!("`--state-dir` is required\n\n{USAGE}"))?;
    let device_name = device_name.ok_or_else(|| format!("`--device-name` is required\n\n{USAGE}"))?;
    let output_path = output_path.ok_or_else(|| format!("`--output` is required\n\n{USAGE}"))?;
    let key_directory = key_directory.unwrap_or_else(|| ".".to_owned());
    let key = OfflineKeyStore::new(&key_directory)
        .load()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "offline enrollment key is missing; run `connect offline enroll` first".to_owned())?;

    let cancel = CancellationToken::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|error| format!("cannot create the offline collector runtime: {error}"))?;
    let output = PathBuf::from(&output_path);
    let operation = async move {
        let signal = async {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| format!("cannot listen for SIGINT: {error}"))
        };
        tokio::pin!(signal);
        let diagnostics = tokio::select! {
            biased;
            signal = signal.as_mut() => {
                cancel.cancel();
                signal?;
                return Err("offline bundle production was cancelled".to_owned());
            }
            result = collect_offline_diagnostics(Path::new(&state_directory), &cancel) => {
                result.map_err(|error| error.to_string())?
            }
        };

        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "the system clock is before the Unix epoch".to_owned())?;
        let produced_at_unix =
            i64::try_from(elapsed.as_secs()).map_err(|_| "the system clock is outside the supported range".to_owned())?;
        let timestamp_millis =
            u64::try_from(elapsed.as_millis()).map_err(|_| "the system clock is outside the supported range".to_owned())?;
        let mut nonce = [0u8; 32];
        getrandom(&mut nonce)?;
        let mut uuid_random = [0u8; 10];
        getrandom(&mut uuid_random)?;
        let bundle_uid = uuid::Builder::from_unix_timestamp_millis(timestamp_millis, &uuid_random)
            .into_uuid()
            .to_string();
        let inventory_captured_at = diagnostics.inventory_captured_at;
        let inventory_age = diagnostics.inventory_age;
        let writer_cancel = cancel.clone();
        let writer = tokio::task::spawn_blocking(move || {
            write_offline_bundle(
                &output,
                &BundleContext {
                    bundle_uid,
                    device_name,
                    nonce,
                    produced_at_unix,
                },
                &diagnostics.entries,
                &key,
                &writer_cancel,
            )
        });
        let receipt = await_offline_writer(&cancel, signal.as_mut(), writer).await?;
        Ok((receipt, inventory_captured_at, inventory_age))
    };
    let (runtime, (receipt, inventory_captured_at, inventory_age)) = run_offline_runtime(runtime, operation)?;
    drop(runtime);

    println!("Bundle: {}", receipt.bundle_uid);
    println!("Device: {}", receipt.device_name);
    println!("Inventory: {} ({}s old)", inventory_captured_at, inventory_age.as_secs());
    println!("L0: {} entries, {} bytes", receipt.l0_count, receipt.l0_bytes);
    println!("L1: {} entries, {} bytes", receipt.l1_count, receipt.l1_bytes);
    println!(
        "Redaction: {} ({})",
        rustfs::connect::offline::redaction::REDACTION_VERSION,
        rustfs::connect::offline::redaction::RULESET_HASH
    );
    println!("Archive: {} bytes, sha256 {}", receipt.archive_size_bytes, receipt.archive_sha256);
    println!("Output: {output_path}");
    println!("Upload: not performed");

    Ok(())
}

async fn await_offline_writer<S>(
    cancel: &CancellationToken,
    mut signal: Pin<&mut S>,
    mut writer: tokio::task::JoinHandle<Result<BundleReceipt, BundleError>>,
) -> Result<BundleReceipt, String>
where
    S: Future<Output = Result<(), String>> + ?Sized,
{
    tokio::select! {
        biased;
        signal = signal.as_mut() => {
            cancel.cancel();
            let writer = finish_offline_writer(writer.await);
            match writer {
                Ok(receipt) => Ok(receipt),
                Err(error) => {
                    signal?;
                    Err(error)
                }
            }
        }
        result = &mut writer => finish_offline_writer(result),
    }
}

fn finish_offline_writer(
    result: Result<Result<BundleReceipt, BundleError>, tokio::task::JoinError>,
) -> Result<BundleReceipt, String> {
    result
        .map_err(|error| format!("offline bundle writer task failed: {error}"))?
        .map_err(|error| error.to_string())
}

fn run_offline_runtime<T>(
    runtime: tokio::runtime::Runtime,
    operation: impl Future<Output = Result<T, String>>,
) -> Result<(tokio::runtime::Runtime, T), String> {
    match runtime.block_on(operation) {
        Ok(value) => Ok((runtime, value)),
        Err(error) => {
            runtime.shutdown_timeout(OFFLINE_RUNTIME_SHUTDOWN_TIMEOUT);
            Err(error)
        }
    }
}

fn enroll(arguments: &[String]) -> Result<(), String> {
    let mut challenge_path: Option<String> = None;
    let mut output_path: Option<String> = None;
    let mut key_directory: Option<String> = None;

    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        let take_value = |name: &str| -> Result<String, String> {
            arguments
                .get(index + 1)
                .cloned()
                .ok_or_else(|| format!("`{name}` needs a value\n\n{USAGE}"))
        };

        match flag {
            "--challenge" => challenge_path = Some(take_value("--challenge")?),
            "--output" => output_path = Some(take_value("--output")?),
            "--key-dir" => key_directory = Some(take_value("--key-dir")?),
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => return Err(format!("unknown option `{other}`\n\n{USAGE}")),
        }

        index += 2;
    }

    let challenge_path = challenge_path.ok_or_else(|| format!("`--challenge` is required\n\n{USAGE}"))?;
    let output_path = output_path.ok_or_else(|| format!("`--output` is required\n\n{USAGE}"))?;
    let key_directory = key_directory.unwrap_or_else(|| ".".to_string());

    let challenge = read_challenge(&challenge_path)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "the system clock is before the Unix epoch".to_string())?
        .as_secs() as i64;
    #[cfg(feature = "offline-enrollment-e2e-root")]
    let now = enrollment_evaluation_time(now)?;

    #[cfg(feature = "offline-enrollment-e2e-root")]
    let verified = verify_enrollment_challenge(&challenge, now).map_err(|error| error.to_string())?;
    #[cfg(not(feature = "offline-enrollment-e2e-root"))]
    let verified = OfflineEnrollment::verify_challenge(&challenge, now).map_err(|error| error.to_string())?;

    // First use mints the key; a retry answers with the one already enrolled,
    // because the operator may already be carrying a response naming it.
    let key = OfflineKeyStore::new(&key_directory)
        .load_or_create()
        .map_err(|error| error.to_string())?;

    let mut device_nonce = [0u8; 32];
    getrandom(&mut device_nonce)?;

    let response = OfflineEnrollment::build_response(&verified, &key, &device_nonce, now).map_err(|error| error.to_string())?;

    write_response(Path::new(&output_path), &response)?;

    Ok(())
}

#[cfg(feature = "offline-enrollment-e2e-root")]
fn enrollment_evaluation_time(system_now: i64) -> Result<i64, String> {
    // The public fixture gate needs a compile-time clock; real E2E builds omit it.
    if env!("CARGO_BIN_NAME") == "rustfs-cli-e2e"
        && let Some(value) = option_env!("RUSTFS_E2E_OFFLINE_ENROLLMENT_FIXTURE_TIME")
    {
        return value
            .parse()
            .map_err(|_| "the compiled E2E enrollment fixture time is invalid".to_owned());
    }

    Ok(system_now)
}

#[cfg(feature = "offline-enrollment-e2e-root")]
fn verify_enrollment_challenge(
    challenge: &[u8],
    now: i64,
) -> Result<rustfs::connect::offline::VerifiedChallenge, rustfs::connect::offline::EnrollmentError> {
    if env!("CARGO_BIN_NAME") == "rustfs-cli-e2e" {
        return OfflineEnrollment::verify_e2e_challenge(challenge, now);
    }

    OfflineEnrollment::verify_challenge(challenge, now)
}

/// Reads the challenge from a file, or from stdin when the path is `-`.
///
/// A challenge is not a secret — it is signed, public, and carried in by hand —
/// so accepting a path is safe. The response's key never arrives this way.
fn read_challenge(path: &str) -> Result<Vec<u8>, String> {
    if path == "-" {
        let mut buffer = Vec::new();
        std::io::stdin()
            .read_to_end(&mut buffer)
            .map_err(|error| format!("cannot read the challenge from stdin: {error}"))?;
        return Ok(buffer);
    }

    fs::read(path).map_err(|error| format!("cannot read the challenge at {path}: {error}"))
}

/// Writes the response durably and atomically at mode 0600.
///
/// Not the no-clobber publish `IdentityStore` performs for a key: an operator
/// who reruns an enrolment expects the response file to be replaced, whereas a
/// second key would strand the first. Same durability, deliberately different
/// publication rule.
fn write_response(path: &Path, response: &[u8]) -> Result<(), String> {
    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty());
    let temporary: PathBuf = match parent {
        Some(parent) => parent.join(format!(".{}.tmp", file_name(path))),
        None => PathBuf::from(format!(".{}.tmp", file_name(path))),
    };

    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(RESPONSE_MODE);
    }

    let write = (|| -> std::io::Result<()> {
        let mut file = options.open(&temporary)?;
        file.write_all(response)?;

        // The umask can only narrow the creation mode, so set the exact mode
        // before the bytes become durable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(RESPONSE_MODE))?;
        }

        file.sync_all()
    })();

    if let Err(error) = write {
        let _ = fs::remove_file(&temporary);
        return Err(format!("cannot write the response to {}: {error}", path.display()));
    }

    fs::rename(&temporary, path).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        format!("cannot publish the response at {}: {error}", path.display())
    })?;

    if let Some(parent) = parent {
        sync_directory(parent);
    }

    Ok(())
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "response".to_string())
}

/// Fsync the directory so the renamed entry survives power loss. Directories
/// cannot be opened for syncing on Windows, where this is a no-op.
fn sync_directory(directory: &Path) {
    #[cfg(unix)]
    {
        if let Ok(handle) = fs::File::open(directory) {
            let _ = handle.sync_all();
        }
    }
    #[cfg(not(unix))]
    let _ = directory;
}

/// Fills `buffer` with operating-system randomness.
///
/// The device nonce must be unpredictable: it is what stops a captured response
/// being replayed as a fresh one. Use the workspace rand 0.10 system RNG,
/// matching the rand_core version used by the upgraded crypto stack.
fn getrandom(buffer: &mut [u8]) -> Result<(), String> {
    use rand::{TryRng as _, rngs::SysRng};

    SysRng
        .try_fill_bytes(buffer)
        .map_err(|error| format!("the operating system random source failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[cfg(feature = "offline-enrollment-e2e-root")]
    #[test]
    fn only_the_dedicated_e2e_target_selects_the_test_root() {
        let challenge =
            fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/offline-enrollment-e2e/challenge.json"))
                .expect("read E2E enrollment challenge");
        let result = verify_enrollment_challenge(&challenge, 4_070_908_800);

        if env!("CARGO_BIN_NAME") == "rustfs-cli-e2e" {
            result.expect("the dedicated E2E binary selects the test root");
        } else {
            assert_eq!(
                result
                    .expect_err("every production binary must retain the hosted root")
                    .reason(),
                "ENROLLMENT_ROOT_UNKNOWN"
            );
        }
    }

    #[test]
    fn connect_offline_bundle_cli_shutdown_is_bounded_with_a_stuck_blocking_collector() {
        let runtime = tokio::runtime::Builder::new_current_thread().build().expect("test runtime");
        let operation = async {
            let (started, observed) = tokio::sync::oneshot::channel();
            tokio::task::spawn_blocking(move || {
                started.send(()).expect("report blocking task");
                std::thread::sleep(Duration::from_secs(5));
            });
            observed.await.expect("blocking task started");
            Err::<(), _>("collector cancelled".to_owned())
        };

        let started_at = Instant::now();
        assert!(run_offline_runtime(runtime, operation).is_err());
        assert!(started_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn connect_offline_bundle_cli_preserves_success_when_signal_and_committed_writer_are_ready() {
        let runtime = tokio::runtime::Builder::new_current_thread().build().expect("test runtime");
        let cancel = CancellationToken::new();
        let temp = tempfile::tempdir().expect("output tempdir");
        let output = temp.path().join("bundle.zip");
        let writer_output = output.clone();
        let signal = std::future::ready(Ok(()));
        tokio::pin!(signal);
        let writer = runtime.spawn(async move {
            fs::write(writer_output, b"published bundle").expect("publish test bundle");
            Ok(BundleReceipt {
                bundle_uid: "0198f3a1-8000-7e50-8f61-4a5b6c7d8e94".to_owned(),
                device_name: "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50/clusters/0198f3a1-5d00-7b20-9c31-1d2e3f4a5b61/clusterDevices/0198f3a1-6e00-7c30-ad41-2e3f4a5b6c72".to_owned(),
                archive_size_bytes: 1,
                archive_sha256: "00".repeat(32),
                l0_count: 6,
                l0_bytes: 1,
                l1_count: 6,
                l1_bytes: 1,
            })
        });
        runtime.block_on(async {
            while !writer.is_finished() {
                tokio::task::yield_now().await;
            }
        });
        assert_eq!(fs::read(&output).expect("read committed output"), b"published bundle");

        let result = runtime.block_on(await_offline_writer(&cancel, signal.as_mut(), writer));
        assert!(result.is_ok());
        assert!(cancel.is_cancelled());
    }
}
