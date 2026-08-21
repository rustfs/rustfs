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
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use rustfs::connect::offline::{OfflineEnrollment, OfflineKeyStore};

/// Owner read/write only. The response names the key being enrolled and the
/// challenge it answers; neither belongs to anyone else on the machine.
#[cfg(unix)]
const RESPONSE_MODE: u32 = 0o600;

const USAGE: &str = "\
Usage: rustfs-cli connect offline enroll --challenge <path|-> --output <path> [--key-dir <path>]

Answers a Connect offline enrolment challenge without a network. Reads the
challenge from a file or from stdin when the path is `-`, verifies it against the
enrolment root compiled into this binary, mints the key being enrolled on first
use, and writes the signed response.

No secret is ever accepted on the command line.
";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();

    // Offline enrolment is handled before the server dispatcher is reached, and
    // the reason is the surface's whole point: `run_process` builds a Tokio
    // runtime and enters the server's async main. An air-gapped enrolment must
    // not start a runtime, a task, or anything that could open a socket, so the
    // two paths cannot share an entry.
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
        Some(other) => Err(format!("unknown offline subcommand `{other}`\n\n{USAGE}")),
        None => Err(format!("missing offline subcommand\n\n{USAGE}")),
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
/// being replayed as a fresh one. Sourced through p256's pinned rand_core 0.6
/// rather than the workspace `rand` 0.10, matching `identity.rs`; the two are
/// different crate versions and only the pinned one is on p256's own path.
fn getrandom(buffer: &mut [u8]) -> Result<(), String> {
    use p256::elliptic_curve::rand_core::{OsRng, RngCore as _};

    OsRng
        .try_fill_bytes(buffer)
        .map_err(|error| format!("the operating system random source failed: {error}"))
}
