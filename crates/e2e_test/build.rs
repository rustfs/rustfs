// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::path::Path;
use std::process::Command;

fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).current_dir(root).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn emit(name: &str, value: &str) {
    let value = if value.contains(['\n', '\r']) { "unknown" } else { value };
    println!("cargo:rustc-env=RUSTFS_E2E_BUILD_{name}={value}");
}

fn main() {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default();
    let root = Path::new(&manifest).join("../..");
    // Cover dependency/common sources as well as this crate. HEAD/ref/index
    // changes must refresh identity even when no Rust source mtime changes.
    for path in [
        "crates",
        "rustfs",
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".cargo",
        ".config",
    ] {
        println!("cargo:rerun-if-changed={}", root.join(path).display());
    }
    let mut git_paths = vec!["HEAD".to_owned(), "index".to_owned(), "packed-refs".to_owned()];
    if let Some(reference) = git(&root, &["symbolic-ref", "-q", "HEAD"]) {
        git_paths.push(reference);
    }
    for path in git_paths {
        if let Some(path) = git(&root, &["rev-parse", "--git-path", &path]) {
            let path = Path::new(&path);
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                root.join(path)
            };
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
    let revision = git(&root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let dirty = git(&root, &["status", "--porcelain", "--untracked-files=normal"]).is_none_or(|status| !status.is_empty());
    let lock = git(&root, &["hash-object", "Cargo.lock"]).unwrap_or_else(|| "unknown".to_owned());
    let mut features = std::env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
                .map(|name| name.to_ascii_lowercase().replace('_', "-"))
        })
        .collect::<Vec<_>>();
    features.sort();
    emit("COMMIT", &revision);
    emit("DIRTY", if dirty { "true" } else { "false" });
    emit("LOCK", &lock);
    emit("FEATURES", &features.join(","));
    for name in ["TARGET", "PROFILE"] {
        emit(name, &std::env::var(name).unwrap_or_else(|_| "unknown".to_owned()));
    }
    println!("cargo:rerun-if-env-changed=CARGO_ENCODED_RUSTFLAGS");
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    let flags: String = flags.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect();
    emit("RUSTFLAGS_HEX", &flags);
}
