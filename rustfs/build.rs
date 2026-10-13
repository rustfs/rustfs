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

fn main() -> shadow_rs::SdResult<()> {
    println!("cargo:rerun-if-env-changed=RUSTFS_BUILD_VERSION");
    println!("cargo:rerun-if-env-changed=RUSTC");
    if let Ok(version) = std::env::var("RUSTFS_BUILD_VERSION")
        && !version.is_empty()
    {
        assert!(
            !version.contains(['\n', '\r']),
            "RUSTFS_BUILD_VERSION must be a single-line version string"
        );
        println!("cargo:rustc-env=RUSTFS_BUILD_VERSION={version}");
    }

    // shadow-rs stamps `BUILD_OS` and `RUST_CHANNEL` from the build *host*, so
    // any build whose host differs from its target (cross compilation, musl
    // targets on a glibc-hosted toolchain) reports the wrong platform in
    // `--version` (#8048). Embed target-based values instead.
    let target = std::env::var("TARGET").expect("cargo sets TARGET for build scripts");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").expect("cargo sets CARGO_CFG_TARGET_OS");
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").expect("cargo sets CARGO_CFG_TARGET_ARCH");
    println!("cargo:rustc-env=RUSTFS_BUILD_OS={target_os}-{target_arch}");

    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let channel = rustc_channel(&rustc);
    println!("cargo:rustc-env=RUSTFS_RUST_CHANNEL={channel}-{target}");

    shadow_rs::ShadowBuilder::builder().build()?;
    Ok(())
}

/// Release channel of the build toolchain ("stable", "beta", "nightly", "dev"),
/// derived from the `release:` line of `rustc -vV`.
fn rustc_channel(rustc: &str) -> &'static str {
    let release = std::process::Command::new(rustc)
        .arg("-vV")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("release: "))
                .map(str::to_owned)
        });
    if release.is_none() {
        println!("cargo:warning=could not parse the `rustc -vV` release line; assuming stable channel");
    }
    match release.as_deref() {
        Some(release) if release.contains("nightly") => "nightly",
        Some(release) if release.contains("beta") => "beta",
        Some(release) if release.contains("dev") => "dev",
        _ => "stable",
    }
}
