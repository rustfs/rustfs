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

use std::io::Write;

fn main() -> shadow_rs::SdResult<()> {
    println!("cargo:rerun-if-env-changed=RUSTFS_BUILD_VERSION");
    if let Ok(version) = std::env::var("RUSTFS_BUILD_VERSION")
        && !version.is_empty()
    {
        assert!(
            !version.contains(['\n', '\r']),
            "RUSTFS_BUILD_VERSION must be a single-line version string"
        );
        println!("cargo:rustc-env=RUSTFS_BUILD_VERSION={version}");
    }

    println!("cargo:rerun-if-env-changed=RUSTC");
    let target = std::env::var("TARGET")?;
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")?;
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH")?;
    let output = std::process::Command::new(std::env::var("RUSTC")?).arg("-vV").output()?;
    if !output.status.success() {
        return Err(std::io::Error::other("failed to query the build compiler").into());
    }
    let output = String::from_utf8(output.stdout)?;
    let release = output
        .lines()
        .find_map(|line| line.strip_prefix("release: "))
        .ok_or_else(|| std::io::Error::other("build compiler did not report its release"))?;
    let channel = match release.split_once('-') {
        Some((_, channel)) if matches!(channel.split('.').next(), Some("beta" | "nightly" | "dev")) => {
            channel.split('.').next().unwrap()
        }
        None if release.split('.').count() == 3 && release.split('.').all(|part| part.parse::<u32>().is_ok()) => "stable",
        _ => return Err(std::io::Error::other("unrecognized build compiler release channel").into()),
    };

    // Generate target metadata once so every shadow-rs consumer agrees, even
    // when the build script and compiler run on a different host platform.
    let mut deny = shadow_rs::default_deny();
    deny.extend([shadow_rs::BUILD_OS, shadow_rs::RUST_CHANNEL]);
    shadow_rs::ShadowBuilder::builder()
        .deny_const(deny)
        .hook(|file: &std::fs::File| {
            writeln!(&*file, "pub const BUILD_OS: &str = {:?};", format!("{target_os}-{target_arch}"))?;
            writeln!(&*file, "pub const RUST_CHANNEL: &str = {:?};", format!("{channel}-{target}"))?;
            Ok(())
        })
        .build()?;
    Ok(())
}
