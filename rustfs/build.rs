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
    if let Ok(version) = std::env::var("RUSTFS_BUILD_VERSION")
        && !version.is_empty()
    {
        assert!(
            !version.contains(['\n', '\r']),
            "RUSTFS_BUILD_VERSION must be a single-line version string"
        );
        println!("cargo:rustc-env=RUSTFS_BUILD_VERSION={version}");
    }

    shadow_rs::ShadowBuilder::builder().build()?;
    Ok(())
}
