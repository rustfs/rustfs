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

fn main() {
    #[cfg(feature = "server")]
    build_server();
}

#[cfg(feature = "server")]
fn build_server() {
    use std::{env, path::PathBuf};

    println!("cargo:rerun-if-env-changed=CUOBJSERVER_ROOT");
    for file in ["src/sdk.rs", "native/server.h", "native/server.cc"] {
        println!("cargo:rerun-if-changed={file}");
    }
    let mut build = cxx_build::bridge("src/sdk.rs");
    if let Some(root) = env::var_os("CUOBJSERVER_ROOT").map(PathBuf::from) {
        build.include(root.join("include"));
        for subdir in ["lib", "lib64"] {
            println!("cargo:rustc-link-search=native={}", root.join(subdir).display());
        }
    }
    build
        .include(".")
        .file("native/server.cc")
        .std("c++17")
        .compile("rustfs_cuobject_server");
    println!("cargo:rustc-link-lib=cuobjserver");
}
