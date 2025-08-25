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

use std::{cmp, env, fs, io::Write, path::Path, process::Command};

type AnyError = Box<dyn std::error::Error>;

const VERSION_PROTOBUF: Version = Version(27, 2, 0); // 27.2.0
const VERSION_FLATBUFFERS: Version = Version(24, 3, 25); // 24.3.25
/// Build protos if the major version of `flatc` or `protoc` is greater
/// or lesser than the expected version.
const ENV_BUILD_PROTOS: &str = "BUILD_PROTOS";
/// Path of `flatc` binary.
const ENV_FLATC_PATH: &str = "FLATC_PATH";

fn main() -> Result<(), AnyError> {
    let version = protobuf_compiler_version()?;
    let need_compile = match version.compare_ext(&VERSION_PROTOBUF) {
        Ok(cmp::Ordering::Greater) => true,
        Ok(_) => {
            let version_err = Version::build_error_message(&version, &VERSION_PROTOBUF).unwrap();
            println!("cargo:warning=Tool `protoc` {version_err}, skip compiling.");
            false
        }
        Err(version_err) => {
            // return Err(format!("Tool `protoc` {version_err}, please update it.").into());
            println!("cargo:warning=Tool `protoc` {version_err}, please update it.");
            false
        }
    };

    if !need_compile {
        return Ok(());
    }

    // path of proto file
    let project_root_dir = env::current_dir()?.join("crates/protos/src");
    let proto_dir = project_root_dir.clone();
    println!("proto_dir: {proto_dir:?}");
    let proto_files = &["node.proto"];
    let proto_out_dir = project_root_dir.join("generated").join("proto_gen");
    let flatbuffer_out_dir = project_root_dir.join("generated").join("flatbuffers_generated");
    // let descriptor_set_path = PathBuf::from(env::var(ENV_OUT_DIR).unwrap()).join("proto-descriptor.bin");

    tonic_prost_build::configure()
        .out_dir(proto_out_dir)
        // .file_descriptor_set_path(descriptor_set_path)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_well_known_types(true)
        .bytes(".")
        .emit_rerun_if_changed(false)
        .compile_protos(proto_files, &[proto_dir.to_string_lossy().as_ref()])
        .map_err(|e| format!("Failed to generate protobuf file: {e}."))?;

    // protos/gen/mod.rs
    let generated_mod_rs_path = project_root_dir.join("generated").join("proto_gen").join("mod.rs");
    let mut generated_mod_rs = fs::File::create(generated_mod_rs_path)?;
    writeln!(
        &mut generated_mod_rs,
        r#"// Copyright 2024 RustFS Team
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
    // limitations under the License."#
    )?;
    writeln!(&mut generated_mod_rs, "\n")?;
    writeln!(&mut generated_mod_rs, "pub mod node_service;")?;
    generated_mod_rs.flush()?;

    let generated_mod_rs_path = project_root_dir.join("generated").join("mod.rs");
    let mut generated_mod_rs = fs::File::create(generated_mod_rs_path)?;

    writeln!(
        &mut generated_mod_rs,
        r#"// Copyright 2024 RustFS Team
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
    // limitations under the License."#
    )?;
    writeln!(&mut generated_mod_rs, "\n")?;
    writeln!(&mut generated_mod_rs, "#![allow(unused_imports)]")?;
    writeln!(&mut generated_mod_rs, "\n")?;
    writeln!(&mut generated_mod_rs, "#![allow(clippy::all)]")?;
    writeln!(&mut generated_mod_rs, "pub mod proto_gen;")?;
    generated_mod_rs.flush()?;

    let flatc_path = match env::var(ENV_FLATC_PATH) {
        Ok(path) => {
            println!("cargo:warning=Specified flatc path by environment {ENV_FLATC_PATH}={path}");
            path
        }
        Err(_) => "flatc".to_string(),
    };

    compile_flatbuffers_models(
        &mut generated_mod_rs,
        &flatc_path,
        proto_dir.clone(),
        flatbuffer_out_dir.clone(),
        vec!["models"],
    )?;

    fmt();
    Ok(())
}

/// Compile proto/**.fbs files.
fn compile_flatbuffers_models<P: AsRef<Path>, S: AsRef<str>>(
    generated_mod_rs: &mut fs::File,
    flatc_path: &str,
    in_fbs_dir: P,
    out_rust_dir: P,
    mod_names: Vec<S>,
) -> Result<(), AnyError> {
    let version = flatbuffers_compiler_version(flatc_path)?;
    let need_compile = match version.compare_ext(&VERSION_FLATBUFFERS) {
        Ok(cmp::Ordering::Greater) => true,
        Ok(_) => {
            let version_err = Version::build_error_message(&version, &VERSION_FLATBUFFERS).unwrap();
            println!("cargo:warning=Tool `{flatc_path}` {version_err}, skip compiling.");
            false
        }
        Err(version_err) => {
            return Err(format!("Tool `{flatc_path}` {version_err}, please update it.").into());
        }
    };

    let fbs_dir = in_fbs_dir.as_ref();
    let rust_dir = out_rust_dir.as_ref();
    fs::create_dir_all(rust_dir)?;

    // $rust_dir/mod.rs
    let mut sub_mod_rs = fs::File::create(rust_dir.join("mod.rs"))?;
    writeln!(generated_mod_rs)?;
    writeln!(generated_mod_rs, "mod flatbuffers_generated;")?;
    for mod_name in mod_names.iter() {
        let mod_name = mod_name.as_ref();
        writeln!(generated_mod_rs, "pub use flatbuffers_generated::{mod_name}::*;")?;
        writeln!(&mut sub_mod_rs, "pub mod {mod_name};")?;

        if need_compile {
            let fbs_file_path = fbs_dir.join(format!("{mod_name}.fbs"));
            let output = Command::new(flatc_path)
                .arg("-o")
                .arg(rust_dir)
                .arg("--rust")
                .arg("--gen-mutable")
                .arg("--gen-onefile")
                .arg("--gen-name-strings")
                .arg("--filename-suffix")
                .arg("")
                .arg(&fbs_file_path)
                .output()
                .map_err(|e| format!("Failed to execute process of flatc: {e}"))?;
            if !output.status.success() {
                return Err(format!(
                    "Failed to generate file '{}' by flatc(path: '{flatc_path}'): {}.",
                    fbs_file_path.display(),
                    String::from_utf8_lossy(&output.stderr),
                )
                .into());
            }
        }
    }
    generated_mod_rs.flush()?;
    sub_mod_rs.flush()?;

    Ok(())
}

/// Run command `flatc --version` to get the version of flatc.
///
/// ```ignore
/// $ flatc --version
/// flatc version 24.3.25
/// ```
fn flatbuffers_compiler_version(flatc_path: impl AsRef<Path>) -> Result<Version, String> {
    let flatc_path = flatc_path.as_ref();
    Version::try_get(format!("{}", flatc_path.display()), |output| {
        const PREFIX_OF_VERSION: &str = "flatc version ";
        let output = output.trim();
        if let Some(version) = output.strip_prefix(PREFIX_OF_VERSION) {
            Ok(version.to_string())
        } else {
            Err(format!("Failed to get flatc version: {output}"))
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Version(u32, u32, u32);

impl Version {
    fn try_get<F: FnOnce(&str) -> Result<String, String>>(exe: String, output_to_version_string: F) -> Result<Self, String> {
        let cmd = format!("{exe} --version");
        let output = std::process::Command::new(exe)
            .arg("--version")
            .output()
            .map_err(|e| format!("Failed to execute `{cmd}`: {e}",))?;
        let output_utf8 = String::from_utf8(output.stdout).map_err(|e| {
            let output_lossy = String::from_utf8_lossy(e.as_bytes());
            format!("Command `{cmd}` returned invalid UTF-8('{output_lossy}'): {e}")
        })?;
        if output.status.success() {
            let version_string = output_to_version_string(&output_utf8)?;
            Ok(version_string.parse::<Self>()?)
        } else {
            Err(format!("Failed to get version by command `{cmd}`: {output_utf8}"))
        }
    }

    fn build_error_message(version: &Self, expected: &Self) -> Option<String> {
        match version.compare_major_version(expected) {
            cmp::Ordering::Equal => None,
            cmp::Ordering::Greater => Some(format!("version({version}) is greater than version({expected})")),
            cmp::Ordering::Less => Some(format!("version({version}) is lesser than version({expected})")),
        }
    }

    fn compare_ext(&self, expected_version: &Self) -> Result<cmp::Ordering, String> {
        match env::var(ENV_BUILD_PROTOS) {
            Ok(build_protos) => {
                if build_protos.is_empty() || build_protos == "0" {
                    Ok(self.compare_major_version(expected_version))
                } else {
                    match self.compare_major_version(expected_version) {
                        cmp::Ordering::Greater => Ok(cmp::Ordering::Greater),
                        _ => Err(Self::build_error_message(self, expected_version).unwrap()),
                    }
                }
            }
            Err(_) => Ok(self.compare_major_version(expected_version)),
        }
    }

    fn compare_major_version(&self, other: &Self) -> cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::str::FromStr for Version {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut version = [0_u32; 3];
        for (i, v) in s.split('.').take(3).enumerate() {
            version[i] = v.parse().map_err(|e| format!("Failed to parse version string '{s}': {e}"))?;
        }
        Ok(Version(version[0], version[1], version[2]))
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

/// Run command `protoc --version` to get the version of flatc.
///
/// ```ignore
/// $ protoc --version
/// libprotoc 27.0
/// ```
fn protobuf_compiler_version() -> Result<Version, String> {
    Version::try_get("protoc".to_string(), |output| {
        const PREFIX_OF_VERSION: &str = "libprotoc ";
        let output = output.trim();
        if let Some(version) = output.strip_prefix(PREFIX_OF_VERSION) {
            Ok(version.to_string())
        } else {
            Err(format!("Failed to get protoc version: {output}"))
        }
    })
}

fn fmt() {
    let output = Command::new("cargo").arg("fmt").arg("-p").arg("rustfs-protos").status();

    match output {
        Ok(status) => {
            if status.success() {
                println!("cargo fmt executed successfully.");
            } else {
                eprintln!("cargo fmt failed with status: {status:?}");
            }
        }
        Err(e) => {
            eprintln!("Failed to execute cargo fmt: {e}");
        }
    }
}
