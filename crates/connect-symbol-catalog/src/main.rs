// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Release tooling only: derive reviewed CPU symbol candidates from the final ELF.

use object::{Object, ObjectSymbol};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const MAX_BINARY_BYTES: u64 = 1_073_741_824;
const MAX_CATALOG_BYTES: usize = 32 * 1024 * 1024;
const MAX_SYMBOLS: usize = 200_000;
const NORMALIZATION: &str = "pyroscope-2.1.1-symbolic-demangle-13.9.0";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Catalog<'a> {
    schema_version: u8,
    normalization: &'static str,
    source_commit: &'a str,
    executable_sha256: String,
    os_family: &'static str,
    architecture: &'static str,
    build_features: Vec<String>,
    symbols: BTreeSet<String>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn symbol_name(raw: &[u8]) -> Option<String> {
    // This is the same lossy UTF-8 conversion and demangler used by pprof-rs.
    let name = symbolic_demangle::demangle(&String::from_utf8_lossy(raw)).into_owned();
    if name.is_empty()
        || name.len() > 4096
        || matches!(name.as_str(), "Unknown" | "<unresolved>")
        || name.bytes().any(|b| b < 32 || b == 127)
    {
        return None;
    }
    Some(name)
}

fn catalog(binary: &[u8], source: &str, target: &str, features: Vec<String>) -> io::Result<Vec<u8>> {
    if source.len() != 40 || !source.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err(invalid("source commit must be a lowercase full SHA"));
    }
    let expected_arch = match target {
        "x86_64-unknown-linux-gnu" => object::Architecture::X86_64,
        "aarch64-unknown-linux-gnu" => object::Architecture::Aarch64,
        _ => return Err(invalid("only supported Linux GNU CPU profiling targets have catalogs")),
    };
    if features.is_empty()
        || features.len() > 64
        || !features.iter().any(|f| f == "pyroscope")
        || features.iter().any(|f| {
            f.is_empty()
                || f.len() > 64
                || !f.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
                || !f
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        })
        || features.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid("build features must be sorted, unique and include pyroscope"));
    }
    let file = object::File::parse(binary).map_err(|_| invalid("cannot parse executable"))?;
    if file.format() != object::BinaryFormat::Elf
        || file.architecture() != expected_arch
        || !matches!(file.kind(), object::ObjectKind::Executable | object::ObjectKind::Dynamic)
    {
        return Err(invalid("the executable does not match the Linux target"));
    }
    if file.symbol_table().is_none() {
        return Err(invalid("the executable is stripped; retain its function symbol table"));
    }
    let mut symbols = BTreeSet::new();
    let mut symbol_bytes = 0;
    for symbol in file.symbols().chain(file.dynamic_symbols()) {
        if symbol.kind() != object::SymbolKind::Text || symbol.is_undefined() {
            continue;
        }
        if let Some(name) = symbol.name_bytes().ok().and_then(symbol_name)
            && !symbols.contains(&name)
        {
            symbol_bytes += name.len() + 3;
            if symbol_bytes > MAX_CATALOG_BYTES {
                return Err(invalid("symbol catalog exceeds its byte limit"));
            }
            symbols.insert(name);
        }
        if symbols.len() > MAX_SYMBOLS {
            return Err(invalid("symbol catalog exceeds its reviewed symbol limit"));
        }
    }
    if symbols.is_empty() {
        return Err(invalid("the executable has no usable function symbols"));
    }
    let catalog = Catalog {
        schema_version: 1,
        normalization: NORMALIZATION,
        source_commit: source,
        executable_sha256: hex_simd::encode_to_string(Sha256::digest(binary), hex_simd::AsciiCase::Lower),
        os_family: "LINUX",
        architecture: if expected_arch == object::Architecture::X86_64 {
            "x86_64"
        } else {
            "aarch64"
        },
        build_features: features,
        symbols,
    };
    let json = serde_json::to_vec(&catalog).map_err(io::Error::other)?;
    if json.len() > MAX_CATALOG_BYTES {
        return Err(invalid("symbol catalog exceeds its byte limit"));
    }
    Ok(json)
}

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [binary_path, source, target, features_json, output] = args.as_slice() else {
        return Err(invalid(
            "usage: rustfs-connect-symbol-catalog BINARY SOURCE_SHA TARGET FEATURES_JSON OUTPUT",
        ));
    };
    let input = fs::File::open(binary_path)?;
    if !input.metadata()?.is_file() || input.metadata()?.len() > MAX_BINARY_BYTES {
        return Err(invalid("input must be a bounded regular executable"));
    }
    let mut binary = Vec::new();
    input.take(MAX_BINARY_BYTES + 1).read_to_end(&mut binary)?;
    if u64::try_from(binary.len()).map_err(io::Error::other)? > MAX_BINARY_BYTES {
        return Err(invalid("executable grew beyond its byte limit"));
    }
    let features = serde_json::from_str(features_json).map_err(io::Error::other)?;
    let json = catalog(&binary, source, target, features)?;
    // Never overwrite another build's reviewed artifact.
    let mut file = OpenOptions::new().write(true).create_new(true).open(Path::new(output))?;
    file.write_all(&json)?;
    file.sync_all()?;
    println!("{}", hex_simd::encode_to_string(Sha256::digest(&json), hex_simd::AsciiCase::Lower));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executable() -> Vec<u8> {
        let mut object =
            object::write::Object::new(object::BinaryFormat::Elf, object::Architecture::X86_64, object::Endianness::Little);
        let section = object.section_id(object::write::StandardSection::Text);
        object.append_section_data(section, &[0xc3], 1);
        object.add_symbol(object::write::Symbol {
            name: b"_ZN4test4main17h0123456789abcdefE".to_vec(),
            value: 0,
            size: 1,
            kind: object::SymbolKind::Text,
            scope: object::SymbolScope::Linkage,
            weak: false,
            section: object::write::SymbolSection::Section(section),
            flags: object::SymbolFlags::None,
        });
        let mut bytes = object.write().expect("write ELF symbol fixture");
        // The object writer emits ET_REL; this fixture models the final ET_EXEC header.
        bytes[16..18].copy_from_slice(&2_u16.to_le_bytes());
        bytes
    }

    fn bulk_executable(count: usize, suffix_len: usize) -> Vec<u8> {
        let mut object =
            object::write::Object::new(object::BinaryFormat::Elf, object::Architecture::X86_64, object::Endianness::Little);
        let section = object.section_id(object::write::StandardSection::Text);
        object.append_section_data(section, &[0xc3], 1);
        let suffix = "x".repeat(suffix_len);
        for index in 0..count {
            object.add_symbol(object::write::Symbol {
                name: format!("rustfs::profile::function_{index:06}_{suffix}").into_bytes(),
                value: 0,
                size: 1,
                kind: object::SymbolKind::Text,
                scope: object::SymbolScope::Linkage,
                weak: false,
                section: object::write::SymbolSection::Section(section),
                flags: object::SymbolFlags::None,
            });
        }
        let mut binary = object.write().expect("write bulk ELF fixture");
        binary[16..18].copy_from_slice(&2_u16.to_le_bytes());
        binary
    }

    #[test]
    fn catalog_binds_the_exact_executable_and_normalizes_function_names() {
        let binary = executable();
        let encoded =
            catalog(&binary, &"a".repeat(40), "x86_64-unknown-linux-gnu", vec!["pyroscope".into()]).expect("generate catalog");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("catalog JSON");
        assert_eq!(value["symbols"], serde_json::json!(["test::main"]));
        assert_eq!(
            value["executableSha256"],
            hex_simd::encode_to_string(Sha256::digest(&binary), hex_simd::AsciiCase::Lower)
        );
        assert_eq!(value["normalization"], NORMALIZATION);
        assert!(catalog(&binary, &"a".repeat(40), "aarch64-unknown-linux-gnu", vec!["pyroscope".into()]).is_err());
        assert!(catalog(&binary, "bad", "x86_64-unknown-linux-gnu", vec!["pyroscope".into()]).is_err());
        assert!(catalog(&binary, &"a".repeat(40), "x86_64-unknown-linux-gnu", vec!["pyroscope".into(); 2]).is_err());
    }

    #[test]
    fn stripped_executables_and_wrong_abis_are_refused() {
        let mut binary = executable();
        // Removing section headers models a fully stripped executable without dynsym.
        binary[40..48].fill(0);
        binary[60..64].fill(0);
        let error = catalog(&binary, &"a".repeat(40), "x86_64-unknown-linux-gnu", vec!["pyroscope".into()])
            .expect_err("stripped binary must not yield an empty but trusted catalog");
        assert!(error.to_string().contains("stripped"));
        assert!(catalog(&executable(), &"a".repeat(40), "x86_64-unknown-linux-musl", vec!["pyroscope".into()]).is_err());
    }

    #[test]
    fn symbol_hash_matches_the_connect_nonce_vector() {
        let nonce: Vec<u8> = (0..32).collect();
        let name = symbol_name(b"core::hint::black_box").expect("reviewed function name");
        let mut digest = Sha256::new();
        digest.update(b"rustfs-connect-cpu-symbol-v1\0");
        digest.update(nonce);
        digest.update(name.as_bytes());
        assert_eq!(
            hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower),
            "5d5b472a0dd09124bfedf0bd243f88d5e61952b8da15d7463da019137320d77c"
        );
    }

    #[test]
    fn unresolved_and_non_function_material_cannot_become_catalog_entries() {
        assert!(symbol_name(b"Unknown").is_none());
        assert!(symbol_name(b"<unresolved>").is_none());
        assert!(symbol_name(b"secret\nname").is_none());
        assert!(symbol_name(&vec![b'x'; 4097]).is_none());
        assert!(catalog(b"not ELF", &"a".repeat(40), "x86_64-unknown-linux-gnu", vec!["pyroscope".into()]).is_err());
    }

    #[test]
    fn release_sized_catalog_retains_all_function_names() {
        let binary = bulk_executable(110_000, 160);
        let encoded = catalog(&binary, &"a".repeat(40), "x86_64-unknown-linux-gnu", vec!["pyroscope".into()])
            .expect("retain complete bounded symbol catalog");
        assert!(encoded.len() > 16 * 1024 * 1024);
        assert!(encoded.len() <= MAX_CATALOG_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("catalog JSON");
        assert_eq!(value["symbols"].as_array().expect("symbol array").len(), 110_000);
    }

    #[test]
    fn catalogs_over_either_resource_limit_are_refused() {
        let source = "a".repeat(40);
        let too_many = catalog(
            &bulk_executable(MAX_SYMBOLS + 1, 0),
            &source,
            "x86_64-unknown-linux-gnu",
            vec!["pyroscope".into()],
        )
        .expect_err("too many function names must be refused");
        assert!(too_many.to_string().contains("reviewed symbol limit"));

        let too_large = catalog(
            &bulk_executable(10_000, 3_500),
            &source,
            "x86_64-unknown-linux-gnu",
            vec!["pyroscope".into()],
        )
        .expect_err("oversized catalog must be refused");
        assert!(too_large.to_string().contains("byte limit"));
    }
}
