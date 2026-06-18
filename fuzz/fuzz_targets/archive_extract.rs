#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs::app::object_usecase::{normalize_extract_entry_key, validate_extract_relative_path};

fn parse_case(data: &[u8]) -> (String, Option<String>, bool, Vec<String>) {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(4, '\n');
    let path = parts.next().unwrap_or_default().to_string();
    let prefix = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let is_dir = parts.next().unwrap_or_default().trim().eq_ignore_ascii_case("dir");
    let flags = parts
        .next()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|flag| !flag.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    (path, prefix, is_dir, flags)
}

fn apply_flag(mut path: String, mut prefix: Option<String>, flag: &str) -> (String, Option<String>) {
    match flag {
        "path_parent" => path.push_str("/../escape.txt"),
        "path_current" => path.push_str("/./child"),
        "path_backslash_parent" => path.push_str("\\..\\escape.txt"),
        "path_leading_slash" => path = format!("/{path}"),
        "path_empty" => path.clear(),
        "path_long" => path = format!("{path}/{}", "segment".repeat(256)),
        "prefix_parent" => prefix = Some("../victim-bucket".to_string()),
        "prefix_trimmed" => prefix = prefix.map(|value| format!(" /{value}/ ")),
        "prefix_empty" => prefix = Some(String::new()),
        _ => {}
    }

    (path, prefix)
}

fn materialize_case(path: String, prefix: Option<String>, flags: &[String]) -> (String, Option<String>) {
    flags
        .iter()
        .fold((path, prefix), |(path, prefix), flag| apply_flag(path, prefix, flag))
}

fn has_dot_segments(path: &str) -> bool {
    path.split(['/', '\\']).any(|segment| {
        let trimmed = segment.trim();
        trimmed == "." || trimmed == ".."
    })
}

fuzz_target!(|data: &[u8]| {
    let (path, prefix, is_dir, flags) = parse_case(data);
    let (path, prefix) = materialize_case(path, prefix, &flags);

    let _ = validate_extract_relative_path(&path);
    if let Some(prefix) = prefix.as_deref() {
        let _ = validate_extract_relative_path(prefix);
    }

    if let Ok(key) = normalize_extract_entry_key(&path, prefix.as_deref(), is_dir) {
        assert!(
            !has_dot_segments(&key),
            "accepted archive entry retained dot segments: path={:?} prefix={:?} key={:?}",
            path,
            prefix,
            key
        );
        assert!(
            !key.starts_with('/'),
            "accepted archive entry escaped bucket namespace: path={:?} prefix={:?} key={:?}",
            path,
            prefix,
            key
        );
        if is_dir {
            assert!(
                key.ends_with('/'),
                "accepted archive directory lost trailing slash: path={:?} prefix={:?} key={:?}",
                path,
                prefix,
                key
            );
        }
    }
});
