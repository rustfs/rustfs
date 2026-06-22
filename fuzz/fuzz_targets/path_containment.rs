#![no_main]

use libfuzzer_sys::fuzz_target;
mod ecstore_fuzz_compat;
use ecstore_fuzz_compat::{check_object_name_for_length_and_slash, has_bad_path_component, is_valid_object_prefix};
use rustfs_utils::path::{clean, path_join};
use std::path::{Path, PathBuf};

const ROOT: &str = "/srv/rustfs";

fn parse_case(data: &[u8]) -> (String, String, String, Vec<String>) {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(4, '\n');
    let bucket = parts.next().unwrap_or_default().to_string();
    let object = parts.next().unwrap_or_default().to_string();
    let extra = parts.next().unwrap_or_default().to_string();
    let flags = parts
        .next()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|flag| !flag.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    (bucket, object, extra, flags)
}

fn stays_within_root(root: &Path, candidate: &Path) -> bool {
    candidate == root || candidate.starts_with(root)
}

fn apply_flag(mut object: String, extra: &str, flag: &str) -> String {
    let extra_segment = if extra.is_empty() { "tail" } else { extra };

    match flag {
        "leading_ws" => format!(" {object}"),
        "trailing_ws" => {
            object.push(' ');
            object
        }
        "leading_slash" => format!("/{object}"),
        "trailing_slash" => format!("{object}/"),
        "double_sep" => {
            if object.contains('/') {
                object.replacen('/', "//", 1)
            } else {
                format!("{object}//{extra_segment}")
            }
        }
        "backslash" => object.replace('/', "\\"),
        "append_extra" => format!("{object}/{extra_segment}"),
        "empty_segment" => {
            if let Some((left, right)) = object.split_once('/') {
                format!("{left}//{right}")
            } else {
                format!("{object}//{extra_segment}")
            }
        }
        "parent_segment" => format!("{object}/../{extra_segment}"),
        "current_segment" => format!("{object}/./{extra_segment}"),
        _ => object,
    }
}

fn materialize_object(base: String, extra: &str, flags: &[String]) -> String {
    flags.iter().fold(base, |object, flag| apply_flag(object, extra, flag))
}

fn has_dot_segments(path: &str) -> bool {
    path.split(['/', '\\']).any(|segment| {
        let mut bytes = segment.as_bytes();

        // Match ecstore's path-component check. `str::trim()` also trims
        // Unicode whitespace such as vertical tab, which would reject object
        // keys that the production validator accepts as ordinary path bytes.
        while let Some((first, rest)) = bytes.split_first()
            && first.is_ascii_whitespace()
        {
            bytes = rest;
        }
        while let Some((last, rest)) = bytes.split_last()
            && last.is_ascii_whitespace()
        {
            bytes = rest;
        }

        bytes == b"." || bytes == b".."
    })
}

fuzz_target!(|data: &[u8]| {
    let (bucket, base_object, extra, flags) = parse_case(data);
    let object = materialize_object(base_object, &extra, &flags);

    let has_bad_component = has_bad_path_component(&object);
    let is_valid_prefix = is_valid_object_prefix(&object);
    let length_and_slash_ok = check_object_name_for_length_and_slash(&bucket, &object).is_ok();

    if object.is_empty() || !(is_valid_prefix && length_and_slash_ok) {
        return;
    }

    assert!(
        !has_bad_component,
        "accepted object unexpectedly retained a bad path component: {:?}",
        object
    );
    assert!(
        !has_dot_segments(&object),
        "accepted object unexpectedly retained dot segments: {:?}",
        object
    );

    let root = PathBuf::from(ROOT);
    let joined = path_join(&[root.clone(), PathBuf::from(&object)]);
    let cleaned = PathBuf::from(clean(joined.to_string_lossy().as_ref()));
    let cleaned_text = cleaned.to_string_lossy();

    assert!(
        stays_within_root(&root, &cleaned),
        "accepted object escaped root containment: object={:?} cleaned={:?}",
        object,
        cleaned
    );
    assert!(
        !has_dot_segments(cleaned_text.as_ref()),
        "cleaned accepted object unexpectedly retained dot segments: object={:?} cleaned={:?}",
        object,
        cleaned
    );
});
