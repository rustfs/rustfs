#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_ecstore::bucket::utils::{check_object_name_for_length_and_slash, has_bad_path_component, is_valid_object_prefix};
use rustfs_utils::path::{clean, path_join};
use std::path::{Path, PathBuf};

const ROOT: &str = "/srv/rustfs";

fn parse_case(data: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(2, '\n');
    let bucket = parts.next().unwrap_or_default().to_string();
    let object = parts.next().unwrap_or_default().to_string();
    (bucket, object)
}

fn stays_within_root(root: &Path, candidate: &Path) -> bool {
    candidate == root || candidate.starts_with(root)
}

fuzz_target!(|data: &[u8]| {
    let (bucket, object) = parse_case(data);

    let has_bad_component = has_bad_path_component(&object);
    let is_valid_prefix = is_valid_object_prefix(&object);
    let length_and_slash_ok = check_object_name_for_length_and_slash(&bucket, &object).is_ok();

    if !(is_valid_prefix && length_and_slash_ok) {
        return;
    }

    assert!(
        !has_bad_component,
        "accepted object unexpectedly retained a bad path component: {:?}",
        object
    );

    let root = PathBuf::from(ROOT);
    let joined = path_join(&[root.clone(), PathBuf::from(&object)]);
    let cleaned = PathBuf::from(clean(joined.to_string_lossy().as_ref()));

    assert!(
        stays_within_root(&root, &cleaned),
        "accepted object escaped root containment: object={:?} cleaned={:?}",
        object,
        cleaned
    );
});
