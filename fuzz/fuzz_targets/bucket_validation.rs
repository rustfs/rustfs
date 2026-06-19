#![no_main]

mod storage_compat;

use libfuzzer_sys::fuzz_target;
use crate::storage_compat::ecstore::bucket::utils::{
    check_bucket_and_object_names, check_list_objs_args, check_valid_bucket_name_strict, is_meta_bucketname,
};

fn parse_case(data: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.splitn(2, '\n');
    let bucket = parts.next().unwrap_or_default().to_string();
    let object = parts.next().unwrap_or_default().to_string();
    (bucket, object)
}

fn looks_like_ipv4(text: &str) -> bool {
    let parts: Vec<_> = text.split('.').collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fuzz_target!(|data: &[u8]| {
    let (bucket, object) = parse_case(data);

    let is_meta_bucket = is_meta_bucketname(&bucket);
    let strict_bucket_ok = check_valid_bucket_name_strict(&bucket).is_ok();
    let pair_ok = check_bucket_and_object_names(&bucket, &object).is_ok();
    let list_ok = check_list_objs_args(&bucket, &object, &None).is_ok();

    if strict_bucket_ok {
        let trimmed = bucket.trim();
        assert!((3..=63).contains(&trimmed.len()), "accepted bucket has invalid length: {:?}", bucket);
        assert_eq!(trimmed, bucket, "strict bucket validation should not accept leading/trailing whitespace");
        assert!(trimmed != "rustfs", "strict bucket validation should reject reserved bucket name");
        assert!(!looks_like_ipv4(trimmed), "strict bucket validation should reject IPv4 bucket names");
        assert!(
            !trimmed.contains("..") && !trimmed.contains(".-") && !trimmed.contains("-."),
            "strict bucket validation should reject forbidden bucket sequences"
        );
        assert!(
            trimmed
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '.' || ch == '-'),
            "strict bucket validation accepted an unexpected character set: {:?}",
            bucket
        );
    }

    if pair_ok {
        assert!(
            strict_bucket_ok || is_meta_bucket,
            "accepted bucket/object pair must satisfy strict bucket validation unless it is an internal meta bucket"
        );
    }

    if list_ok {
        assert!(
            strict_bucket_ok || is_meta_bucket,
            "accepted list-object args must satisfy strict bucket validation unless it is an internal meta bucket"
        );
    }
});
