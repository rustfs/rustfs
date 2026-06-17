#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs::storage::options::parse_copy_source_range;

fuzz_target!(|data: &[u8]| {
    let range = String::from_utf8_lossy(data);

    if let Ok(spec) = parse_copy_source_range(range.as_ref()) {
        if spec.is_suffix_length {
            assert_eq!(spec.end, -1, "suffix ranges should use -1 as the open end");
            assert!(spec.start <= 0, "suffix ranges should not have a positive start");
        } else {
            assert!(spec.start >= 0, "non-suffix ranges should not have a negative start");
            assert!(
                spec.end == -1 || spec.end >= spec.start,
                "non-suffix ranges must be open-ended or ordered"
            );
        }
    }
});
