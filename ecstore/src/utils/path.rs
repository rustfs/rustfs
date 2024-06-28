const GLOBAL_DIR_SUFFIX: &str = "__XLDIR__";
const SLASH_SEPARATOR: char = '/';

pub fn has_suffix(s: &str, suffix: &str) -> bool {
    if cfg!(target_os = "windows") {
        s.to_lowercase().ends_with(&suffix.to_lowercase())
    } else {
        s.ends_with(suffix)
    }
}

pub fn encode_dir_object(object: &str) -> String {
    if has_suffix(object, &SLASH_SEPARATOR.to_string()) {
        format!(
            "{}{}",
            object.trim_end_matches(SLASH_SEPARATOR),
            GLOBAL_DIR_SUFFIX
        )
    } else {
        object.to_string()
    }
}

pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        format!(
            "{}{}",
            object.trim_end_matches(GLOBAL_DIR_SUFFIX),
            SLASH_SEPARATOR
        )
    } else {
        object.to_string()
    }
}
