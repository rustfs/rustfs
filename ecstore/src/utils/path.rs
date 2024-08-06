const GLOBAL_DIR_SUFFIX: &str = "__XLDIR__";

const SLASH_SEPARATOR: &str = "/";

pub fn has_suffix(s: &str, suffix: &str) -> bool {
    if cfg!(target_os = "windows") {
        s.to_lowercase().ends_with(&suffix.to_lowercase())
    } else {
        s.ends_with(suffix)
    }
}

pub fn encode_dir_object(object: &str) -> String {
    if has_suffix(object, SLASH_SEPARATOR) {
        format!("{}{}", object.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX)
    } else {
        object.to_string()
    }
}

#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        format!("{}{}", object.trim_end_matches(GLOBAL_DIR_SUFFIX), SLASH_SEPARATOR)
    } else {
        object.to_string()
    }
}

pub fn retain_slash(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    if s.ends_with(SLASH_SEPARATOR) {
        s.to_string()
    } else {
        format!("{}{}", s, SLASH_SEPARATOR)
    }
}
