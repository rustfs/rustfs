use crate::{disk::RUSTFS_META_BUCKET, error::Error};

pub fn is_meta_bucketname(name: &str) -> bool {
    name.starts_with(RUSTFS_META_BUCKET)
}

use regex::Regex;

lazy_static::lazy_static! {
    static ref VALID_BUCKET_NAME: Regex = Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-\_\:]{1,61}[A-Za-z0-9]$").unwrap();
    static ref VALID_BUCKET_NAME_STRICT: Regex = Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap();
    static ref IP_ADDRESS: Regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
}

pub fn check_bucket_name_common(bucket_name: &str, strict: bool) -> Result<(), Error> {
    let bucket_name_trimmed = bucket_name.trim();

    if bucket_name_trimmed.is_empty() {
        return Err(Error::msg("Bucket name cannot be empty"));
    }
    if bucket_name_trimmed.len() < 3 {
        return Err(Error::msg("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name_trimmed.len() > 63 {
        return Err(Error::msg("Bucket name cannot be longer than 63 characters"));
    }

    if bucket_name_trimmed == "rustfs" {
        return Err(Error::msg("Bucket name cannot be rustfs"));
    }

    if IP_ADDRESS.is_match(bucket_name_trimmed) {
        return Err(Error::msg("Bucket name cannot be an IP address"));
    }
    if bucket_name_trimmed.contains("..") || bucket_name_trimmed.contains(".-") || bucket_name_trimmed.contains("-.") {
        return Err(Error::msg("Bucket name contains invalid characters"));
    }
    if strict {
        if !VALID_BUCKET_NAME_STRICT.is_match(bucket_name_trimmed) {
            return Err(Error::msg("Bucket name contains invalid characters"));
        }
    } else if !VALID_BUCKET_NAME.is_match(bucket_name_trimmed) {
        return Err(Error::msg("Bucket name contains invalid characters"));
    }
    Ok(())
}

pub fn check_valid_bucket_name(bucket_name: &str) -> Result<(), Error> {
    check_bucket_name_common(bucket_name, false)
}

pub fn check_valid_bucket_name_strict(bucket_name: &str) -> Result<(), Error> {
    check_bucket_name_common(bucket_name, true)
}

pub fn check_valid_object_name_prefix(object_name: &str) -> Result<(), Error> {
    if object_name.len() > 1024 {
        return Err(Error::msg("Object name cannot be longer than 1024 characters"));
    }
    if !object_name.is_ascii() {
        return Err(Error::msg("Object name with non-UTF-8 strings are not supported"));
    }
    Ok(())
}

pub fn check_valid_object_name(object_name: &str) -> Result<(), Error> {
    if object_name.trim().is_empty() {
        return Err(Error::msg("Object name cannot be empty"));
    }
    check_valid_object_name_prefix(object_name)
}
