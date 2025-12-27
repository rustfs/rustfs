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

use crate::config::storageclass::STANDARD;
use crate::disk::RUSTFS_META_BUCKET;
use regex::Regex;
use rustfs_utils::http::headers::{AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS};
use std::collections::HashMap;
use std::io::{Error, Result};

pub fn clean_metadata(metadata: &mut HashMap<String, String>) {
    remove_standard_storage_class(metadata);
    clean_metadata_keys(metadata, &["md5Sum", "etag", "expires", AMZ_OBJECT_TAGGING, "last-modified"]);
}

pub fn remove_standard_storage_class(metadata: &mut HashMap<String, String>) {
    if metadata.get(AMZ_STORAGE_CLASS) == Some(&STANDARD.to_string()) {
        metadata.remove(AMZ_STORAGE_CLASS);
    }
}

pub fn clean_metadata_keys(metadata: &mut HashMap<String, String>, key_names: &[&str]) {
    for key in key_names {
        metadata.remove(key.to_owned());
    }
}

// Check whether the bucket is the metadata bucket
fn is_meta_bucket(bucket_name: &str) -> bool {
    bucket_name == RUSTFS_META_BUCKET
}

// Check whether the bucket is reserved
fn is_reserved_bucket(bucket_name: &str) -> bool {
    bucket_name == "rustfs"
}

// Check whether the bucket name is reserved or invalid
pub fn is_reserved_or_invalid_bucket(bucket_entry: &str, strict: bool) -> bool {
    if bucket_entry.is_empty() {
        return true;
    }

    let bucket_entry = bucket_entry.trim_end_matches('/');
    let result = check_bucket_name(bucket_entry, strict).is_err();

    result || is_meta_bucket(bucket_entry) || is_reserved_bucket(bucket_entry)
}

// Check whether the bucket name is valid
fn check_bucket_name(bucket_name: &str, strict: bool) -> Result<()> {
    if bucket_name.trim().is_empty() {
        return Err(Error::other("Bucket name cannot be empty"));
    }
    if bucket_name.len() < 3 {
        return Err(Error::other("Bucket name cannot be shorter than 3 characters"));
    }
    if bucket_name.len() > 63 {
        return Err(Error::other("Bucket name cannot be longer than 63 characters"));
    }

    let ip_address_regex = Regex::new(r"^(\d+\.){3}\d+$").unwrap();
    if ip_address_regex.is_match(bucket_name) {
        return Err(Error::other("Bucket name cannot be an IP address"));
    }

    let valid_bucket_name_regex = if strict {
        Regex::new(r"^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$").unwrap()
    } else {
        Regex::new(r"^[A-Za-z0-9][A-Za-z0-9\.\-_:]{1,61}[A-Za-z0-9]$").unwrap()
    };

    if !valid_bucket_name_regex.is_match(bucket_name) {
        return Err(Error::other("Bucket name contains invalid characters"));
    }

    // Check for "..", ".-", "-."
    if bucket_name.contains("..") || bucket_name.contains(".-") || bucket_name.contains("-.") {
        return Err(Error::other("Bucket name contains invalid characters"));
    }

    Ok(())
}
