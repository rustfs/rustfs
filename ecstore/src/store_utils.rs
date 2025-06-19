use crate::config::storageclass::STANDARD;
use rustfs_filemeta::headers::AMZ_OBJECT_TAGGING;
use rustfs_filemeta::headers::AMZ_STORAGE_CLASS;
use std::collections::HashMap;

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
