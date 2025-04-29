use common::error::{Error, Result};
use ecstore::bucket::versioning_sys::BucketVersioningSys;
use ecstore::store_api::ObjectOptions;
use ecstore::store_err::StorageError;
use ecstore::utils::path::is_dir_object;
use http::{HeaderMap, HeaderValue};
use lazy_static::lazy_static;
use std::collections::HashMap;
use uuid::Uuid;

/// Creates options for deleting an object in a bucket.
pub async fn del_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::suspended(bucket).await;

    // TODO: delete_prefix

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(Error::new(StorageError::InvalidVersionID(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }

        if !versioned {
            return Err(Error::new(StorageError::InvalidArgument(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }
    }

    let mut opts = put_opts_from_headers(headers, metadata)
        .map_err(|err| Error::new(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for getting an object from a bucket.
pub async fn get_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    part_num: Option<usize>,
    headers: &HeaderMap<HeaderValue>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(Error::new(StorageError::InvalidVersionID(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }

        if !versioned {
            return Err(Error::new(StorageError::InvalidArgument(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }
    }

    let mut opts = get_default_opts(headers, None, false)
        .map_err(|err| Error::new(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
        } else {
            vid
        }
    };

    opts.part_number = part_num;

    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for putting an object in a bucket.
pub async fn put_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
) -> Result<ObjectOptions> {
    let versioned = BucketVersioningSys::prefix_enabled(bucket, object).await;
    let version_suspended = BucketVersioningSys::prefix_suspended(bucket, object).await;

    let vid = vid.map(|v| v.as_str().trim().to_owned());

    if let Some(ref id) = vid {
        if let Err(_err) = Uuid::parse_str(id.as_str()) {
            return Err(Error::new(StorageError::InvalidVersionID(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }

        if !versioned {
            return Err(Error::new(StorageError::InvalidArgument(
                bucket.to_owned(),
                object.to_owned(),
                id.clone(),
            )));
        }
    }

    let mut opts = put_opts_from_headers(headers, metadata)
        .map_err(|err| Error::new(StorageError::InvalidArgument(bucket.to_owned(), object.to_owned(), err.to_string())))?;

    opts.version_id = {
        if is_dir_object(object) && vid.is_none() {
            Some(Uuid::nil().to_string())
        } else {
            vid
        }
    };
    opts.version_suspended = version_suspended;
    opts.versioned = versioned;

    Ok(opts)
}

/// Creates options for copying an object in a bucket.
pub async fn copy_dst_opts(
    bucket: &str,
    object: &str,
    vid: Option<String>,
    headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
) -> Result<ObjectOptions> {
    put_opts(bucket, object, vid, headers, metadata).await
}

pub fn copy_src_opts(_bucket: &str, _object: &str, headers: &HeaderMap<HeaderValue>) -> Result<ObjectOptions> {
    get_default_opts(headers, None, false)
}

pub fn put_opts_from_headers(
    headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
) -> Result<ObjectOptions> {
    get_default_opts(headers, metadata, false)
}

/// Creates default options for getting an object from a bucket.
pub fn get_default_opts(
    _headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
    _copy_source: bool,
) -> Result<ObjectOptions> {
    Ok(ObjectOptions {
        user_defined: metadata.clone(),
        ..Default::default()
    })
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata(headers: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    extract_metadata_from_mime(headers, &mut metadata);

    metadata
}

/// Extracts metadata from headers and returns it as a HashMap.
pub fn extract_metadata_from_mime(headers: &HeaderMap<HeaderValue>, metadata: &mut HashMap<String, String>) {
    for (k, v) in headers.iter() {
        if let Some(key) = k.as_str().strip_prefix("x-amz-meta-") {
            if key.is_empty() {
                continue;
            }

            metadata.insert(key.to_owned(), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        if let Some(key) = k.as_str().strip_prefix("x-rustfs-meta-") {
            metadata.insert(key.to_owned(), String::from_utf8_lossy(v.as_bytes()).to_string());
            continue;
        }

        for hd in SUPPORTED_HEADERS.iter() {
            if k.as_str() == *hd {
                metadata.insert(k.to_string(), String::from_utf8_lossy(v.as_bytes()).to_string());
                continue;
            }
        }
    }

    if !metadata.contains_key("content-type") {
        metadata.insert("content-type".to_owned(), "binary/octet-stream".to_owned());
    }
}

lazy_static! {
    /// List of supported headers.
    static ref SUPPORTED_HEADERS: Vec<&'static str> = vec![
        "content-type",
        "cache-control",
        "content-language",
        "content-encoding",
        "content-disposition",
        "x-amz-storage-class",
        "x-amz-tagging",
        "expires",
        "x-amz-replication-status"
    ];
}
