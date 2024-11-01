use http::{HeaderMap, HeaderValue};
use uuid::Uuid;

use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::error::{Error, Result};
use crate::store_api::ObjectOptions;
use crate::store_err::StorageError;
use crate::utils::path::is_dir_object;
use std::collections::HashMap;

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

pub fn put_opts_from_headers(
    headers: &HeaderMap<HeaderValue>,
    metadata: Option<HashMap<String, String>>,
) -> Result<ObjectOptions> {
    // TODO custom headers
    get_default_opts(headers, metadata, false)
}

fn get_default_opts(
    _headers: &HeaderMap<HeaderValue>,
    _metadata: Option<HashMap<String, String>>,
    _copy_source: bool,
) -> Result<ObjectOptions> {
    Ok(ObjectOptions::default())
}
