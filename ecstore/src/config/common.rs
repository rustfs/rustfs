use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::store::ECStore;
use crate::store_api::{HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader};
use http::HeaderMap;
use s3s::dto::StreamingBlob;
use s3s::Body;
use tracing::warn;

use super::error::ConfigError;

pub async fn read_config(api: &ECStore, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(api, file, &ObjectOptions::default()).await?;

    Ok(data)
}

async fn read_config_with_metadata(api: &ECStore, file: &str, opts: &ObjectOptions) -> Result<(Vec<u8>, ObjectInfo)> {
    let range = HTTPRangeSpec::nil();
    let h = HeaderMap::new();
    let mut rd = api.get_object_reader(RUSTFS_META_BUCKET, file, range, h, &opts).await?;

    let data = rd.read_all().await?;

    if data.is_empty() {
        return Err(Error::new(ConfigError::NotFound));
    }

    Ok((data, rd.object_info))
}

pub async fn save_config(api: &ECStore, file: &str, data: &[u8]) -> Result<()> {
    save_config_with_opts(
        api,
        file,
        data,
        &ObjectOptions {
            max_parity: true,
            ..Default::default()
        },
    )
    .await
}

async fn save_config_with_opts(api: &ECStore, file: &str, data: &[u8], opts: &ObjectOptions) -> Result<()> {
    let _ = api
        .put_object(
            RUSTFS_META_BUCKET,
            file,
            PutObjReader::new(StreamingBlob::from(Body::from(data.to_vec())), data.len()),
            opts,
        )
        .await?;
    Ok(())
}
