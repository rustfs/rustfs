use std::{fs::Metadata, path::Path};

use rustfs_error::{to_file_error, Error, Result};

pub async fn read_file_exists(path: impl AsRef<Path>) -> Result<(Vec<u8>, Option<Metadata>)> {
    let p = path.as_ref();
    let (data, meta) = match read_file_all(&p).await {
        Ok((data, meta)) => (data, Some(meta)),
        Err(e) => {
            if e == Error::FileNotFound {
                (Vec::new(), None)
            } else {
                return Err(e);
            }
        }
    };

    Ok((data, meta))
}

pub async fn read_file_all(path: impl AsRef<Path>) -> Result<(Vec<u8>, Metadata)> {
    let p = path.as_ref();
    let meta = read_file_metadata(&path).await?;

    let data = read_all(&p).await?;

    Ok((data, meta))
}

pub async fn read_file_metadata(p: impl AsRef<Path>) -> Result<Metadata> {
    Ok(tokio::fs::metadata(&p).await.map_err(to_file_error)?)
}
pub async fn read_all(p: impl AsRef<Path>) -> Result<Vec<u8>> {
    tokio::fs::read(&p).await.map_err(|e| to_file_error(e).into())
}
