use std::{fs::Metadata,  path::Path};

use tokio::{fs, io};

#[cfg(target_os = "linux")]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
   use os::unix::fs::MetadataExt;

    if f1.dev() != f2.dev() {
        return false;
    }

    if f1.ino() != f2.ino() {
        return false;
    }

    if f1.size() != f2.size() {
        return false;
    }
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.mtime() != f2.mtime() {
        return false;
    }

    true
}

#[cfg(target_os = "windows")]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    if f1.permissions() != f2.permissions() {
        return  false;
    }

    
    if f1.file_type() != f2.file_type() {
        return  false;
    }

    if f1.len() != f2.len() {
        return false;
    }
    true
}

pub async fn access(path: impl AsRef<Path>) -> io::Result<()>{
    fs::metadata(path).await?;
    Ok(())
}

pub async fn make_dir_all(path: impl AsRef<Path>) -> io::Result<()>{
    fs::create_dir_all(path.as_ref()).await
}