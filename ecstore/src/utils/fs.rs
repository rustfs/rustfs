use std::{fs::Metadata, path::Path};

use tokio::{fs, io};

#[cfg(not(windows))]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

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

#[cfg(windows)]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.file_type() != f2.file_type() {
        return false;
    }

    if f1.len() != f2.len() {
        return false;
    }
    true
}

pub async fn access(path: impl AsRef<Path>) -> io::Result<()> {
    fs::metadata(path).await?;
    Ok(())
}

pub async fn lstat(path: impl AsRef<Path>) -> io::Result<Metadata> {
    fs::metadata(path).await
}

pub async fn make_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(path.as_ref()).await
}

pub async fn remove(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = fs::metadata(path.as_ref()).await?;
    if meta.is_dir() {
        fs::remove_dir(path.as_ref()).await
    } else {
        fs::remove_file(path.as_ref()).await
    }
}

pub async fn mkdir(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir(path.as_ref()).await
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    fs::rename(from, to).await
}
