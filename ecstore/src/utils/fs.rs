use std::{fs::Metadata, path::Path};

use tokio::{
    fs::{self, File},
    io,
};

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

type FileMode = usize;

pub const O_RDONLY: FileMode = 0x00000;
pub const O_WRONLY: FileMode = 0x00001;
pub const O_RDWR: FileMode = 0x00002;
pub const O_CREATE: FileMode = 0x00040;
// pub const O_EXCL: FileMode = 0x00080;
// pub const O_NOCTTY: FileMode = 0x00100;
pub const O_TRUNC: FileMode = 0x00200;
// pub const O_NONBLOCK: FileMode = 0x00800;
pub const O_APPEND: FileMode = 0x00400;
// pub const O_SYNC: FileMode = 0x01000;
// pub const O_ASYNC: FileMode = 0x02000;
// pub const O_CLOEXEC: FileMode = 0x80000;

//      read: bool,
//     write: bool,
//     append: bool,
//     truncate: bool,
//     create: bool,
//     create_new: bool,

pub async fn open_file(path: impl AsRef<Path>, mode: FileMode) -> io::Result<File> {
    let mut opts = fs::OpenOptions::new();

    match mode & (O_RDONLY | O_WRONLY | O_RDWR) {
        O_RDONLY => {
            opts.read(true);
        }
        O_WRONLY => {
            opts.write(true);
        }
        O_RDWR => {
            opts.read(true);
            opts.write(true);
        }
        _ => (),
    };

    if mode & O_CREATE != 0 {
        opts.create(true);
    }

    if mode & O_APPEND != 0 {
        opts.append(true);
    }

    if mode & O_TRUNC != 0 {
        opts.truncate(true);
    }

    opts.open(path.as_ref()).await
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

pub async fn remove_all(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = fs::metadata(path.as_ref()).await?;
    if meta.is_dir() {
        fs::remove_dir_all(path.as_ref()).await
    } else {
        fs::remove_file(path.as_ref()).await
    }
}

pub fn remove_std(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = std::fs::metadata(path.as_ref())?;
    if meta.is_dir() {
        std::fs::remove_dir(path.as_ref())
    } else {
        std::fs::remove_file(path.as_ref())
    }
}

pub fn remove_all_std(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = std::fs::metadata(path.as_ref())?;
    if meta.is_dir() {
        std::fs::remove_dir_all(path.as_ref())
    } else {
        std::fs::remove_file(path.as_ref())
    }
}

pub async fn mkdir(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir(path.as_ref()).await
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    fs::rename(from, to).await
}

pub async fn read_file(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    fs::read(path.as_ref()).await
}
