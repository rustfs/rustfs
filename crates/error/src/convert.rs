use crate::Error;

pub fn to_file_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => Error::FileNotFound.into(),
        std::io::ErrorKind::PermissionDenied => Error::FileAccessDenied.into(),
        std::io::ErrorKind::IsADirectory => Error::IsNotRegular.into(),
        std::io::ErrorKind::NotADirectory => Error::FileAccessDenied.into(),
        std::io::ErrorKind::DirectoryNotEmpty => Error::FileAccessDenied.into(),
        std::io::ErrorKind::UnexpectedEof => Error::FaultyDisk.into(),
        std::io::ErrorKind::TooManyLinks => Error::TooManyOpenFiles.into(),
        std::io::ErrorKind::InvalidInput => Error::FileNotFound.into(),
        std::io::ErrorKind::InvalidData => Error::FileCorrupt.into(),
        std::io::ErrorKind::StorageFull => Error::DiskFull.into(),
        _ => io_err,
    }
}

pub fn to_volume_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => Error::VolumeNotFound.into(),
        std::io::ErrorKind::PermissionDenied => Error::DiskAccessDenied.into(),
        std::io::ErrorKind::DirectoryNotEmpty => Error::VolumeNotEmpty.into(),
        std::io::ErrorKind::NotADirectory => Error::IsNotRegular.into(),
        std::io::ErrorKind::Other => {
            let err = Error::from(io_err.to_string());
            match err {
                Error::FileNotFound => Error::VolumeNotFound.into(),
                Error::FileAccessDenied => Error::DiskAccessDenied.into(),
                _ => to_file_error(io_err),
            }
        }
        _ => to_file_error(io_err),
    }
}

pub fn to_disk_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => Error::DiskNotFound.into(),
        std::io::ErrorKind::PermissionDenied => Error::DiskAccessDenied.into(),
        std::io::ErrorKind::Other => {
            let err = Error::from(io_err.to_string());
            match err {
                Error::FileNotFound => Error::DiskNotFound.into(),
                Error::VolumeNotFound => Error::DiskNotFound.into(),
                Error::FileAccessDenied => Error::DiskAccessDenied.into(),
                Error::VolumeAccessDenied => Error::DiskAccessDenied.into(),
                _ => to_volume_error(io_err),
            }
        }
        _ => to_volume_error(io_err),
    }
}

// only errors from FileSystem operations
pub fn to_access_error(io_err: std::io::Error, per_err: Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::PermissionDenied => per_err.into(),
        std::io::ErrorKind::NotADirectory => per_err.into(),
        std::io::ErrorKind::NotFound => Error::VolumeNotFound.into(),
        std::io::ErrorKind::UnexpectedEof => Error::FaultyDisk.into(),
        std::io::ErrorKind::Other => {
            let err = Error::from(io_err.to_string());
            match err {
                Error::DiskAccessDenied => per_err.into(),
                Error::FileAccessDenied => per_err.into(),
                Error::FileNotFound => Error::VolumeNotFound.into(),
                _ => to_volume_error(io_err),
            }
        }
        _ => to_volume_error(io_err),
    }
}

pub fn to_unformatted_disk_error(io_err: std::io::Error) -> std::io::Error {
    match io_err.kind() {
        std::io::ErrorKind::NotFound => Error::UnformattedDisk.into(),
        std::io::ErrorKind::PermissionDenied => Error::DiskAccessDenied.into(),
        std::io::ErrorKind::Other => {
            let err = Error::from(io_err.to_string());
            match err {
                Error::FileNotFound => Error::UnformattedDisk.into(),
                Error::DiskNotFound => Error::UnformattedDisk.into(),
                Error::VolumeNotFound => Error::UnformattedDisk.into(),
                Error::FileAccessDenied => Error::DiskAccessDenied.into(),
                Error::DiskAccessDenied => Error::DiskAccessDenied.into(),
                _ => Error::CorruptedBackend.into(),
            }
        }
        _ => Error::CorruptedBackend.into(),
    }
}
