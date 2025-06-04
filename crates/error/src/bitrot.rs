use crate::Error;

pub const CHECK_PART_UNKNOWN: usize = 0;
pub const CHECK_PART_SUCCESS: usize = 1;
pub const CHECK_PART_DISK_NOT_FOUND: usize = 2;
pub const CHECK_PART_VOLUME_NOT_FOUND: usize = 3;
pub const CHECK_PART_FILE_NOT_FOUND: usize = 4;
pub const CHECK_PART_FILE_CORRUPT: usize = 5;

pub fn conv_part_err_to_int(err: &Option<Error>) -> usize {
    if let Some(err) = err {
        match err {
            Error::FileNotFound | Error::FileVersionNotFound => CHECK_PART_FILE_NOT_FOUND,
            Error::FileCorrupt => CHECK_PART_FILE_CORRUPT,
            Error::VolumeNotFound => CHECK_PART_VOLUME_NOT_FOUND,
            Error::DiskNotFound => CHECK_PART_DISK_NOT_FOUND,
            Error::Nil => CHECK_PART_SUCCESS,
            _ => CHECK_PART_UNKNOWN,
        }
    } else {
        CHECK_PART_SUCCESS
    }
}

pub fn has_part_err(part_errs: &[usize]) -> bool {
    part_errs.iter().any(|err| *err != CHECK_PART_SUCCESS)
}
