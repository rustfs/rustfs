use crate::Error;
pub static OBJECT_OP_IGNORED_ERRS: &[Error] = &[
    Error::DiskNotFound,
    Error::FaultyDisk,
    Error::FaultyRemoteDisk,
    Error::DiskAccessDenied,
    Error::DiskOngoingReq,
    Error::UnformattedDisk,
];

pub static BASE_IGNORED_ERRS: &[Error] = &[Error::DiskNotFound, Error::FaultyDisk, Error::FaultyRemoteDisk];
