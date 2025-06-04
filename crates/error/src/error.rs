use std::hash::Hash;
use std::str::FromStr;

const ERROR_PREFIX: &str = "[RUSTFS error] ";

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Default, Debug)]
pub enum Error {
    #[default]
    #[error("[RUSTFS error] Nil")]
    Nil,
    #[error("I/O error: {0}")]
    IoError(std::io::Error),
    #[error("[RUSTFS error] Erasure Read quorum not met")]
    ErasureReadQuorum,
    #[error("[RUSTFS error] Erasure Write quorum not met")]
    ErasureWriteQuorum,

    #[error("[RUSTFS error] Disk not found")]
    DiskNotFound,
    #[error("[RUSTFS error] Faulty disk")]
    FaultyDisk,
    #[error("[RUSTFS error] Faulty remote disk")]
    FaultyRemoteDisk,
    #[error("[RUSTFS error] Unsupported disk")]
    UnsupportedDisk,
    #[error("[RUSTFS error] Unformatted disk")]
    UnformattedDisk,
    #[error("[RUSTFS error] Corrupted backend")]
    CorruptedBackend,

    #[error("[RUSTFS error] Disk access denied")]
    DiskAccessDenied,
    #[error("[RUSTFS error] Disk ongoing request")]
    DiskOngoingReq,
    #[error("[RUSTFS error] Disk full")]
    DiskFull,

    #[error("[RUSTFS error] Volume not found")]
    VolumeNotFound,
    #[error("[RUSTFS error] Volume not empty")]
    VolumeNotEmpty,
    #[error("[RUSTFS error] Volume access denied")]
    VolumeAccessDenied,

    #[error("[RUSTFS error] Volume exists")]
    VolumeExists,

    #[error("[RUSTFS error] Disk not a directory")]
    DiskNotDir,

    #[error("[RUSTFS error] File not found")]
    FileNotFound,
    #[error("[RUSTFS error] File corrupt")]
    FileCorrupt,
    #[error("[RUSTFS error] File access denied")]
    FileAccessDenied,
    #[error("[RUSTFS error] Too many open files")]
    TooManyOpenFiles,
    #[error("[RUSTFS error] Is not a regular file")]
    IsNotRegular,

    #[error("[RUSTFS error] File version not found")]
    FileVersionNotFound,

    #[error("[RUSTFS error] Less data than expected")]
    LessData,
    #[error("[RUSTFS error] Short write")]
    ShortWrite,

    #[error("[RUSTFS error] Done for now")]
    DoneForNow,

    #[error("[RUSTFS error] Method not allowed")]
    MethodNotAllowed,

    #[error("[RUSTFS error] Inconsistent disk")]
    InconsistentDisk,

    #[error("[RUSTFS error] File name too long")]
    FileNameTooLong,

    #[error("[RUSTFS error] Scan ignore file contribution")]
    ScanIgnoreFileContrib,
    #[error("[RUSTFS error] Scan skip file")]
    ScanSkipFile,
    #[error("[RUSTFS error] Scan heal stop signaled")]
    ScanHealStopSignal,
    #[error("[RUSTFS error] Scan heal idle timeout")]
    ScanHealIdleTimeout,
    #[error("[RUSTFS error] Scan retry healing")]
    ScanRetryHealing,

    #[error("[RUSTFS error] {0}")]
    Other(String),
}

// Generic From implementation removed to avoid conflicts with std::convert::From<T> for T

impl FromStr for Error {
    type Err = Error;
    fn from_str(s: &str) -> core::result::Result<Self, Self::Err> {
        // Only strip prefix for non-IoError
        let s = if s.starts_with("I/O error: ") {
            s
        } else {
            s.strip_prefix(ERROR_PREFIX).unwrap_or(s)
        };

        match s {
            "Nil" => Ok(Error::Nil),
            "ErasureReadQuorum" => Ok(Error::ErasureReadQuorum),
            "ErasureWriteQuorum" => Ok(Error::ErasureWriteQuorum),
            "DiskNotFound" | "Disk not found" => Ok(Error::DiskNotFound),
            "FaultyDisk" | "Faulty disk" => Ok(Error::FaultyDisk),
            "FaultyRemoteDisk" | "Faulty remote disk" => Ok(Error::FaultyRemoteDisk),
            "UnformattedDisk" | "Unformatted disk" => Ok(Error::UnformattedDisk),
            "DiskAccessDenied" | "Disk access denied" => Ok(Error::DiskAccessDenied),
            "DiskOngoingReq" | "Disk ongoing request" => Ok(Error::DiskOngoingReq),
            "FileNotFound" | "File not found" => Ok(Error::FileNotFound),
            "FileCorrupt" | "File corrupt" => Ok(Error::FileCorrupt),
            "FileVersionNotFound" | "File version not found" => Ok(Error::FileVersionNotFound),
            "LessData" | "Less data than expected" => Ok(Error::LessData),
            "ShortWrite" | "Short write" => Ok(Error::ShortWrite),
            "VolumeNotFound" | "Volume not found" => Ok(Error::VolumeNotFound),
            "VolumeNotEmpty" | "Volume not empty" => Ok(Error::VolumeNotEmpty),
            "VolumeExists" | "Volume exists" => Ok(Error::VolumeExists),
            "VolumeAccessDenied" | "Volume access denied" => Ok(Error::VolumeAccessDenied),
            "DiskNotDir" | "Disk not a directory" => Ok(Error::DiskNotDir),
            "FileAccessDenied" | "File access denied" => Ok(Error::FileAccessDenied),
            "TooManyOpenFiles" | "Too many open files" => Ok(Error::TooManyOpenFiles),
            "IsNotRegular" | "Is not a regular file" => Ok(Error::IsNotRegular),
            "CorruptedBackend" | "Corrupted backend" => Ok(Error::CorruptedBackend),
            "UnsupportedDisk" | "Unsupported disk" => Ok(Error::UnsupportedDisk),
            "InconsistentDisk" | "Inconsistent disk" => Ok(Error::InconsistentDisk),
            "DiskFull" | "Disk full" => Ok(Error::DiskFull),
            "FileNameTooLong" | "File name too long" => Ok(Error::FileNameTooLong),
            "ScanIgnoreFileContrib" | "Scan ignore file contribution" => Ok(Error::ScanIgnoreFileContrib),
            "ScanSkipFile" | "Scan skip file" => Ok(Error::ScanSkipFile),
            "ScanHealStopSignal" | "Scan heal stop signaled" => Ok(Error::ScanHealStopSignal),
            "ScanHealIdleTimeout" | "Scan heal idle timeout" => Ok(Error::ScanHealIdleTimeout),
            "ScanRetryHealing" | "Scan retry healing" => Ok(Error::ScanRetryHealing),
            s if s.starts_with("I/O error: ") => {
                Ok(Error::IoError(std::io::Error::other(s.strip_prefix("I/O error: ").unwrap_or(""))))
            }
            "DoneForNow" | "Done for now" => Ok(Error::DoneForNow),
            "MethodNotAllowed" | "Method not allowed" => Ok(Error::MethodNotAllowed),
            str => Err(Error::IoError(std::io::Error::other(str.to_string()))),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::IoError(e) => e,
            e => std::io::Error::other(e),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            // convert Error from string to Error
            std::io::ErrorKind::Other => Error::from(e.to_string()),
            _ => Error::IoError(e),
        }
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::from_str(&s).unwrap_or(Error::IoError(std::io::Error::other(s)))
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::from_str(s).unwrap_or(Error::IoError(std::io::Error::other(s)))
    }
}

// Common error type conversions for ? operator
impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::Other(format!("Parse int error: {}", e))
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(e: std::num::ParseFloatError) -> Self {
        Error::Other(format!("Parse float error: {}", e))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Other(format!("UTF-8 error: {}", e))
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::Other(format!("UTF-8 conversion error: {}", e))
    }
}

impl From<std::fmt::Error> for Error {
    fn from(e: std::fmt::Error) -> Self {
        Error::Other(format!("Format error: {}", e))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Other(e.to_string())
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(e: time::error::ComponentRange) -> Self {
        Error::Other(format!("Time component range error: {}", e))
    }
}

impl From<rmp::decode::NumValueReadError<std::io::Error>> for Error {
    fn from(e: rmp::decode::NumValueReadError<std::io::Error>) -> Self {
        Error::Other(format!("NumValueReadError: {}", e))
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(e: rmp::encode::ValueWriteError) -> Self {
        Error::Other(format!("ValueWriteError: {}", e))
    }
}

impl From<rmp::decode::ValueReadError> for Error {
    fn from(e: rmp::decode::ValueReadError) -> Self {
        Error::Other(format!("ValueReadError: {}", e))
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Error::Other(format!("UUID error: {}", e))
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::Other(format!("rmp_serde::decode::Error: {}", e))
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::Other(format!("rmp_serde::encode::Error: {}", e))
    }
}

impl From<serde::de::value::Error> for Error {
    fn from(e: serde::de::value::Error) -> Self {
        Error::Other(format!("serde::de::value::Error: {}", e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Other(format!("serde_json::Error: {}", e))
    }
}

impl From<std::collections::TryReserveError> for Error {
    fn from(e: std::collections::TryReserveError) -> Self {
        Error::Other(format!("TryReserveError: {}", e))
    }
}

impl From<tonic::Status> for Error {
    fn from(e: tonic::Status) -> Self {
        Error::Other(format!("tonic::Status: {}", e.message()))
    }
}

impl From<protos::proto_gen::node_service::Error> for Error {
    fn from(e: protos::proto_gen::node_service::Error) -> Self {
        Error::from_str(&e.error_info).unwrap_or(Error::Other(format!("Proto_Error: {}", e.error_info)))
    }
}

impl From<Error> for protos::proto_gen::node_service::Error {
    fn from(val: Error) -> Self {
        protos::proto_gen::node_service::Error {
            code: 0,
            error_info: val.to_string(),
        }
    }
}

impl Hash for Error {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Error::IoError(err) => {
                err.kind().hash(state);
                err.to_string().hash(state);
            }
            e => e.to_string().hash(state),
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Self {
        match self {
            Error::IoError(err) => Error::IoError(std::io::Error::new(err.kind(), err.to_string())),
            Error::ErasureReadQuorum => Error::ErasureReadQuorum,
            Error::ErasureWriteQuorum => Error::ErasureWriteQuorum,
            Error::DiskNotFound => Error::DiskNotFound,
            Error::FaultyDisk => Error::FaultyDisk,
            Error::FaultyRemoteDisk => Error::FaultyRemoteDisk,
            Error::UnformattedDisk => Error::UnformattedDisk,
            Error::DiskAccessDenied => Error::DiskAccessDenied,
            Error::DiskOngoingReq => Error::DiskOngoingReq,
            Error::FileNotFound => Error::FileNotFound,
            Error::FileCorrupt => Error::FileCorrupt,
            Error::FileVersionNotFound => Error::FileVersionNotFound,
            Error::LessData => Error::LessData,
            Error::ShortWrite => Error::ShortWrite,
            Error::VolumeNotFound => Error::VolumeNotFound,
            Error::VolumeNotEmpty => Error::VolumeNotEmpty,
            Error::VolumeAccessDenied => Error::VolumeAccessDenied,
            Error::VolumeExists => Error::VolumeExists,
            Error::DiskNotDir => Error::DiskNotDir,
            Error::FileAccessDenied => Error::FileAccessDenied,
            Error::TooManyOpenFiles => Error::TooManyOpenFiles,
            Error::IsNotRegular => Error::IsNotRegular,
            Error::CorruptedBackend => Error::CorruptedBackend,
            Error::UnsupportedDisk => Error::UnsupportedDisk,
            Error::DiskFull => Error::DiskFull,
            Error::Nil => Error::Nil,
            Error::DoneForNow => Error::DoneForNow,
            Error::MethodNotAllowed => Error::MethodNotAllowed,
            Error::InconsistentDisk => Error::InconsistentDisk,
            Error::FileNameTooLong => Error::FileNameTooLong,
            Error::ScanIgnoreFileContrib => Error::ScanIgnoreFileContrib,
            Error::ScanSkipFile => Error::ScanSkipFile,
            Error::ScanHealStopSignal => Error::ScanHealStopSignal,
            Error::ScanHealIdleTimeout => Error::ScanHealIdleTimeout,
            Error::ScanRetryHealing => Error::ScanRetryHealing,
            Error::Other(msg) => Error::Other(msg.clone()),
        }
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::IoError(e1), Error::IoError(e2)) => e1.kind() == e2.kind() && e1.to_string() == e2.to_string(),
            (Error::ErasureReadQuorum, Error::ErasureReadQuorum) => true,
            (Error::ErasureWriteQuorum, Error::ErasureWriteQuorum) => true,
            (Error::DiskNotFound, Error::DiskNotFound) => true,
            (Error::FaultyDisk, Error::FaultyDisk) => true,
            (Error::FaultyRemoteDisk, Error::FaultyRemoteDisk) => true,
            (Error::UnformattedDisk, Error::UnformattedDisk) => true,
            (Error::DiskAccessDenied, Error::DiskAccessDenied) => true,
            (Error::DiskOngoingReq, Error::DiskOngoingReq) => true,
            (Error::FileNotFound, Error::FileNotFound) => true,
            (Error::FileCorrupt, Error::FileCorrupt) => true,
            (Error::FileVersionNotFound, Error::FileVersionNotFound) => true,
            (Error::LessData, Error::LessData) => true,
            (Error::ShortWrite, Error::ShortWrite) => true,
            (Error::VolumeNotFound, Error::VolumeNotFound) => true,
            (Error::VolumeNotEmpty, Error::VolumeNotEmpty) => true,
            (Error::VolumeAccessDenied, Error::VolumeAccessDenied) => true,
            (Error::VolumeExists, Error::VolumeExists) => true,
            (Error::DiskNotDir, Error::DiskNotDir) => true,
            (Error::FileAccessDenied, Error::FileAccessDenied) => true,
            (Error::TooManyOpenFiles, Error::TooManyOpenFiles) => true,
            (Error::IsNotRegular, Error::IsNotRegular) => true,
            (Error::CorruptedBackend, Error::CorruptedBackend) => true,
            (Error::UnsupportedDisk, Error::UnsupportedDisk) => true,
            (Error::DiskFull, Error::DiskFull) => true,
            (Error::Nil, Error::Nil) => true,
            (Error::DoneForNow, Error::DoneForNow) => true,
            (Error::MethodNotAllowed, Error::MethodNotAllowed) => true,
            (Error::InconsistentDisk, Error::InconsistentDisk) => true,
            (Error::FileNameTooLong, Error::FileNameTooLong) => true,
            (Error::ScanIgnoreFileContrib, Error::ScanIgnoreFileContrib) => true,
            (Error::ScanSkipFile, Error::ScanSkipFile) => true,
            (Error::ScanHealStopSignal, Error::ScanHealStopSignal) => true,
            (Error::ScanHealIdleTimeout, Error::ScanHealIdleTimeout) => true,
            (Error::ScanRetryHealing, Error::ScanRetryHealing) => true,
            (Error::Other(s1), Error::Other(s2)) => s1 == s2,
            _ => false,
        }
    }
}

impl Eq for Error {}

impl Error {
    /// Create an error from a message string (for backward compatibility)
    pub fn msg<S: Into<String>>(message: S) -> Self {
        Error::Other(message.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::io;

    #[test]
    fn test_display_and_debug() {
        let e = Error::DiskNotFound;
        assert_eq!(format!("{}", e), format!("{ERROR_PREFIX}Disk not found"));
        assert_eq!(format!("{:?}", e), "DiskNotFound");
        let io_err = Error::IoError(io::Error::other("fail"));
        assert_eq!(format!("{}", io_err), "I/O error: fail");
    }

    #[test]
    fn test_partial_eq_and_eq() {
        assert_eq!(Error::DiskNotFound, Error::DiskNotFound);
        assert_ne!(Error::DiskNotFound, Error::FaultyDisk);
        let e1 = Error::IoError(io::Error::other("fail"));
        let e2 = Error::IoError(io::Error::other("fail"));
        assert_eq!(e1, e2);
        let e3 = Error::IoError(io::Error::new(io::ErrorKind::NotFound, "fail"));
        assert_ne!(e1, e3);
    }

    #[test]
    fn test_clone() {
        let e = Error::DiskAccessDenied;
        let cloned = e.clone();
        assert_eq!(e, cloned);
        let io_err = Error::IoError(io::Error::other("fail"));
        let cloned_io = io_err.clone();
        assert_eq!(io_err, cloned_io);
    }

    #[test]
    fn test_hash() {
        let e1 = Error::DiskNotFound;
        let e2 = Error::DiskNotFound;
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        e1.hash(&mut h1);
        e2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
        let io_err1 = Error::IoError(io::Error::other("fail"));
        let io_err2 = Error::IoError(io::Error::other("fail"));
        let mut h3 = DefaultHasher::new();
        let mut h4 = DefaultHasher::new();
        io_err1.hash(&mut h3);
        io_err2.hash(&mut h4);
        assert_eq!(h3.finish(), h4.finish());
    }

    #[test]
    fn test_from_error_for_io_error() {
        let e = Error::DiskNotFound;
        let io_err: io::Error = e.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Other);
        assert_eq!(io_err.to_string(), format!("{ERROR_PREFIX}Disk not found"));

        assert_eq!(Error::from(io_err.to_string()), Error::DiskNotFound);

        let orig = io::Error::other("fail");
        let e2 = Error::IoError(orig.kind().into());
        let io_err2: io::Error = e2.into();
        assert_eq!(io_err2.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn test_from_io_error_for_error() {
        let orig = io::Error::other("fail");
        let e: Error = orig.into();
        match e {
            Error::IoError(ioe) => {
                assert_eq!(ioe.kind(), io::ErrorKind::Other);
                assert_eq!(ioe.to_string(), "fail");
            }
            _ => panic!("Expected IoError variant"),
        }
    }

    #[test]
    fn test_default() {
        let e = Error::default();
        assert_eq!(e, Error::Nil);
    }

    #[test]
    fn test_from_str() {
        use std::str::FromStr;
        assert_eq!(Error::from_str("Nil"), Ok(Error::Nil));
        assert_eq!(Error::from_str("DiskNotFound"), Ok(Error::DiskNotFound));
        assert_eq!(Error::from_str("ErasureReadQuorum"), Ok(Error::ErasureReadQuorum));
        assert_eq!(Error::from_str("I/O error: fail"), Ok(Error::IoError(io::Error::other("fail"))));
        assert_eq!(Error::from_str(&format!("{ERROR_PREFIX}Disk not found")), Ok(Error::DiskNotFound));
        assert_eq!(
            Error::from_str("UnknownError"),
            Err(Error::IoError(std::io::Error::other("UnknownError")))
        );
    }

    #[test]
    fn test_from_string() {
        let e: Error = format!("{ERROR_PREFIX}Disk not found").parse().unwrap();
        assert_eq!(e, Error::DiskNotFound);
        let e2: Error = "I/O error: fail".to_string().parse().unwrap();
        assert_eq!(e2, Error::IoError(std::io::Error::other("fail")));
    }

    #[test]
    fn test_from_io_error() {
        let e = Error::IoError(io::Error::other("fail"));
        let io_err: io::Error = e.clone().into();
        assert_eq!(io_err.to_string(), "fail");

        let e2: Error = io::Error::other("fail").into();
        assert_eq!(e2, Error::IoError(io::Error::other("fail")));

        let result = Error::from(io::Error::other("fail"));
        assert_eq!(result, Error::IoError(io::Error::other("fail")));

        let io_err2: std::io::Error = Error::CorruptedBackend.into();
        assert_eq!(io_err2.to_string(), "[RUSTFS error] Corrupted backend");

        assert_eq!(Error::from(io_err2), Error::CorruptedBackend);

        let io_err3: std::io::Error = Error::DiskNotFound.into();
        assert_eq!(io_err3.to_string(), "[RUSTFS error] Disk not found");

        assert_eq!(Error::from(io_err3), Error::DiskNotFound);

        let io_err4: std::io::Error = Error::DiskAccessDenied.into();
        assert_eq!(io_err4.to_string(), "[RUSTFS error] Disk access denied");

        assert_eq!(Error::from(io_err4), Error::DiskAccessDenied);
    }

    #[test]
    fn test_question_mark_operator() {
        fn parse_number(s: &str) -> Result<i32> {
            let num = s.parse::<i32>()?; // ParseIntError automatically converts to Error
            Ok(num)
        }

        fn format_string() -> Result<String> {
            use std::fmt::Write;
            let mut s = String::new();
            write!(&mut s, "test")?; // fmt::Error automatically converts to Error
            Ok(s)
        }

        fn utf8_conversion() -> Result<String> {
            let bytes = vec![0xFF, 0xFE]; // Invalid UTF-8
            let s = String::from_utf8(bytes)?; // FromUtf8Error automatically converts to Error
            Ok(s)
        }

        // Test successful case
        assert_eq!(parse_number("42").unwrap(), 42);

        // Test error conversion
        let err = parse_number("not_a_number").unwrap_err();
        assert!(matches!(err, Error::Other(_)));
        assert!(err.to_string().contains("Parse int error"));

        // Test format error conversion
        assert_eq!(format_string().unwrap(), "test");

        // Test UTF-8 error conversion
        let err = utf8_conversion().unwrap_err();
        assert!(matches!(err, Error::Other(_)));
        assert!(err.to_string().contains("UTF-8 conversion error"));
    }
}
