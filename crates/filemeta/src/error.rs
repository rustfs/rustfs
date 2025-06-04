pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("File not found")]
    FileNotFound,
    #[error("File version not found")]
    FileVersionNotFound,

    #[error("File corrupt")]
    FileCorrupt,

    #[error("Done for now")]
    DoneForNow,

    #[error("Method not allowed")]
    MethodNotAllowed,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("rmp serde decode error: {0}")]
    RmpSerdeDecode(String),

    #[error("rmp serde encode error: {0}")]
    RmpSerdeEncode(String),

    #[error("Invalid UTF-8: {0}")]
    FromUtf8(String),

    #[error("rmp decode value read error: {0}")]
    RmpDecodeValueRead(String),

    #[error("rmp encode value write error: {0}")]
    RmpEncodeValueWrite(String),

    #[error("rmp decode num value read error: {0}")]
    RmpDecodeNumValueRead(String),

    #[error("rmp decode marker read error: {0}")]
    RmpDecodeMarkerRead(String),

    #[error("time component range error: {0}")]
    TimeComponentRange(String),

    #[error("uuid parse error: {0}")]
    UuidParse(String),
}

impl Error {
    pub fn other<E>(error: E) -> Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        std::io::Error::other(error).into()
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::FileCorrupt, Error::FileCorrupt) => true,
            (Error::DoneForNow, Error::DoneForNow) => true,
            (Error::MethodNotAllowed, Error::MethodNotAllowed) => true,
            (Error::FileNotFound, Error::FileNotFound) => true,
            (Error::FileVersionNotFound, Error::FileVersionNotFound) => true,
            (Error::Io(e1), Error::Io(e2)) => e1 == e2,
            (Error::RmpSerdeDecode(e1), Error::RmpSerdeDecode(e2)) => e1 == e2,
            (Error::RmpSerdeEncode(e1), Error::RmpSerdeEncode(e2)) => e1 == e2,
            (Error::RmpDecodeValueRead(e1), Error::RmpDecodeValueRead(e2)) => e1 == e2,
            (Error::RmpEncodeValueWrite(e1), Error::RmpEncodeValueWrite(e2)) => e1 == e2,
            (Error::RmpDecodeNumValueRead(e1), Error::RmpDecodeNumValueRead(e2)) => e1 == e2,
            (Error::TimeComponentRange(e1), Error::TimeComponentRange(e2)) => e1 == e2,
            (Error::UuidParse(e1), Error::UuidParse(e2)) => e1 == e2,
            (a, b) => a.to_string() == b.to_string(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Error::RmpSerdeDecode(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::RmpSerdeEncode(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Error::FromUtf8(e.to_string())
    }
}

impl From<rmp::decode::ValueReadError> for Error {
    fn from(e: rmp::decode::ValueReadError) -> Self {
        Error::RmpDecodeValueRead(e.to_string())
    }
}

impl From<rmp::encode::ValueWriteError> for Error {
    fn from(e: rmp::encode::ValueWriteError) -> Self {
        Error::RmpEncodeValueWrite(e.to_string())
    }
}

impl From<rmp::decode::NumValueReadError> for Error {
    fn from(e: rmp::decode::NumValueReadError) -> Self {
        Error::RmpDecodeNumValueRead(e.to_string())
    }
}

impl From<time::error::ComponentRange> for Error {
    fn from(e: time::error::ComponentRange) -> Self {
        Error::TimeComponentRange(e.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Error::UuidParse(e.to_string())
    }
}

impl From<rmp::decode::MarkerReadError> for Error {
    fn from(e: rmp::decode::MarkerReadError) -> Self {
        let serr = format!("{:?}", e);
        Error::RmpDecodeMarkerRead(serr)
    }
}
