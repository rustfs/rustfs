pub type Result<T> = std::result::Result<T, PyroscopeError>;

/// Error type of Pyroscope
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum PyroscopeError {
    #[error("Other: {}", &.0)]
    AdHoc(String),

    #[error("{msg}: {source:?}")]
    Compat {
        msg: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("BackendImpl error")]
    BackendImpl,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ParseError(#[from] url::ParseError),

    #[error(transparent)]
    TimeSource(#[from] std::time::SystemTimeError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl PyroscopeError {
    /// Create a new instance of PyroscopeError
    pub fn new(msg: &str) -> Self {
        PyroscopeError::AdHoc(msg.to_string())
    }

    /// Create a new instance of PyroscopeError with source
    pub fn new_with_source<E>(msg: &str, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        PyroscopeError::Compat {
            msg: msg.to_string(),
            source: Box::new(source),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for PyroscopeError {
    fn from(_err: std::sync::PoisonError<T>) -> Self {
        PyroscopeError::AdHoc("Poison Error".to_owned())
    }
}

impl<T: 'static + Send + Sync> From<std::sync::mpsc::SendError<T>> for PyroscopeError {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        PyroscopeError::Compat {
            msg: String::from("SendError Error"),
            source: Box::new(err),
        }
    }
}
