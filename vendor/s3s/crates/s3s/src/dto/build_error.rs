#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct BuildError {
    #[from]
    kind: BuildErrorKind,
}

#[derive(Debug, thiserror::Error)]
enum BuildErrorKind {
    #[error("Missing field: {field:?}")]
    MissingField { field: &'static str },
    // #[error("BuildError: {source}")]
    // Other { source: StdError },
}

impl BuildError {
    pub(crate) fn missing_field(field: &'static str) -> Self {
        Self {
            kind: BuildErrorKind::MissingField { field },
        }
    }

    // pub(crate) fn other(source: StdError) -> Self {
    //     Self {
    //         kind: BuildErrorKind::Other { source },
    //     }
    // }
}
