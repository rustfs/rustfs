use crate::{
    error::{Error, Result},
    utils,
};
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum VersioningErr {
    #[error("too many excluded prefixes")]
    TooManyExcludedPrefixes,
    #[error("excluded prefixes extension supported only when versioning is enabled")]
    ExcludedPrefixNotSupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Deserialize, Serialize)]
pub enum State {
    #[default]
    Suspended,
    Enabled,
}

// 实现Display trait用于打印
impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                State::Enabled => "Enabled",
                State::Suspended => "Suspended",
            }
        )
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ExcludedPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Versioning {
    pub status: State,
    pub excluded_prefixes: Vec<ExcludedPrefix>,
    pub exclude_folders: bool,
}

impl Versioning {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Versioning = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn validate(&self) -> Result<()> {
        match self.status {
            State::Suspended => {
                if !self.excluded_prefixes.is_empty() {
                    return Err(Error::new(VersioningErr::ExcludedPrefixNotSupported));
                }
            }
            State::Enabled => {
                if self.excluded_prefixes.len() > 10 {
                    return Err(Error::new(VersioningErr::TooManyExcludedPrefixes));
                }
            }
        }

        Ok(())
    }

    pub fn enabled(&self) -> bool {
        self.status == State::Enabled
    }

    pub fn versioned(&self, prefix: &str) -> bool {
        self.prefix_enabled(prefix) || self.prefix_suspended(prefix)
    }

    pub fn prefix_enabled(&self, prefix: &str) -> bool {
        if self.status != State::Enabled {
            return false;
        }

        if prefix.is_empty() {
            return true;
        }
        if self.exclude_folders && prefix.ends_with("/") {
            return false;
        }

        for sprefix in self.excluded_prefixes.iter() {
            let full_prefix = format!("{}*", sprefix.prefix);
            if utils::wildcard::match_simple(&full_prefix, prefix) {
                return false;
            }
        }
        true
    }

    pub fn suspended(&self) -> bool {
        self.status == State::Suspended
    }

    pub fn prefix_suspended(&self, prefix: &str) -> bool {
        if self.status == State::Suspended {
            return true;
        }

        if self.status == State::Enabled {
            if prefix.is_empty() {
                return false;
            }

            if self.exclude_folders && prefix.starts_with("/") {
                return true;
            }

            for sprefix in self.excluded_prefixes.iter() {
                let full_prefix = format!("{}*", sprefix.prefix);
                if utils::wildcard::match_simple(&full_prefix, prefix) {
                    return true;
                }
            }
        }
        false
    }

    pub fn prefixes_excluded(&self) -> bool {
        !self.excluded_prefixes.is_empty() || self.exclude_folders
    }
}
