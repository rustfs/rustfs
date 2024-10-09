use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Deserialize, Serialize)]
pub enum State {
    #[default]
    Enabled,
    Suspended,
    // 如果未来可能会使用到Disabled状态，可以在这里添加
    // Disabled,
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
                // 如果未来可能会使用到Disabled状态，可以在这里添加
                // State::Disabled => "Disabled",
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
}
