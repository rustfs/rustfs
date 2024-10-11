use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub enum Effect {
    #[default]
    Allow,
    Deny,
}

impl Effect {
    pub fn is_allowed(self, b: bool) -> bool {
        if self == Effect::Allow {
            b
        } else {
            !b
        }
    }

    pub fn is_valid(self) -> bool {
        match self {
            Effect::Allow => true,
            Effect::Deny => true,
        }
    }
}

// 实现从字符串解析Effect的功能
impl FromStr for Effect {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Allow" => Ok(Effect::Allow),
            "Deny" => Ok(Effect::Deny),
            _ => Err(()),
        }
    }
}
