use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
pub enum Effect {
    #[default]
    Allow,
    Deny,
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
