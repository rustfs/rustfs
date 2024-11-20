use crate::error::{Error, Result};

pub fn parse_bool(str: &str) -> Result<bool> {
    match str {
        "1" | "t" | "T" | "true" | "TRUE" | "True" | "on" | "ON" | "On" | "enabled" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" | "off" | "OFF" | "Off" | "disabled" => Ok(false),
        _ => Err(Error::from_string(format!("ParseBool: parsing {}", str))),
    }
}
