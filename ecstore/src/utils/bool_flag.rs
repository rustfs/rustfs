use crate::error::{Error, Result};

pub fn parse_bool(str: &str) -> Result<bool> {
    match str {
        "1" | "t" | "T" | "true" | "TRUE" | "True" | "on" | "ON" | "On" | "enabled" => {
            return Ok(true);
        }
        "0" | "f" | "F" | "false" | "FALSE" | "False" | "off" | "OFF" | "Off" | "disabled" => {
            return Ok(false);
        }
        _ => {
            return Err(Error::from_string(format!("ParseBool: parsing {}", str)));
        }
    }
}
