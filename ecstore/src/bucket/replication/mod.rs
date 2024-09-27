mod and;
mod filter;
mod rule;
mod tag;

use rule::Rule;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Config {
    rules: Vec<Rule>,
    role_arn: String,
}
