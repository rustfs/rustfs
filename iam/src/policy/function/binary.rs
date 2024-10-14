use serde::{Deserialize, Serialize};

use super::func::InnerFunc;

pub type BinaryFunc = InnerFunc<BinaryFuncValue>;

// todo implement it
#[derive(Serialize, Deserialize, Clone)]
#[serde(transparent)]
pub struct BinaryFuncValue(String);
