use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::func::InnerFunc;

pub type BinaryFunc = InnerFunc<BinaryFuncValue>;

// todo implement it
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(transparent)]
pub struct BinaryFuncValue(String);

impl BinaryFunc {
    pub fn evaluate(&self, _values: &HashMap<String, Vec<String>>) -> bool {
        todo!()
    }
}
