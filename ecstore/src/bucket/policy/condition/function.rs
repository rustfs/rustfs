use super::name::Name;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

#[derive(Debug, Clone)]
pub struct Key {
    name: String,
    variable: String,
}

// 定义ValueSet类型
pub type ValueSet = HashSet<String>;

// 定义Function trait
pub trait FunctionApi {
    // evaluate方法
    fn evaluate(&self, values: &HashMap<Key, ValueSet>) -> bool;

    // key方法
    fn key(&self) -> Key;

    // name方法
    fn name(&self) -> Name;

    // String方法
    fn to_string(&self) -> String;

    // to_map方法
    fn to_map(&self) -> HashMap<Key, ValueSet>;

    // clone方法
    fn clone(&self) -> Box<Function>;
}

// impl Debug for dyn Function {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", self.to_string())
//     }
// }

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
enum Function {
    #[default]
    Test,
}

// 定义Functions类型
#[derive(Deserialize, Serialize, Default, Clone)]
pub struct Functions(Vec<Function>);

impl Debug for Functions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Functions").field(&self.0).finish()
    }
}
