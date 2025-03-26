use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Default)]
pub struct Format {
    pub version: i32,
}

// impl Format {
//     pub const PATH: &str = "config/iam/config/format.json";
//     pub const DEFAULT_VERSION: i32 = 1;

//     pub fn new() -> Self {
//         Self {
//             version: Self::DEFAULT_VERSION,
//         }
//     }
// }
