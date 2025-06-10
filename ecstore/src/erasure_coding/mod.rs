pub mod decode;
pub mod encode;
pub mod erasure;
pub mod heal;

mod bitrot;
pub use bitrot::*;

pub use erasure::{Erasure, ReedSolomonEncoder, calc_shard_size};
