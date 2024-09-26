// use crate::error::{Error, Result};

// default_partiy_count 默认配置，根据磁盘总数分配校验磁盘数量
pub fn default_partiy_count(drive: usize) -> usize {
    match drive {
        1 => 0,
        2 | 3 => 1,
        4 | 5 => 2,
        6 | 7 => 3,
        _ => 4,
    }
}

// Define the minimum number of parity drives required.
// const MIN_PARITY_DRIVES: usize = 0;

// // ValidateParity validates standard storage class parity.
// pub fn validate_parity(ss_parity: usize, set_drive_count: usize) -> Result<()> {
//     // if ss_parity > 0 && ss_parity < MIN_PARITY_DRIVES {
//     //     return Err(Error::msg(format!("parity {} 应该大于等于 {}", ss_parity, MIN_PARITY_DRIVES)));
//     // }

//     if ss_parity > set_drive_count / 2 {
//         return Err(Error::msg(format!("parity {} 应该小于等于 {}", ss_parity, set_drive_count / 2)));
//     }

//     Ok(())
// }
