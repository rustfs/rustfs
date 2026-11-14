// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rustfs_filemeta::{FileInfoOpts, get_file_info};
use std::{env, fs, io};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "usage: inline_lab_inspect <xl.meta> [version]"))?;
    let version = env::args().nth(2).unwrap_or_default();
    let data = fs::read(path)?;
    let info = get_file_info(
        &data,
        "validation-bucket",
        "validation-object",
        &version,
        FileInfoOpts {
            data: false,
            include_free_versions: false,
            include_part_checksums: false,
        },
    )?;
    println!(
        "{}",
        serde_json::json!({
            "size": info.size,
            "block_size": info.erasure.block_size,
            "data_blocks": info.erasure.data_blocks,
            "parity_blocks": info.erasure.parity_blocks,
            "inline": info.inline_data(),
            "version_id": info.version_id,
        })
    );
    Ok(())
}
