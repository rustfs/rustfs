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
use std::{env, fs, path::PathBuf};
fn main() {
    let path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: dump_fileinfo <xl.meta path>");
    let data = fs::read(&path).expect("read xl.meta");
    let fi = get_file_info(
        &data,
        "debug-bucket",
        "debug-object",
        "",
        FileInfoOpts {
            data: false,
            include_free_versions: true,
        },
    )
    .expect("decode file info");
    println!("path: {}", path.display());
    println!("size: {}", fi.size);
    println!("etag: {:?}", fi.get_etag());
    println!("parts: {}", fi.parts.len());
    for (idx, part) in fi.parts.iter().enumerate() {
        println!(
            "part#{idx}: number={} size={} actual_size={} etag={}",
            part.number, part.size, part.actual_size, part.etag
        );
    }
    println!("metadata entries: {}", fi.metadata.len());
    let mut keys = fi.metadata.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        println!("meta[{key}]={}", fi.metadata.get(&key).unwrap());
    }
}
