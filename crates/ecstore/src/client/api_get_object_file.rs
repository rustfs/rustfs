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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use bytes::Bytes;
use http::HeaderMap;
use std::io::Cursor;
#[cfg(not(windows))]
use std::os::unix::fs::MetadataExt;
#[cfg(not(windows))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(not(windows))]
use std::os::unix::fs::PermissionsExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use tokio::io::BufReader;

use crate::client::{
    api_error_response::err_invalid_argument,
    api_get_options::GetObjectOptions,
    transition_api::{ObjectInfo, ReadCloser, ReaderImpl, RequestMetadata, TransitionClient, to_object_info},
};

impl TransitionClient {
    pub async fn fget_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        file_path: &str,
        opts: GetObjectOptions,
    ) -> Result<(), std::io::Error> {
        match std::fs::metadata(file_path) {
            Ok(file_path_stat) => {
                let ft = file_path_stat.file_type();
                if ft.is_dir() {
                    return Err(std::io::Error::other(err_invalid_argument("filename is a directory.")));
                }
            }
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        }

        let path = std::path::Path::new(file_path);
        if let Some(parent) = path.parent() {
            if let Some(object_dir) = parent.file_name() {
                match std::fs::create_dir_all(object_dir) {
                    Ok(_) => {
                        let dir = std::path::Path::new(object_dir);
                        if let Ok(dir_stat) = dir.metadata() {
                            #[cfg(not(windows))]
                            dir_stat.permissions().set_mode(0o700);
                        }
                    }
                    Err(err) => {
                        return Err(std::io::Error::other(err));
                    }
                }
            }
        }

        let object_stat = match self.stat_object(bucket_name, object_name, &opts).await {
            Ok(object_stat) => object_stat,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };

        let mut file_part_path = file_path.to_string();
        file_part_path.push_str("" /*sum_sha256_hex(object_stat.etag.as_bytes())*/);
        file_part_path.push_str(".part.rustfs");

        #[cfg(not(windows))]
        let file_part = match std::fs::OpenOptions::new().mode(0o600).open(file_part_path.clone()) {
            Ok(file_part) => file_part,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };
        #[cfg(windows)]
        let file_part = match std::fs::OpenOptions::new().open(file_part_path.clone()) {
            Ok(file_part) => file_part,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };

        let mut close_and_remove = true;
        /*defer(|| {
            if close_and_remove {
                _ = file_part.close();
                let _ = std::fs::remove(file_part_path);
            }
        });*/

        let st = match file_part.metadata() {
            Ok(st) => st,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };

        let mut opts = opts;
        #[cfg(windows)]
        if st.file_size() > 0 {
            opts.set_range(st.file_size() as i64, 0);
        }

        let object_reader = match self.get_object(bucket_name, object_name, &opts) {
            Ok(object_reader) => object_reader,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };

        /*if let Err(err) = std::fs::copy(file_part, object_reader) {
            return Err(std::io::Error::other(err));
        }*/

        close_and_remove = false;
        /*if let Err(err) = file_part.close() {
            return Err(std::io::Error::other(err));
        }*/

        if let Err(err) = std::fs::rename(file_part_path, file_path) {
            return Err(std::io::Error::other(err));
        }

        Ok(())
    }
}
