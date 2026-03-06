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

use super::*;

impl SetDisks {
    pub(super) async fn list_parts(
        disks: &[Option<DiskStore>],
        part_path: &str,
        read_quorum: usize,
    ) -> disk::error::Result<Vec<usize>> {
        let mut futures = Vec::with_capacity(disks.len());
        for (i, disk) in disks.iter().enumerate() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, part_path, -1)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    errs.push(None);
                    object_parts.push(res);
                }
                Err(e) => {
                    errs.push(Some(e));
                    object_parts.push(vec![]);
                }
            }
        }

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        let mut part_quorum_map: HashMap<usize, usize> = HashMap::new();

        for drive_parts in object_parts {
            let mut parts_with_meta_count: HashMap<usize, usize> = HashMap::new();

            // part files can be either part.N or part.N.meta
            for part_path in drive_parts {
                if let Some(num_str) = part_path.strip_prefix("part.") {
                    if let Some(meta_idx) = num_str.find(".meta") {
                        if let Ok(part_num) = num_str[..meta_idx].parse::<usize>() {
                            *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                        }
                    } else if let Ok(part_num) = num_str.parse::<usize>() {
                        *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                    }
                }
            }

            // Include only part.N.meta files with corresponding part.N
            for (&part_num, &cnt) in &parts_with_meta_count {
                if cnt >= 2 {
                    *part_quorum_map.entry(part_num).or_insert(0) += 1;
                }
            }
        }

        let mut part_numbers = Vec::with_capacity(part_quorum_map.len());
        for (part_num, count) in part_quorum_map {
            if count >= read_quorum {
                part_numbers.push(part_num);
            }
        }

        part_numbers.sort();

        Ok(part_numbers)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn check_upload_id_exists(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, bucket, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path, "", false, false).await?;

        let map_err_notfound = |err: DiskError| {
            if err == DiskError::FileNotFound {
                return StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned());
            }
            err.into()
        };

        let (read_quorum, write_quorum) =
            Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count).map_err(map_err_notfound)?;

        if read_quorum < 0 {
            error!("check_upload_id_exists: read_quorum < 0, errs={:?}", errs);
            return Err(Error::ErasureReadQuorum);
        }

        if write_quorum < 0 {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut quorum = read_quorum as usize;
        if write {
            quorum = write_quorum as usize;

            if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
                return Err(map_err_notfound(err));
            }
        } else if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
            return Err(map_err_notfound(err));
        }

        let (_, mod_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, quorum);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, quorum)?;

        Ok((fi, parts_metadata))
    }
}
