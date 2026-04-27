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
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinSet;

async fn collect_list_parts_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> disk::error::Result<(Vec<Option<DiskError>>, Vec<Vec<String>>)>
where
    F: Future<Output = disk::error::Result<Vec<String>>> + Send + 'static,
{
    let mut errs = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut object_parts = vec![Vec::new(); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(parts))) => {
                errs[index] = None;
                object_parts[index] = parts;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errs[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum {
            return Err(DiskError::ErasureReadQuorum);
        }
    }

    Ok((errs, object_parts))
}

fn reduce_quorum_part_numbers(object_parts: Vec<Vec<String>>, read_quorum: usize) -> Vec<usize> {
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
    part_numbers
}

impl SetDisks {
    pub(super) async fn list_parts(
        disks: &[Option<DiskStore>],
        part_path: &str,
        read_quorum: usize,
    ) -> disk::error::Result<Vec<usize>> {
        let mut futures = Vec::with_capacity(disks.len());
        let part_path = part_path.to_string();
        for disk in disks.iter() {
            let disk = disk.clone();
            let part_path = part_path.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, part_path.as_str(), -1)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        let (collected_errs, collected_parts) = collect_list_parts_results(futures, read_quorum).await?;
        errs.extend(collected_errs);
        object_parts.extend(collected_parts);

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        Ok(reduce_quorum_part_numbers(object_parts, read_quorum))
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
            Self::read_all_fileinfo(&disks, bucket, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path, "", false, false, false)
                .await?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_list_parts_results_fails_early_when_quorum_is_impossible() {
        let started = std::time::Instant::now();
        let tasks: Vec<_> = vec![
            (10_u64, Err(DiskError::DiskNotFound)),
            (15, Err(DiskError::DiskNotFound)),
            (250, Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("quorum should become impossible before slow tail completes");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn collect_list_parts_results_tolerates_single_panicked_task_when_quorum_is_met() {
        let tasks: Vec<_> = vec![(5_u64, true), (10, false), (12, false)]
            .into_iter()
            .map(|(delay_ms, should_panic)| async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                if should_panic {
                    panic!("simulated task panic");
                }
                Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])
            })
            .collect();

        let (errs, object_parts) = collect_list_parts_results(tasks, 2)
            .await
            .expect("quorum should still succeed");
        assert_eq!(errs.iter().filter(|err| err.is_none()).count(), 2);
        assert_eq!(object_parts.iter().filter(|parts| !parts.is_empty()).count(), 2);
    }

    #[test]
    fn reduce_quorum_part_numbers_only_keeps_parts_present_on_quorum_of_drives() {
        let object_parts = vec![
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.3".to_string(),
                "part.3.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
        ];

        let parts = reduce_quorum_part_numbers(object_parts, 2);
        assert_eq!(parts, vec![1, 2]);
    }
}
