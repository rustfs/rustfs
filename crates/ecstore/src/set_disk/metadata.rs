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
    pub(super) fn reduce_common_data_dir(data_dirs: &Vec<Option<Uuid>>, write_quorum: usize) -> Option<Uuid> {
        let mut data_dirs_count = HashMap::new();

        for ddir in data_dirs {
            *data_dirs_count.entry(ddir).or_insert(0) += 1;
        }

        let mut max = 0;
        let mut data_dir = None;
        for (ddir, count) in data_dirs_count {
            if count > max {
                max = count;
                data_dir = *ddir;
            }
        }

        if max >= write_quorum { data_dir } else { None }
    }

    pub(super) fn get_upload_id_dir(bucket: &str, object: &str, upload_id: &str) -> String {
        let upload_uuid = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(upload_id.as_bytes())
            .and_then(|v| {
                String::from_utf8(v).map_or(Ok(upload_id.to_owned()), |v| {
                    let parts: Vec<_> = v.splitn(2, '.').collect();
                    if parts.len() == 2 {
                        Ok(parts[1].to_string())
                    } else {
                        Ok(upload_id.to_string())
                    }
                })
            })
            .unwrap_or_default();

        format!("{}/{}", Self::get_multipart_sha_dir(bucket, object), upload_uuid)
    }

    pub(super) fn get_multipart_sha_dir(bucket: &str, object: &str) -> String {
        let path = format!("{bucket}/{object}");
        let mut hasher = Sha256::new();
        hasher.update(path);
        hex(hasher.finalize())
    }

    pub(super) fn common_parity(parities: &[i32], default_parity_count: i32) -> i32 {
        let n = parities.len() as i32;

        let mut occ_map: HashMap<i32, i32> = HashMap::new();
        for &p in parities {
            *occ_map.entry(p).or_insert(0) += 1;
        }

        let mut max_occ = 0;
        let mut cparity = 0;
        for (&parity, &occ) in &occ_map {
            if parity == -1 {
                // Ignore non defined parity
                continue;
            }

            let mut read_quorum = n - parity;
            if default_parity_count > 0 && parity == 0 {
                // In this case, parity == 0 implies that this object version is a
                // delete marker
                read_quorum = n / 2 + 1;
            }
            if occ < read_quorum {
                // Ignore this parity since we don't have enough shards for read quorum
                continue;
            }

            if occ > max_occ {
                max_occ = occ;
                cparity = parity;
            }
        }

        if max_occ == 0 {
            // Did not find anything useful
            return -1;
        }
        cparity
    }

    pub(super) fn list_object_modtimes(parts_metadata: &[FileInfo], errs: &[Option<DiskError>]) -> Vec<Option<OffsetDateTime>> {
        let mut times = vec![None; parts_metadata.len()];

        for (i, metadata) in parts_metadata.iter().enumerate() {
            if errs[i].is_some() {
                continue;
            }

            times[i] = metadata.mod_time
        }

        times
    }

    pub(super) fn common_time(times: &[Option<OffsetDateTime>], quorum: usize) -> Option<OffsetDateTime> {
        let (time, count) = Self::common_time_and_occurrence(times);
        if count >= quorum { time } else { None }
    }

    pub(super) fn common_time_and_occurrence(times: &[Option<OffsetDateTime>]) -> (Option<OffsetDateTime>, usize) {
        let mut time_occurrence_map = HashMap::new();

        // Ignore the uuid sentinel and count the rest.
        for time in times.iter().flatten() {
            *time_occurrence_map.entry(time.unix_timestamp_nanos()).or_insert(0) += 1;
        }

        let mut maxima = 0; // Counter for remembering max occurrence of elements.
        let mut latest = 0;

        // Find the common cardinality from previously collected
        // occurrences of elements.
        for (&nano, &count) in &time_occurrence_map {
            if count < maxima {
                continue;
            }

            // We are at or above maxima
            if count > maxima || nano > latest {
                maxima = count;
                latest = nano;
            }
        }

        if latest == 0 {
            return (None, maxima);
        }

        if let Ok(time) = OffsetDateTime::from_unix_timestamp_nanos(latest) {
            (Some(time), maxima)
        } else {
            (None, maxima)
        }
    }

    pub(super) fn common_etag(etags: &[Option<String>], quorum: usize) -> Option<String> {
        let (etag, count) = Self::common_etags(etags);
        if count >= quorum { etag } else { None }
    }

    pub(super) fn common_etags(etags: &[Option<String>]) -> (Option<String>, usize) {
        let mut etags_map = HashMap::new();

        for etag in etags.iter().flatten() {
            *etags_map.entry(etag).or_insert(0) += 1;
        }

        let mut maxima = 0; // Counter for remembering max occurrence of elements.
        let mut latest = None;

        for (&etag, &count) in &etags_map {
            if count < maxima {
                continue;
            }

            // We are at or above maxima
            if count > maxima {
                maxima = count;
                latest = Some(etag.clone());
            }
        }

        (latest, maxima)
    }

    pub(super) fn list_object_etags(parts_metadata: &[FileInfo], errs: &[Option<DiskError>]) -> Vec<Option<String>> {
        let mut etags = vec![None; parts_metadata.len()];

        for (i, metadata) in parts_metadata.iter().enumerate() {
            if errs[i].is_some() {
                continue;
            }

            if let Some(etag) = metadata.metadata.get("etag") {
                etags[i] = Some(etag.clone())
            }
        }

        etags
    }

    pub(super) fn list_object_parities(parts_metadata: &[FileInfo], errs: &[Option<DiskError>]) -> Vec<i32> {
        let total_shards = parts_metadata.len();
        let half = total_shards as i32 / 2;
        let mut parities: Vec<i32> = vec![-1; total_shards];

        for (index, metadata) in parts_metadata.iter().enumerate() {
            if errs[index].is_some() {
                parities[index] = -1;
                continue;
            }

            if !metadata.is_valid() {
                parities[index] = -1;
                continue;
            }

            if metadata.deleted || metadata.size == 0 {
                parities[index] = half;
            // } else if metadata.transition_status == "TransitionComplete" {
            // TODO: metadata.transition_status
            //     parities[index] = total_shards - (total_shards / 2 + 1);
            } else {
                parities[index] = metadata.erasure.parity_blocks as i32;
            }
        }
        parities
    }

    #[tracing::instrument(level = "debug", skip(parts_metadata))]
    pub(super) fn object_quorum_from_meta(
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        default_parity_count: usize,
    ) -> disk::error::Result<(i32, i32)> {
        let expected_rquorum = if default_parity_count == 0 {
            parts_metadata.len()
        } else {
            parts_metadata.len() / 2
        };

        if let Some(err) = reduce_read_quorum_errs(errs, OBJECT_OP_IGNORED_ERRS, expected_rquorum) {
            // let object = parts_metadata.first().map(|v| v.name.clone()).unwrap_or_default();
            // error!("object_quorum_from_meta: {:?}, errs={:?}, object={:?}", err, errs, object);
            return Err(err);
        }

        if default_parity_count == 0 {
            return Ok((parts_metadata.len() as i32, parts_metadata.len() as i32));
        }

        let parities = Self::list_object_parities(parts_metadata, errs);

        let parity_blocks = Self::common_parity(&parities, default_parity_count as i32);

        if parity_blocks < 0 {
            error!("object_quorum_from_meta: parity_blocks < 0, errs={:?}", errs);
            return Err(DiskError::ErasureReadQuorum);
        }

        let data_blocks = parts_metadata.len() as i32 - parity_blocks;
        let write_quorum = if data_blocks == parity_blocks {
            data_blocks + 1
        } else {
            data_blocks
        };

        Ok((data_blocks, write_quorum))
    }

    #[tracing::instrument(level = "debug", skip(disks, parts_metadata))]
    pub(super) fn list_online_disks(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        quorum: usize,
    ) -> (Vec<Option<DiskStore>>, Option<OffsetDateTime>, Option<String>) {
        let mod_times = Self::list_object_modtimes(parts_metadata, errs);
        let etags = Self::list_object_etags(parts_metadata, errs);

        let mod_time = Self::common_time(&mod_times, quorum);
        let etag = Self::common_etag(&etags, quorum);

        let mut new_disk = vec![None; disks.len()];

        for (i, &t) in mod_times.iter().enumerate() {
            if parts_metadata[i].is_valid() && mod_time == t {
                new_disk[i].clone_from(&disks[i]);
            }
        }

        (new_disk, mod_time, etag)
    }

    pub(super) fn pick_valid_fileinfo(
        metas: &[FileInfo],
        mod_time: Option<OffsetDateTime>,
        etag: Option<String>,
        quorum: usize,
    ) -> disk::error::Result<FileInfo> {
        Self::find_file_info_in_quorum(metas, &mod_time, &etag, quorum)
    }

    pub(super) fn find_file_info_in_quorum(
        metas: &[FileInfo],
        mod_time: &Option<OffsetDateTime>,
        etag: &Option<String>,
        quorum: usize,
    ) -> disk::error::Result<FileInfo> {
        if quorum < 1 {
            warn!("find_file_info_in_quorum: quorum < 1");
            return Err(DiskError::ErasureReadQuorum);
        }

        let mut meta_hashes = vec![None; metas.len()];
        let mut hasher = Sha256::new();

        for (i, meta) in metas.iter().enumerate() {
            if !meta.is_valid() {
                debug!(
                    index = i,
                    valid = false,
                    version_id = ?meta.version_id,
                    mod_time = ?meta.mod_time,
                    "find_file_info_in_quorum: skipping invalid meta"
                );
                continue;
            }

            debug!(
                index = i,
                valid = true,
                version_id = ?meta.version_id,
                mod_time = ?meta.mod_time,
                deleted = meta.deleted,
                size = meta.size,
                "find_file_info_in_quorum: inspecting meta"
            );

            let etag_only = mod_time.is_none() && etag.is_some() && meta.get_etag().is_some_and(|v| &v == etag.as_ref().unwrap());
            let mod_valid = mod_time == &meta.mod_time;

            if etag_only || mod_valid {
                for part in meta.parts.iter() {
                    hasher.update(format!("part.{}", part.number).as_bytes());
                    hasher.update(format!("part.{}", part.size).as_bytes());
                }

                if !meta.deleted && meta.size != 0 {
                    hasher.update(format!("{}+{}", meta.erasure.data_blocks, meta.erasure.parity_blocks).as_bytes());
                    hasher.update(format!("{:?}", meta.erasure.distribution).as_bytes());
                }

                if meta.is_remote() {
                    // TODO:
                }

                // TODO: IsEncrypted

                // TODO: IsCompressed

                meta_hashes[i] = Some(hex(hasher.clone().finalize().as_slice()));

                hasher.reset();
            } else {
                debug!(
                    index = i,
                    etag_only_match = etag_only,
                    mod_valid_match = mod_valid,
                    "find_file_info_in_quorum: meta does not match common etag or mod_time, skipping hash calculation"
                );
            }
        }

        let mut count_map = HashMap::new();

        for hash in meta_hashes.iter().flatten() {
            *count_map.entry(hash).or_insert(0) += 1;
        }

        let mut max_val = None;
        let mut max_count = 0;

        for (&val, &count) in &count_map {
            if count > max_count {
                max_val = Some(val);
                max_count = count;
            }
        }

        if max_count < quorum {
            warn!("find_file_info_in_quorum: max_count < quorum, max_val={:?}", max_val);
            return Err(DiskError::ErasureReadQuorum);
        }

        let mut found_fi = None;
        let mut found = false;

        let mut valid_obj_map = HashMap::new();

        for (i, op_hash) in meta_hashes.iter().enumerate() {
            if let Some(hash) = op_hash
                && let Some(max_hash) = max_val
                && hash == max_hash
            {
                if metas[i].is_valid() && !found {
                    found_fi = Some(metas[i].clone());
                    found = true;
                }

                let props = ObjProps {
                    mod_time: metas[i].mod_time,
                    num_versions: metas[i].num_versions,
                };

                *valid_obj_map.entry(props).or_insert(0) += 1;
            }
        }

        if found {
            let mut fi = found_fi.unwrap();

            for (val, &count) in &valid_obj_map {
                if count > quorum {
                    fi.mod_time = val.mod_time;
                    fi.num_versions = val.num_versions;
                    fi.is_latest = val.mod_time.is_none();

                    break;
                }
            }

            return Ok(fi);
        }

        warn!("find_file_info_in_quorum: fileinfo not found");

        Err(DiskError::ErasureReadQuorum)
    }

    pub(super) fn shuffle_disks_and_parts_metadata_by_index(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        fi: &FileInfo,
    ) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
        let mut shuffled_disks = vec![None; disks.len()];
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
        let distribution = &fi.erasure.distribution;

        let mut inconsistent = 0;
        for (k, v) in parts_metadata.iter().enumerate() {
            if disks[k].is_none() {
                inconsistent += 1;
                continue;
            }

            if !v.is_valid() {
                inconsistent += 1;
                continue;
            }

            if distribution[k] != v.erasure.index {
                inconsistent += 1;
                continue;
            }

            let block_idx = distribution[k];
            shuffled_parts_metadata[block_idx - 1] = parts_metadata[k].clone();
            shuffled_disks[block_idx - 1].clone_from(&disks[k]);
        }

        if inconsistent < fi.erasure.parity_blocks {
            return (shuffled_disks, shuffled_parts_metadata);
        }

        Self::shuffle_disks_and_parts_metadata(disks, parts_metadata, fi)
    }

    pub(super) fn shuffle_disks_and_parts_metadata(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        fi: &FileInfo,
    ) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
        let init = fi.mod_time.is_none();

        let mut shuffled_disks = vec![None; disks.len()];
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
        let distribution = &fi.erasure.distribution;

        for (k, v) in disks.iter().enumerate() {
            if v.is_none() {
                continue;
            }

            if !init && !parts_metadata[k].is_valid() {
                continue;
            }

            // if !init && fi.xlv1 != parts_metadata[k].xlv1 {
            //     continue;
            // }

            let block_idx = distribution[k];
            shuffled_parts_metadata[block_idx - 1] = parts_metadata[k].clone();
            shuffled_disks[block_idx - 1].clone_from(&disks[k]);
        }

        (shuffled_disks, shuffled_parts_metadata)
    }

    pub(super) fn shuffle_parts_metadata(parts_metadata: &[FileInfo], distribution: &[usize]) -> Vec<FileInfo> {
        if distribution.is_empty() {
            return parts_metadata.to_vec();
        }
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];
        // Shuffle slice xl metadata for expected distribution.
        for index in 0..parts_metadata.len() {
            let block_index = distribution[index];
            shuffled_parts_metadata[block_index - 1] = parts_metadata[index].clone();
        }
        shuffled_parts_metadata
    }

    pub(super) fn shuffle_disks(disks: &[Option<DiskStore>], distribution: &[usize]) -> Vec<Option<DiskStore>> {
        if distribution.is_empty() {
            return disks.to_vec();
        }

        let mut shuffled_disks = vec![None; disks.len()];

        for (i, v) in disks.iter().enumerate() {
            let idx = distribution[i];
            shuffled_disks[idx - 1].clone_from(v);
        }

        shuffled_disks
    }

    pub(super) fn shuffle_check_parts(parts_errs: &[usize], distribution: &[usize]) -> Vec<usize> {
        if distribution.is_empty() {
            return parts_errs.to_vec();
        }
        let mut shuffled_parts_errs = vec![0; parts_errs.len()];
        for (i, v) in parts_errs.iter().enumerate() {
            let idx = distribution[i];
            shuffled_parts_errs[idx - 1] = *v;
        }
        shuffled_parts_errs
    }
}
