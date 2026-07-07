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
use rustfs_utils::http;

#[derive(Clone, Copy)]
struct FileInfoIdentityGroup {
    hash: [u8; 32],
    count: usize,
    mod_time: Option<OffsetDateTime>,
}

impl SetDisks {
    pub(super) fn all_not_found_metadata(errs: &[Option<DiskError>]) -> bool {
        !errs.is_empty()
            && errs.iter().all(|err| match err {
                Some(err) => {
                    matches!(
                        err,
                        DiskError::FileNotFound
                            | DiskError::FileVersionNotFound
                            | DiskError::VolumeNotFound
                            | DiskError::DiskNotFound
                    ) || OBJECT_OP_IGNORED_ERRS.contains(err)
                }
                None => false,
            })
            && errs.iter().any(|err| {
                matches!(
                    err,
                    Some(DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::VolumeNotFound)
                )
            })
    }

    pub(super) fn reduce_common_data_dir(data_dirs: &[Option<Uuid>], write_quorum: usize) -> Option<Uuid> {
        let mut data_dirs_count = HashMap::new();

        for ddir in data_dirs.iter().flatten().copied() {
            *data_dirs_count.entry(ddir).or_insert(0) += 1;
        }

        let mut max = 0;
        let mut data_dir = None;
        for (ddir, count) in data_dirs_count {
            if count > max {
                max = count;
                data_dir = Some(ddir);
            }
        }

        if max >= write_quorum { data_dir } else { None }
    }

    pub(super) fn get_upload_id_dir(bucket: &str, object: &str, upload_id: &str) -> String {
        let upload_uuid = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(upload_id.as_bytes())
            .and_then(|v| {
                String::from_utf8(v).map_or_else(
                    |_| Ok(upload_id.to_owned()),
                    |v| {
                        let parts: Vec<_> = v.splitn(2, '.').collect();
                        if parts.len() == 2 {
                            Ok(parts[1].to_string())
                        } else {
                            Ok(upload_id.to_string())
                        }
                    },
                )
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
        if Self::all_not_found_metadata(errs) {
            return Err(DiskError::FileNotFound);
        }

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

        let mod_time = Self::common_time(&mod_times, quorum);

        if mod_time.is_none() {
            let etags = Self::list_object_etags(parts_metadata, errs);
            let etag_op = Self::common_etag(&etags, quorum);
            if let Some(etag) = etag_op {
                let mut new_disk = vec![None; disks.len()];
                for (i, etag_item) in etags.iter().enumerate() {
                    if let Some(etag_item) = etag_item
                        && etag_item == &etag
                        && parts_metadata[i].is_valid()
                    {
                        new_disk[i].clone_from(&disks[i]);
                    }
                }

                return (new_disk, None, Some(etag));
            }
        }

        let mut new_disk = vec![None; disks.len()];

        for (i, &t) in mod_times.iter().enumerate() {
            if parts_metadata[i].is_valid() && mod_time == t {
                new_disk[i].clone_from(&disks[i]);
            }
        }

        (new_disk, mod_time, None)
    }

    fn usable_fileinfo_count(parts_metadata: &[FileInfo], errs: &[Option<DiskError>]) -> (usize, bool) {
        let mut has_read_error = false;
        let mut usable_metadata = 0;
        for (meta, err) in parts_metadata.iter().zip(errs.iter()) {
            if err.is_some() {
                has_read_error = true;
                continue;
            }

            if meta.is_valid() {
                usable_metadata += 1;
            }
        }

        (usable_metadata, has_read_error)
    }

    pub(super) fn latest_fileinfo_selection_quorum(
        version_id: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        read_quorum: usize,
        write_quorum: usize,
    ) -> usize {
        if !version_id.is_empty() || write_quorum <= read_quorum {
            return read_quorum;
        }

        let (usable_metadata, has_read_error) = Self::usable_fileinfo_count(parts_metadata, errs);

        if usable_metadata < write_quorum {
            return read_quorum;
        }

        if !has_read_error {
            return write_quorum;
        }

        let mut identity_counts = HashMap::with_capacity(usable_metadata);
        for (meta, err) in parts_metadata.iter().zip(errs.iter()) {
            if err.is_some() || !meta.is_valid() {
                continue;
            }

            let key = Self::file_info_quorum_hash(meta);

            let count = identity_counts.entry(key).or_insert(0);
            *count += 1;
            if *count >= write_quorum {
                return write_quorum;
            }
        }

        read_quorum
    }

    pub(super) fn select_valid_fileinfo(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        version_id: &str,
        read_quorum: usize,
        write_quorum: usize,
    ) -> disk::error::Result<(Vec<Option<DiskStore>>, FileInfo, usize)> {
        let selection_quorum =
            Self::latest_fileinfo_selection_quorum(version_id, parts_metadata, errs, read_quorum, write_quorum);
        let (usable_metadata, has_read_error) = Self::usable_fileinfo_count(parts_metadata, errs);

        if version_id.is_empty()
            && write_quorum > read_quorum
            && has_read_error
            && usable_metadata >= write_quorum
            && selection_quorum == read_quorum
        {
            let (online_disks, fi) = Self::pick_degraded_latest_fileinfo(disks, parts_metadata, errs, read_quorum, write_quorum)?;
            return Ok((online_disks, fi, read_quorum));
        }

        let (online_disks, mod_time, etag) = Self::list_online_disks(disks, parts_metadata, errs, selection_quorum);
        let fi = Self::pick_valid_fileinfo(parts_metadata, mod_time, etag, selection_quorum)?;

        Ok((online_disks, fi, selection_quorum))
    }

    pub(super) fn pick_valid_fileinfo(
        metas: &[FileInfo],
        mod_time: Option<OffsetDateTime>,
        etag: Option<String>,
        quorum: usize,
    ) -> disk::error::Result<FileInfo> {
        Self::find_file_info_in_quorum(metas, &mod_time, &etag, quorum)
    }

    fn update_hash_bytes(hasher: &mut Sha256, value: &[u8]) {
        hasher.update(value.len().to_le_bytes());
        hasher.update(value);
    }

    fn update_hash_str(hasher: &mut Sha256, value: &str) {
        Self::update_hash_bytes(hasher, value.as_bytes());
    }

    fn update_hash_optional_uuid(hasher: &mut Sha256, value: Option<Uuid>) {
        if let Some(value) = value {
            hasher.update([1]);
            hasher.update(value.as_bytes());
        } else {
            hasher.update([0]);
        }
    }

    fn update_hash_optional_time(hasher: &mut Sha256, value: Option<OffsetDateTime>) {
        if let Some(value) = value {
            hasher.update([1]);
            hasher.update(value.unix_timestamp_nanos().to_le_bytes());
        } else {
            hasher.update([0]);
        }
    }

    fn update_hash_optional_u32(hasher: &mut Sha256, value: Option<u32>) {
        if let Some(value) = value {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        } else {
            hasher.update([0]);
        }
    }

    fn update_hash_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
        if let Some(value) = value {
            hasher.update([1]);
            hasher.update(value.to_le_bytes());
        } else {
            hasher.update([0]);
        }
    }

    fn update_hash_optional_bytes(hasher: &mut Sha256, value: Option<&Bytes>) {
        if let Some(value) = value {
            hasher.update([1]);
            Self::update_hash_bytes(hasher, value);
        } else {
            hasher.update([0]);
        }
    }

    fn update_hash_optional_str(hasher: &mut Sha256, value: Option<&str>) {
        if let Some(value) = value {
            hasher.update([1]);
            Self::update_hash_str(hasher, value);
        } else {
            hasher.update([0]);
        }
    }

    fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
        value
            .get(..prefix.len())
            .is_some_and(|value_prefix| value_prefix.eq_ignore_ascii_case(prefix))
    }

    fn internal_metadata_suffix(name: &str) -> Option<&str> {
        name.get(http::RUSTFS_INTERNAL_PREFIX.len()..)
            .filter(|_| Self::starts_with_ignore_ascii_case(name, http::RUSTFS_INTERNAL_PREFIX))
            .or_else(|| {
                name.get(http::MINIO_INTERNAL_PREFIX.len()..)
                    .filter(|_| Self::starts_with_ignore_ascii_case(name, http::MINIO_INTERNAL_PREFIX))
            })
    }

    fn is_replication_quorum_metadata_key(name: &str) -> bool {
        if name.eq_ignore_ascii_case(http::AMZ_BUCKET_REPLICATION_STATUS) {
            return true;
        }

        let Some(suffix) = Self::internal_metadata_suffix(name) else {
            return false;
        };

        suffix.eq_ignore_ascii_case(http::SUFFIX_REPLICA_STATUS)
            || suffix.eq_ignore_ascii_case(http::SUFFIX_REPLICA_TIMESTAMP)
            || suffix.eq_ignore_ascii_case(http::SUFFIX_REPLICATION_STATUS)
            || suffix.eq_ignore_ascii_case(http::SUFFIX_REPLICATION_TIMESTAMP)
            || suffix.eq_ignore_ascii_case(http::SUFFIX_PURGESTATUS)
            || Self::starts_with_ignore_ascii_case(suffix, http::SUFFIX_REPLICATION_RESET_ARN_PREFIX)
    }

    fn update_hash_quorum_metadata_map(hasher: &mut Sha256, entries: &HashMap<String, String>) {
        let mut entries = entries
            .iter()
            .filter(|(name, _)| !Self::is_replication_quorum_metadata_key(name))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(right.0));
        hasher.update(entries.len().to_le_bytes());
        for (name, value) in entries {
            Self::update_hash_str(hasher, name);
            Self::update_hash_str(hasher, value);
        }
    }

    fn file_info_quorum_hash(meta: &FileInfo) -> [u8; 32] {
        let mut hasher = Sha256::new();
        Self::update_file_info_quorum_hash(&mut hasher, meta);
        let digest = hasher.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(digest.as_slice());
        key
    }

    fn update_file_info_quorum_hash(hasher: &mut Sha256, meta: &FileInfo) {
        hasher.update(meta.size.to_le_bytes());
        hasher.update([u8::from(meta.deleted), u8::from(meta.mark_deleted)]);
        hasher.update([u8::from(meta.expire_restored)]);
        Self::update_hash_optional_time(hasher, meta.mod_time);
        Self::update_hash_str(hasher, &meta.transition_status);
        Self::update_hash_str(hasher, &meta.transition_tier);
        Self::update_hash_str(hasher, &meta.transitioned_objname);
        Self::update_hash_optional_uuid(hasher, meta.transition_version_id);
        Self::update_hash_optional_u32(hasher, meta.mode);
        Self::update_hash_optional_u64(hasher, meta.written_by_version);

        Self::update_hash_optional_uuid(hasher, meta.version_id);
        Self::update_hash_optional_uuid(hasher, meta.data_dir);

        Self::update_hash_optional_bytes(hasher, meta.checksum.as_ref());

        Self::update_hash_quorum_metadata_map(hasher, &meta.metadata);

        hasher.update(meta.parts.len().to_le_bytes());
        for part in meta.parts.iter() {
            hasher.update(part.number.to_le_bytes());
            hasher.update(part.size.to_le_bytes());
            hasher.update(part.actual_size.to_le_bytes());
            Self::update_hash_str(hasher, &part.etag);

            Self::update_hash_optional_time(hasher, part.mod_time);

            Self::update_hash_optional_bytes(hasher, part.index.as_ref());
            Self::update_hash_optional_str(hasher, part.error.as_deref());

            if let Some(checksums) = &part.checksums {
                let mut checksum_entries = checksums.iter().collect::<Vec<_>>();
                checksum_entries.sort_by(|left, right| left.0.cmp(right.0));
                hasher.update(checksum_entries.len().to_le_bytes());
                for (name, value) in checksum_entries {
                    Self::update_hash_str(hasher, name);
                    Self::update_hash_str(hasher, value);
                }
            } else {
                hasher.update(0usize.to_le_bytes());
            }
        }

        if !meta.deleted && meta.size != 0 {
            hasher.update(meta.erasure.data_blocks.to_le_bytes());
            hasher.update(meta.erasure.parity_blocks.to_le_bytes());
            hasher.update(meta.erasure.distribution.len().to_le_bytes());
            for disk_index in meta.erasure.distribution.iter() {
                hasher.update(disk_index.to_le_bytes());
            }
        }
    }

    fn latest_fileinfo_identity_groups(parts_metadata: &[FileInfo], errs: &[Option<DiskError>]) -> Vec<FileInfoIdentityGroup> {
        let mut groups: Vec<FileInfoIdentityGroup> = Vec::with_capacity(parts_metadata.len());
        for (meta, err) in parts_metadata.iter().zip(errs.iter()) {
            if err.is_some() || !meta.is_valid() {
                continue;
            }

            let hash = Self::file_info_quorum_hash(meta);
            if let Some(group) = groups.iter_mut().find(|group| group.hash == hash) {
                group.count += 1;
                continue;
            }

            groups.push(FileInfoIdentityGroup {
                hash,
                count: 1,
                mod_time: meta.mod_time,
            });
        }

        groups
    }

    fn pick_fileinfo_identity(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        hash: [u8; 32],
        quorum: usize,
    ) -> disk::error::Result<(Vec<Option<DiskStore>>, FileInfo)> {
        let mut online_disks = vec![None; disks.len()];
        let mut selected = None;
        let mut count = 0;

        for (i, ((meta, err), disk)) in parts_metadata.iter().zip(errs.iter()).zip(disks.iter()).enumerate() {
            if err.is_some() || !meta.is_valid() || Self::file_info_quorum_hash(meta) != hash {
                continue;
            }

            count += 1;
            online_disks[i].clone_from(disk);
            if selected.is_none() {
                selected = Some(meta.clone());
            }
        }

        if count < quorum {
            return Err(DiskError::ErasureReadQuorum);
        }

        selected
            .map(|mut fi| {
                fi.is_latest = fi.successor_mod_time.is_none();
                (online_disks, fi)
            })
            .ok_or(DiskError::ErasureReadQuorum)
    }

    fn pick_degraded_latest_fileinfo(
        disks: &[Option<DiskStore>],
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        read_quorum: usize,
        write_quorum: usize,
    ) -> disk::error::Result<(Vec<Option<DiskStore>>, FileInfo)> {
        let mut groups = Self::latest_fileinfo_identity_groups(parts_metadata, errs);
        if groups.is_empty() {
            return Err(DiskError::ErasureReadQuorum);
        }

        groups.sort_by(|left, right| right.mod_time.cmp(&left.mod_time).then_with(|| right.count.cmp(&left.count)));
        let latest_mod_time = groups[0].mod_time;

        let mut older_start = 0;
        while older_start < groups.len() && groups[older_start].mod_time == latest_mod_time {
            if groups[older_start].count >= write_quorum {
                return Self::pick_fileinfo_identity(disks, parts_metadata, errs, groups[older_start].hash, write_quorum);
            }
            older_start += 1;
        }

        if older_start > 1 {
            return Err(DiskError::ErasureReadQuorum);
        }

        for group in groups.iter().skip(older_start) {
            if group.count >= read_quorum {
                return Self::pick_fileinfo_identity(disks, parts_metadata, errs, group.hash, read_quorum);
            }
        }

        Err(DiskError::ErasureReadQuorum)
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

            let etag_only = mod_time.is_none()
                && etag.is_some()
                && meta
                    .get_etag()
                    .is_some_and(|v| &v == etag.as_ref().expect("operation should succeed"));
            let mod_valid = mod_time == &meta.mod_time;

            if etag_only || mod_valid {
                if meta.is_remote() {
                    // TODO:
                }

                // TODO: IsEncrypted

                // TODO: IsCompressed

                meta_hashes[i] = Some(Self::file_info_quorum_hash(meta));
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

        for hash in meta_hashes.iter().flatten().copied() {
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
            warn!(
                quorum,
                max_count,
                max_val = ?max_val,
                count_map = ?count_map,
                "find_file_info_in_quorum: fileinfo content identity did not reach quorum"
            );
            return Err(DiskError::ErasureReadQuorum);
        }

        let mut found_fi = None;
        let mut found = false;

        let mut valid_obj_map = HashMap::new();

        for (i, op_hash) in meta_hashes.iter().enumerate() {
            if let Some(hash) = op_hash
                && let Some(max_hash) = max_val
                && *hash == max_hash
                && metas[i].is_valid()
            {
                if !found {
                    found_fi = Some(metas[i].clone());
                    found = true;
                }

                let props = ObjProps {
                    successor_mod_time: metas[i].successor_mod_time,
                    num_versions: metas[i].num_versions,
                };

                *valid_obj_map.entry(props).or_insert(0) += 1;
            }
        }

        if found {
            let mut fi = found_fi.expect("operation should succeed");

            for (val, &count) in &valid_obj_map {
                if count >= quorum {
                    fi.successor_mod_time = val.successor_mod_time;
                    fi.num_versions = val.num_versions;
                    fi.is_latest = val.successor_mod_time.is_none();

                    break;
                }
            }

            return Ok(fi);
        }

        warn!("find_file_info_in_quorum: fileinfo not found");

        Err(DiskError::ErasureReadQuorum)
    }

    /// Ownership-taking variant of `shuffle_disks_and_parts_metadata_by_index`
    /// (backlog#873): callers that already own the vectors avoid one deep
    /// `FileInfo` clone per disk by moving entries into their shuffled slots.
    ///
    /// Semantics match the borrowing variant, including the fallback to the
    /// mod-time based placement when `parity_blocks` or more sources are
    /// inconsistent; the consistency check runs as a read-only first pass so
    /// the fallback still sees the untouched inputs.
    pub(super) fn shuffle_disks_and_parts_metadata_by_index_owned(
        mut disks: Vec<Option<DiskStore>>,
        mut parts_metadata: Vec<FileInfo>,
        fi: &FileInfo,
    ) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
        let distribution = &fi.erasure.distribution;

        let mut inconsistent = 0;
        for (k, v) in parts_metadata.iter().enumerate() {
            if disks[k].is_none() || !v.is_valid() || distribution[k] != v.erasure.index {
                inconsistent += 1;
            }
        }

        let use_by_index = inconsistent < fi.erasure.parity_blocks;
        let init = fi.mod_time.is_none();

        let mut shuffled_disks = vec![None; disks.len()];
        let mut shuffled_parts_metadata = vec![FileInfo::default(); parts_metadata.len()];

        for k in 0..parts_metadata.len() {
            if disks[k].is_none() {
                continue;
            }
            let eligible = if use_by_index {
                parts_metadata[k].is_valid() && distribution[k] == parts_metadata[k].erasure.index
            } else {
                init || parts_metadata[k].is_valid()
            };
            if !eligible {
                continue;
            }

            let block_idx = distribution[k];
            shuffled_parts_metadata[block_idx - 1] = std::mem::take(&mut parts_metadata[k]);
            shuffled_disks[block_idx - 1] = disks[k].take();
        }

        (shuffled_disks, shuffled_parts_metadata)
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn shuffle_test_disks(tempdir: &tempfile::TempDir, count: usize) -> Vec<Option<DiskStore>> {
        let endpoint =
            Endpoint::try_from(tempdir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");
        // The shuffle only inspects Some/None and clones the Arc handle, so
        // one shared disk handle per slot is sufficient.
        (0..count).map(|_| Some(disk.clone())).collect()
    }

    fn shuffle_fixture(consistent: bool) -> (FileInfo, Vec<FileInfo>) {
        let mut fi = FileInfo::new("bucket/object", 2, 1);
        fi.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_705_312_300).expect("valid timestamp"));
        fi.size = 1;
        fi.add_object_part(1, String::new(), 1, None, 1, None, None);

        let slots = fi.erasure.distribution.len();
        let parts = (0..slots)
            .map(|k| {
                let mut part_fi = fi.clone();
                part_fi.erasure.index = if consistent {
                    fi.erasure.distribution[k]
                } else {
                    // Misplace every source so the by-index pass is rejected
                    // and the mod-time fallback placement runs instead.
                    fi.erasure.distribution[(k + 1) % slots]
                };
                part_fi
            })
            .collect();
        (fi, parts)
    }

    #[tokio::test]
    async fn owned_shuffle_matches_borrowing_variant_when_consistent() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let (fi, parts) = shuffle_fixture(true);
        let disks = shuffle_test_disks(&tempdir, parts.len()).await;

        let (expected_disks, expected_parts) = SetDisks::shuffle_disks_and_parts_metadata_by_index(&disks, &parts, &fi);
        let (owned_disks, owned_parts) = SetDisks::shuffle_disks_and_parts_metadata_by_index_owned(disks.clone(), parts, &fi);

        assert_eq!(owned_parts, expected_parts, "owned shuffle must place identical metadata");
        let expected_slots: Vec<bool> = expected_disks.iter().map(Option::is_some).collect();
        let owned_slots: Vec<bool> = owned_disks.iter().map(Option::is_some).collect();
        assert_eq!(owned_slots, expected_slots, "owned shuffle must fill identical disk slots");
    }

    #[tokio::test]
    async fn owned_shuffle_matches_borrowing_variant_on_fallback() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let (fi, parts) = shuffle_fixture(false);
        let disks = shuffle_test_disks(&tempdir, parts.len()).await;

        let (expected_disks, expected_parts) = SetDisks::shuffle_disks_and_parts_metadata_by_index(&disks, &parts, &fi);
        let (owned_disks, owned_parts) = SetDisks::shuffle_disks_and_parts_metadata_by_index_owned(disks.clone(), parts, &fi);

        assert_eq!(owned_parts, expected_parts, "fallback placement must match the borrowing variant");
        let expected_slots: Vec<bool> = expected_disks.iter().map(Option::is_some).collect();
        let owned_slots: Vec<bool> = owned_disks.iter().map(Option::is_some).collect();
        assert_eq!(owned_slots, expected_slots, "fallback disk slots must match the borrowing variant");
    }
}
