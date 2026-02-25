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

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone, Eq, PartialOrd, Ord)]
pub struct FileMetaShallowVersion {
    pub header: FileMetaVersionHeader,
    pub meta: Vec<u8>, // FileMetaVersion.marshal_msg
}

impl FileMetaShallowVersion {
    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> Result<FileInfo> {
        let file_version = FileMetaVersion::try_from(self.meta.as_slice())?;

        Ok(file_version.into_fileinfo(volume, path, all_parts))
    }
}

impl TryFrom<FileMetaVersion> for FileMetaShallowVersion {
    type Error = Error;

    fn try_from(value: FileMetaVersion) -> std::result::Result<Self, Self::Error> {
        let header = value.header();
        let meta = value.marshal_msg()?;
        Ok(Self { meta, header })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct FileMetaVersion {
    #[serde(rename = "Type")]
    pub version_type: VersionType,
    #[serde(rename = "V2Obj")]
    pub object: Option<MetaObject>,
    #[serde(rename = "DelObj")]
    pub delete_marker: Option<MetaDeleteMarker>,
    #[serde(rename = "v")]
    pub write_version: u64, // rustfs version
}

impl FileMetaVersion {
    pub fn valid(&self) -> bool {
        if !self.version_type.valid() {
            return false;
        }

        match self.version_type {
            VersionType::Object => self
                .object
                .as_ref()
                .map(|v| v.erasure_algorithm.valid() && v.bitrot_checksum_algo.valid() && v.mod_time.is_some())
                .unwrap_or_default(),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .map(|v| v.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH) > OffsetDateTime::UNIX_EPOCH)
                .unwrap_or_default(),
            _ => false,
        }
    }

    pub fn get_data_dir(&self) -> Option<Uuid> {
        if self.valid() {
            {
                if self.version_type == VersionType::Object {
                    self.object.as_ref().map(|v| v.data_dir).unwrap_or_default()
                } else {
                    None
                }
            }
        } else {
            Default::default()
        }
    }

    pub fn get_version_id(&self) -> Option<Uuid> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.version_id).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.version_id).unwrap_or_default(),
            _ => None,
        }
    }

    pub fn get_mod_time(&self) -> Option<OffsetDateTime> {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            VersionType::Delete => self.delete_marker.as_ref().map(|v| v.mod_time).unwrap_or_default(),
            _ => None,
        }
    }

    // decode_data_dir_from_meta reads data_dir from meta TODO: directly parse only data_dir from meta buf, msg.skip
    pub fn decode_data_dir_from_meta(buf: &[u8]) -> Result<Option<Uuid>> {
        let mut ver = Self::default();
        ver.unmarshal_msg(buf)?;

        let data_dir = ver.object.map(|v| v.data_dir).unwrap_or_default();
        Ok(data_dir)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)
    }

    pub fn free_version(&self) -> bool {
        self.version_type == VersionType::Delete && self.delete_marker.as_ref().map(|m| m.free_version()).unwrap_or_default()
    }

    pub fn header(&self) -> FileMetaVersionHeader {
        FileMetaVersionHeader::from(self.clone())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        match self.version_type {
            VersionType::Invalid | VersionType::Legacy => FileInfo {
                name: path.to_string(),
                volume: volume.to_string(),
                ..Default::default()
            },
            VersionType::Object => self
                .object
                .as_ref()
                .unwrap_or(&MetaObject::default())
                .into_fileinfo(volume, path, all_parts),
            VersionType::Delete => self
                .delete_marker
                .as_ref()
                .unwrap_or(&MetaDeleteMarker::default())
                .into_fileinfo(volume, path, all_parts),
        }
    }

    /// Support for Legacy version type
    pub fn is_legacy(&self) -> bool {
        self.version_type == VersionType::Legacy
    }

    /// Get signature for version
    pub fn get_signature(&self) -> [u8; 4] {
        match self.version_type {
            VersionType::Object => {
                if let Some(ref obj) = self.object {
                    // Calculate signature based on object metadata
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(obj.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = obj.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            VersionType::Delete => {
                if let Some(ref dm) = self.delete_marker {
                    // Calculate signature for delete marker
                    let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
                    hasher.update(dm.version_id.unwrap_or_default().as_bytes());
                    if let Some(mod_time) = dm.mod_time {
                        hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
                    }
                    let hash = hasher.finish();
                    let bytes = hash.to_le_bytes();
                    [bytes[0], bytes[1], bytes[2], bytes[3]]
                } else {
                    [0; 4]
                }
            }
            _ => [0; 4],
        }
    }

    /// Check if this version uses data directory
    pub fn uses_data_dir(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.uses_data_dir()).unwrap_or(false),
            _ => false,
        }
    }

    /// Check if this version uses inline data
    pub fn uses_inline_data(&self) -> bool {
        match self.version_type {
            VersionType::Object => self.object.as_ref().map(|obj| obj.inlinedata()).unwrap_or(false),
            _ => false,
        }
    }
}

impl TryFrom<&[u8]> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let mut ver = FileMetaVersion::default();
        ver.unmarshal_msg(value)?;
        Ok(ver)
    }
}

impl From<FileInfo> for FileMetaVersion {
    fn from(value: FileInfo) -> Self {
        {
            if value.deleted {
                FileMetaVersion {
                    version_type: VersionType::Delete,
                    delete_marker: Some(MetaDeleteMarker::from(value)),
                    object: None,
                    write_version: 0,
                }
            } else {
                FileMetaVersion {
                    version_type: VersionType::Object,
                    delete_marker: None,
                    object: Some(MetaObject::from(value)),
                    write_version: 0,
                }
            }
        }
    }
}

impl TryFrom<FileMetaShallowVersion> for FileMetaVersion {
    type Error = Error;

    fn try_from(value: FileMetaShallowVersion) -> std::result::Result<Self, Self::Error> {
        FileMetaVersion::try_from(value.meta.as_slice())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
pub struct FileMetaVersionHeader {
    pub version_id: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub signature: [u8; 4],
    pub version_type: VersionType,
    pub flags: u8,
    pub ec_n: u8,
    pub ec_m: u8,
}

impl FileMetaVersionHeader {
    pub fn has_ec(&self) -> bool {
        self.ec_m > 0 && self.ec_n > 0
    }

    pub fn matches_not_strict(&self, o: &FileMetaVersionHeader) -> bool {
        let mut ok = self.version_id == o.version_id && self.version_type == o.version_type && self.matches_ec(o);
        if self.version_id.is_none() {
            ok = ok && self.mod_time == o.mod_time;
        }

        ok
    }

    pub fn matches_ec(&self, o: &FileMetaVersionHeader) -> bool {
        if self.has_ec() && o.has_ec() {
            return self.ec_n == o.ec_n && self.ec_m == o.ec_m;
        }

        true
    }

    pub fn free_version(&self) -> bool {
        self.flags & XL_FLAG_FREE_VERSION != 0
    }

    pub fn sorts_before(&self, o: &FileMetaVersionHeader) -> bool {
        if self == o {
            return false;
        }

        // Prefer newest modtime.
        if self.mod_time != o.mod_time {
            return self.mod_time > o.mod_time;
        }

        match self.mod_time.cmp(&o.mod_time) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        // The following doesn't make too much sense, but we want sort to be consistent nonetheless.
        // Prefer lower types
        if self.version_type != o.version_type {
            return self.version_type < o.version_type;
        }
        // Consistent sort on signature
        match self.version_id.cmp(&o.version_id) {
            Ordering::Greater => {
                return true;
            }
            Ordering::Less => {
                return false;
            }
            _ => {}
        }

        if self.flags != o.flags {
            return self.flags > o.flags;
        }

        false
    }

    pub fn uses_data_dir(&self) -> bool {
        self.flags & Flags::UsesDataDir as u8 != 0
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut wr = Vec::new();

        // array len 7
        rmp::encode::write_array_len(&mut wr, 7)?;

        // version_id
        rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or_default().as_bytes())?;
        // mod_time
        rmp::encode::write_i64(&mut wr, self.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH).unix_timestamp_nanos() as i64)?;
        // signature
        rmp::encode::write_bin(&mut wr, self.signature.as_slice())?;
        // version_type
        rmp::encode::write_uint8(&mut wr, self.version_type.to_u8())?;
        // flags
        rmp::encode::write_uint8(&mut wr, self.flags)?;
        // ec_n
        rmp::encode::write_uint8(&mut wr, self.ec_n)?;
        // ec_m
        rmp::encode::write_uint8(&mut wr, self.ec_m)?;

        Ok(wr)
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let mut cur = Cursor::new(buf);
        let alen = rmp::decode::read_array_len(&mut cur)?;
        if alen != 7 {
            return Err(Error::other(format!("version header array len err need 7 got {alen}")));
        }

        // version_id
        rmp::decode::read_bin_len(&mut cur)?;
        let mut buf = [0u8; 16];
        cur.read_exact(&mut buf)?;
        self.version_id = {
            let id = Uuid::from_bytes(buf);
            // if id.is_nil() { None } else { Some(id) }
            Some(id)
        };

        // mod_time
        let unix: i128 = rmp::decode::read_int(&mut cur)?;

        let time = OffsetDateTime::from_unix_timestamp_nanos(unix)?;
        if time == OffsetDateTime::UNIX_EPOCH {
            self.mod_time = None;
        } else {
            self.mod_time = Some(time);
        }

        // signature
        rmp::decode::read_bin_len(&mut cur)?;
        cur.read_exact(&mut self.signature)?;

        // version_type
        let typ: u8 = rmp::decode::read_int(&mut cur)?;
        self.version_type = VersionType::from_u8(typ);

        // flags
        self.flags = rmp::decode::read_int(&mut cur)?;
        // ec_n
        self.ec_n = rmp::decode::read_int(&mut cur)?;
        // ec_m
        self.ec_m = rmp::decode::read_int(&mut cur)?;

        Ok(cur.position())
    }

    /// Get signature for header
    pub fn get_signature(&self) -> [u8; 4] {
        self.signature
    }

    /// Check if this header represents inline data
    pub fn inline_data(&self) -> bool {
        self.flags & Flags::InlineData as u8 != 0
    }

    /// Update signature based on version content
    pub fn update_signature(&mut self, version: &FileMetaVersion) {
        self.signature = version.get_signature();
    }
}

impl PartialOrd for FileMetaVersionHeader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileMetaVersionHeader {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.mod_time.cmp(&other.mod_time) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        match self.version_type.cmp(&other.version_type) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.signature.cmp(&other.signature) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.version_id.cmp(&other.version_id) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.flags.cmp(&other.flags)
    }
}

impl From<FileMetaVersion> for FileMetaVersionHeader {
    fn from(value: FileMetaVersion) -> Self {
        let flags = {
            let mut f: u8 = 0;
            if value.free_version() {
                f |= Flags::FreeVersion as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.uses_data_dir()).unwrap_or_default() {
                f |= Flags::UsesDataDir as u8;
            }

            if value.version_type == VersionType::Object && value.object.as_ref().map(|v| v.inlinedata()).unwrap_or_default() {
                f |= Flags::InlineData as u8;
            }

            f
        };

        let (ec_n, ec_m) = match (value.version_type == VersionType::Object, value.object.as_ref()) {
            (true, Some(obj)) => (obj.erasure_n as u8, obj.erasure_m as u8),
            _ => (0, 0),
        };

        Self {
            version_id: value.get_version_id(),
            mod_time: value.get_mod_time(),
            signature: [0, 0, 0, 0],
            version_type: value.version_type,
            flags,
            ec_n,
            ec_m,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
// Because of custom message_pack, field order must be guaranteed
pub struct MetaObject {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID
    #[serde(rename = "DDir")]
    pub data_dir: Option<Uuid>, // Data dir ID
    #[serde(rename = "EcAlgo")]
    pub erasure_algorithm: ErasureAlgo, // Erasure coding algorithm
    #[serde(rename = "EcM")]
    pub erasure_m: usize, // Erasure data blocks
    #[serde(rename = "EcN")]
    pub erasure_n: usize, // Erasure parity blocks
    #[serde(rename = "EcBSize")]
    pub erasure_block_size: usize, // Erasure block size
    #[serde(rename = "EcIndex")]
    pub erasure_index: usize, // Erasure disk index
    #[serde(rename = "EcDist")]
    pub erasure_dist: Vec<u8>, // Erasure distribution
    #[serde(rename = "CSumAlgo")]
    pub bitrot_checksum_algo: ChecksumAlgo, // Bitrot checksum algo
    #[serde(rename = "PartNums")]
    pub part_numbers: Vec<usize>, // Part Numbers
    #[serde(rename = "PartETags")]
    pub part_etags: Vec<String>, // Part ETags
    #[serde(rename = "PartSizes")]
    pub part_sizes: Vec<usize>, // Part Sizes
    #[serde(rename = "PartASizes")]
    pub part_actual_sizes: Vec<i64>, // Part ActualSizes (compression)
    #[serde(rename = "PartIdx")]
    pub part_indices: Vec<Bytes>, // Part Indexes (compression)
    #[serde(rename = "Size")]
    pub size: i64, // Object version size
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object version modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Object version internal metadata
    #[serde(rename = "MetaUsr")]
    pub meta_user: HashMap<String, String>, // Object version metadata set by user
}

impl MetaObject {
    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)
    }
    // marshal_msg custom messagepack naming consistent with go
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        let version_id = self.version_id.filter(|&vid| !vid.is_nil());

        let parts = if all_parts {
            let mut parts = vec![ObjectPartInfo::default(); self.part_numbers.len()];

            for (i, part) in parts.iter_mut().enumerate() {
                part.number = self.part_numbers[i];
                part.size = self.part_sizes[i];
                part.actual_size = self.part_actual_sizes[i];

                if self.part_etags.len() == self.part_numbers.len() {
                    part.etag = self.part_etags[i].clone();
                }

                if self.part_indices.len() == self.part_numbers.len() {
                    part.index = if self.part_indices[i].is_empty() {
                        None
                    } else {
                        Some(self.part_indices[i].clone())
                    };
                }
            }
            parts
        } else {
            Vec::new()
        };

        let mut metadata = HashMap::with_capacity(self.meta_user.len() + self.meta_sys.len());
        for (k, v) in &self.meta_user {
            if k == AMZ_META_UNENCRYPTED_CONTENT_LENGTH || k == AMZ_META_UNENCRYPTED_CONTENT_MD5 {
                continue;
            }

            if k == AMZ_STORAGE_CLASS && v == "STANDARD" {
                continue;
            }

            metadata.insert(k.to_owned(), v.to_owned());
        }

        let tier_fvidkey = format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_ID}").to_lowercase();
        let tier_fvmarker_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TIER_FV_MARKER}").to_lowercase();

        for (k, v) in &self.meta_sys {
            let lower_k = k.to_lowercase();

            if lower_k == tier_fvidkey || lower_k == tier_fvmarker_key {
                continue;
            }

            if lower_k == AMZ_STORAGE_CLASS.to_lowercase() && v == b"STANDARD" {
                continue;
            }

            if k.starts_with(RESERVED_METADATA_PREFIX)
                || k.starts_with(RESERVED_METADATA_PREFIX_LOWER)
                || lower_k == VERSION_PURGE_STATUS_KEY.to_lowercase()
            {
                metadata.insert(k.to_owned(), String::from_utf8(v.to_owned()).unwrap_or_default());
            }
        }

        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut deleted = false;

        if let Some(v) = replication_state_internal.as_ref() {
            if !v.composite_version_purge_status().is_empty() {
                deleted = true;
            }

            let st = v.composite_replication_status();
            if !st.is_empty() {
                metadata.insert(AMZ_BUCKET_REPLICATION_STATUS.to_string(), st.to_string());
            }
        }

        let checksum = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}crc").as_str())
            .map(|v| Bytes::from(v.clone()));

        let erasure = ErasureInfo {
            algorithm: self.erasure_algorithm.to_string(),
            data_blocks: self.erasure_m,
            parity_blocks: self.erasure_n,
            block_size: self.erasure_block_size,
            index: self.erasure_index,
            distribution: self.erasure_dist.iter().map(|&v| v as usize).collect(),
            ..Default::default()
        };

        let transition_status = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        let transitioned_objname = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();
        let transition_version_id = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}").as_str())
            .map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        let transition_tier = self
            .meta_sys
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}").as_str())
            .map(|v| String::from_utf8_lossy(v).to_string())
            .unwrap_or_default();

        FileInfo {
            version_id,
            erasure,
            data_dir: self.data_dir,
            mod_time: self.mod_time,
            size: self.size,
            name: path.to_string(),
            volume: volume.to_string(),
            parts,
            metadata,
            replication_state_internal,
            deleted,
            checksum,
            transition_status,
            transitioned_objname,
            transition_version_id,
            transition_tier,
            ..Default::default()
        }
    }

    pub fn set_transition(&mut self, fi: &FileInfo) {
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"),
            fi.transition_status.as_bytes().to_vec(),
        );
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}"),
            fi.transitioned_objname.as_bytes().to_vec(),
        );
        if let Some(transition_version_id) = fi.transition_version_id.as_ref() {
            self.meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"),
                transition_version_id.as_bytes().to_vec(),
            );
        }
        self.meta_sys.insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}"),
            fi.transition_tier.as_bytes().to_vec(),
        );
    }

    pub fn remove_restore_hdrs(&mut self) {
        self.meta_user.remove(X_AMZ_RESTORE.as_str());
        self.meta_user.remove(AMZ_RESTORE_EXPIRY_DAYS);
        self.meta_user.remove(AMZ_RESTORE_REQUEST_DATE);
    }

    pub fn uses_data_dir(&self) -> bool {
        if let Some(status) = self
            .meta_sys
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"))
            && *status == TRANSITION_COMPLETE.as_bytes().to_vec()
        {
            return false;
        }

        is_restored_object_on_disk(&self.meta_user)
    }

    pub fn inlinedata(&self) -> bool {
        self.meta_sys
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").as_str())
    }

    pub fn reset_inline_data(&mut self) {
        self.meta_sys
            .remove(format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data").as_str());
    }

    /// Remove restore headers
    pub fn remove_restore_headers(&mut self) {
        // Remove any restore-related metadata
        self.meta_sys.retain(|k, _| !k.starts_with("X-Amz-Restore"));
    }

    /// Get object signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        hasher.update(&self.size.to_le_bytes());
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    pub fn init_free_version(&self, fi: &FileInfo) -> (FileMetaVersion, bool) {
        if fi.skip_tier_free_version() {
            return (FileMetaVersion::default(), false);
        }
        if let Some(status) = self
            .meta_sys
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"))
            && *status == TRANSITION_COMPLETE.as_bytes().to_vec()
        {
            let vid = Uuid::parse_str(&fi.tier_free_version_id());
            if let Err(err) = vid {
                panic!("Invalid Tier Object delete marker versionId {} {}", fi.tier_free_version_id(), err);
            }
            let vid = vid.unwrap();
            let mut free_entry = FileMetaVersion {
                version_type: VersionType::Delete,
                write_version: 0,
                ..Default::default()
            };
            free_entry.delete_marker = Some(MetaDeleteMarker {
                version_id: Some(vid),
                mod_time: self.mod_time,
                meta_sys: HashMap::<String, Vec<u8>>::new(),
            });

            let delete_marker = free_entry.delete_marker.as_mut().unwrap();

            delete_marker
                .meta_sys
                .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}{FREE_VERSION}"), vec![]);

            let tier_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}");
            let tier_obj_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}");
            let tier_obj_vid_key = format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}");

            let aa = [tier_key, tier_obj_key, tier_obj_vid_key];
            for (k, v) in &self.meta_sys {
                if aa.contains(k) {
                    delete_marker.meta_sys.insert(k.clone(), v.clone());
                }
            }
            return (free_entry, true);
        }
        (FileMetaVersion::default(), false)
    }
}

impl From<FileInfo> for MetaObject {
    fn from(value: FileInfo) -> Self {
        let part_etags = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.etag.clone()).collect()
        } else {
            vec![]
        };

        let part_indices = if !value.parts.is_empty() {
            value.parts.iter().map(|v| v.index.clone().unwrap_or_default()).collect()
        } else {
            vec![]
        };

        let mut meta_sys = HashMap::new();
        let mut meta_user = HashMap::new();
        for (k, v) in value.metadata.iter() {
            if k.len() > RESERVED_METADATA_PREFIX.len()
                && (k.starts_with(RESERVED_METADATA_PREFIX) || k.starts_with(RESERVED_METADATA_PREFIX_LOWER))
            {
                if k == headers::X_RUSTFS_HEALING || k == headers::X_RUSTFS_DATA_MOV {
                    continue;
                }

                meta_sys.insert(k.to_owned(), v.as_bytes().to_vec());
            } else {
                meta_user.insert(k.to_owned(), v.to_owned());
            }
        }

        if !value.transition_status.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_STATUS}"),
                value.transition_status.as_bytes().to_vec(),
            );
        }

        if !value.transitioned_objname.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}"),
                value.transitioned_objname.as_bytes().to_vec(),
            );
        }

        if let Some(vid) = &value.transition_version_id {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}"),
                vid.as_bytes().to_vec(),
            );
        }

        if !value.transition_tier.is_empty() {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}"),
                value.transition_tier.as_bytes().to_vec(),
            );
        }

        if let Some(content_hash) = value.checksum {
            meta_sys.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}crc"), content_hash.to_vec());
        }

        Self {
            version_id: value.version_id,
            data_dir: value.data_dir,
            size: value.size,
            mod_time: value.mod_time,
            erasure_algorithm: ErasureAlgo::ReedSolomon,
            erasure_m: value.erasure.data_blocks,
            erasure_n: value.erasure.parity_blocks,
            erasure_block_size: value.erasure.block_size,
            erasure_index: value.erasure.index,
            erasure_dist: value.erasure.distribution.iter().map(|x| *x as u8).collect(),
            bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
            part_numbers: value.parts.iter().map(|v| v.number).collect(),
            part_etags,
            part_sizes: value.parts.iter().map(|v| v.size).collect(),
            part_actual_sizes: value.parts.iter().map(|v| v.actual_size).collect(),
            part_indices,
            meta_sys,
            meta_user,
        }
    }
}

fn get_internal_replication_state(metadata: &HashMap<String, String>) -> Option<ReplicationState> {
    let mut rs = ReplicationState::default();
    let mut has = false;

    for (k, v) in metadata.iter() {
        if k == VERSION_PURGE_STATUS_KEY {
            rs.version_purge_status_internal = Some(v.clone());
            rs.purge_targets = version_purge_statuses_map(v.as_str());
            has = true;
            continue;
        }

        if let Some(sub_key) = k.strip_prefix(RESERVED_METADATA_PREFIX_LOWER) {
            match sub_key {
                "replica-timestamp" => {
                    has = true;
                    rs.replica_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH));
                }
                "replica-status" => {
                    has = true;
                    rs.replica_status = ReplicationStatusType::from(v.as_str());
                }
                "replication-timestamp" => {
                    has = true;
                    rs.replication_timestamp = Some(OffsetDateTime::parse(v, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH))
                }
                "replication-status" => {
                    has = true;
                    rs.replication_status_internal = Some(v.clone());
                    rs.targets = replication_statuses_map(v.as_str());
                }
                _ => {
                    if let Some(arn) = sub_key.strip_prefix("replication-reset-") {
                        has = true;
                        rs.reset_statuses_map.insert(arn.to_string(), v.clone());
                    }
                }
            }
        }
    }

    if has { Some(rs) } else { None }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct MetaDeleteMarker {
    #[serde(rename = "ID")]
    pub version_id: Option<Uuid>, // Version ID for delete marker
    #[serde(rename = "MTime")]
    pub mod_time: Option<OffsetDateTime>, // Object delete marker modified time
    #[serde(rename = "MetaSys")]
    pub meta_sys: HashMap<String, Vec<u8>>, // Delete marker internal metadata
}

impl MetaDeleteMarker {
    pub fn free_version(&self) -> bool {
        self.meta_sys
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}{FREE_VERSION}").as_str())
    }

    pub fn into_fileinfo(&self, volume: &str, path: &str, _all_parts: bool) -> FileInfo {
        let metadata = self
            .meta_sys
            .clone()
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).to_string()))
            .collect();
        let replication_state_internal = get_internal_replication_state(&metadata);

        let mut fi = FileInfo {
            version_id: self.version_id.filter(|&vid| !vid.is_nil()),
            name: path.to_string(),
            volume: volume.to_string(),
            deleted: true,
            mod_time: self.mod_time,
            metadata,
            replication_state_internal,
            ..Default::default()
        };

        if self.free_version() {
            fi.set_tier_free_version();
            fi.transition_tier = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITION_TIER}").as_str())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();

            fi.transitioned_objname = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_OBJECTNAME}").as_str())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();

            fi.transition_version_id = self
                .meta_sys
                .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{TRANSITIONED_VERSION_ID}").as_str())
                .map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default());
        }

        fi
    }

    pub fn unmarshal_msg(&mut self, buf: &[u8]) -> Result<u64> {
        let ret: Self = rmp_serde::from_slice(buf)?;

        *self = ret;

        Ok(buf.len() as u64)

        // let mut cur = Cursor::new(buf);

        // let mut fields_len = rmp::decode::read_map_len(&mut cur)?;

        // while fields_len > 0 {
        //     fields_len -= 1;

        //     let str_len = rmp::decode::read_str_len(&mut cur)?;

        //     // !!! Vec::with_capacity(str_len) fails, vec! works normally
        //     let mut field_buff = vec![0u8; str_len as usize];

        //     cur.read_exact(&mut field_buff)?;

        //     let field = String::from_utf8(field_buff)?;

        //     match field.as_str() {
        //         "ID" => {
        //             rmp::decode::read_bin_len(&mut cur)?;
        //             let mut buf = [0u8; 16];
        //             cur.read_exact(&mut buf)?;
        //             self.version_id = {
        //                 let id = Uuid::from_bytes(buf);
        //                 if id.is_nil() { None } else { Some(id) }
        //             };
        //         }

        //         "MTime" => {
        //             let unix: i64 = rmp::decode::read_int(&mut cur)?;
        //             let time = OffsetDateTime::from_unix_timestamp(unix)?;
        //             if time == OffsetDateTime::UNIX_EPOCH {
        //                 self.mod_time = None;
        //             } else {
        //                 self.mod_time = Some(time);
        //             }
        //         }
        //         "MetaSys" => {
        //             let l = rmp::decode::read_map_len(&mut cur)?;
        //             let mut map = HashMap::new();
        //             for _ in 0..l {
        //                 let str_len = rmp::decode::read_str_len(&mut cur)?;
        //                 let mut field_buff = vec![0u8; str_len as usize];
        //                 cur.read_exact(&mut field_buff)?;
        //                 let key = String::from_utf8(field_buff)?;

        //                 let blen = rmp::decode::read_bin_len(&mut cur)?;
        //                 let mut val = vec![0u8; blen as usize];
        //                 cur.read_exact(&mut val)?;

        //                 map.insert(key, val);
        //             }

        //             self.meta_sys = Some(map);
        //         }
        //         name => return Err(Error::other(format!("not support field name {name}"))),
        //     }
        // }

        // Ok(cur.position())
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let buf = rmp_serde::to_vec(self)?;
        Ok(buf)

        // let mut len: u32 = 3;
        // let mut mask: u8 = 0;

        // if self.meta_sys.is_none() {
        //     len -= 1;
        //     mask |= 0x4;
        // }

        // let mut wr = Vec::new();

        // // Field count
        // rmp::encode::write_map_len(&mut wr, len)?;

        // // string "ID"
        // rmp::encode::write_str(&mut wr, "ID")?;
        // rmp::encode::write_bin(&mut wr, self.version_id.unwrap_or_default().as_bytes())?;

        // // string "MTime"
        // rmp::encode::write_str(&mut wr, "MTime")?;
        // rmp::encode::write_uint(
        //     &mut wr,
        //     self.mod_time
        //         .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        //         .unix_timestamp()
        //         .try_into()
        //         .unwrap(),
        // )?;

        // if (mask & 0x4) == 0 {
        //     let metas = self.meta_sys.as_ref().unwrap();
        //     rmp::encode::write_map_len(&mut wr, metas.len() as u32)?;
        //     for (k, v) in metas {
        //         rmp::encode::write_str(&mut wr, k.as_str())?;
        //         rmp::encode::write_bin(&mut wr, v)?;
        //     }
        // }

        // Ok(wr)
    }

    /// Get delete marker signature
    pub fn get_signature(&self) -> [u8; 4] {
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(XXHASH_SEED);
        hasher.update(self.version_id.unwrap_or_default().as_bytes());
        if let Some(mod_time) = self.mod_time {
            hasher.update(&mod_time.unix_timestamp_nanos().to_le_bytes());
        }
        let hash = hasher.finish();
        let bytes = hash.to_le_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }
}

impl From<FileInfo> for MetaDeleteMarker {
    fn from(value: FileInfo) -> Self {
        Self {
            version_id: value.version_id,
            mod_time: value.mod_time,
            meta_sys: HashMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Default, Clone, PartialOrd, Ord, Hash)]
pub enum VersionType {
    #[default]
    Invalid = 0,
    Object = 1,
    Delete = 2,
    Legacy = 3,
}

impl VersionType {
    pub fn valid(&self) -> bool {
        matches!(*self, VersionType::Object | VersionType::Delete | VersionType::Legacy)
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            VersionType::Invalid => 0,
            VersionType::Object => 1,
            VersionType::Delete => 2,
            VersionType::Legacy => 3,
        }
    }

    pub fn from_u8(n: u8) -> Self {
        match n {
            1 => VersionType::Object,
            2 => VersionType::Delete,
            3 => VersionType::Legacy,
            _ => VersionType::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum ChecksumAlgo {
    #[default]
    Invalid = 0,
    HighwayHash = 1,
}

impl ChecksumAlgo {
    pub fn valid(&self) -> bool {
        *self > ChecksumAlgo::Invalid
    }
    pub fn to_u8(&self) -> u8 {
        match self {
            ChecksumAlgo::Invalid => 0,
            ChecksumAlgo::HighwayHash => 1,
        }
    }
    pub fn from_u8(u: u8) -> Self {
        match u {
            1 => ChecksumAlgo::HighwayHash,
            _ => ChecksumAlgo::Invalid,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Default, Clone)]
pub enum Flags {
    #[default]
    FreeVersion = 1 << 0,
    UsesDataDir = 1 << 1,
    InlineData = 1 << 2,
}

// mergeXLV2Versions
pub fn merge_file_meta_versions(
    mut quorum: usize,
    mut strict: bool,
    requested_versions: usize,
    versions: &[Vec<FileMetaShallowVersion>],
) -> Vec<FileMetaShallowVersion> {
    if quorum == 0 {
        quorum = 1;
    }

    if versions.len() < quorum || versions.is_empty() {
        return Vec::new();
    }

    if versions.len() == 1 {
        return versions[0].clone();
    }

    if quorum == 1 {
        strict = true;
    }

    let mut versions = versions.to_owned();

    let mut n_versions = 0;

    let mut merged = Vec::new();
    loop {
        let mut tops = Vec::new();
        let mut top_sig = FileMetaVersionHeader::default();
        let mut consistent = true;
        for vers in versions.iter() {
            if vers.is_empty() {
                consistent = false;
                continue;
            }
            if tops.is_empty() {
                consistent = true;
                top_sig = vers[0].header.clone();
            } else {
                consistent = consistent && vers[0].header == top_sig;
            }
            tops.push(vers[0].clone());
        }

        // check if done...
        if tops.len() < quorum {
            break;
        }

        let mut latest = FileMetaShallowVersion::default();
        if consistent {
            merged.push(tops[0].clone());
            if !tops[0].header.free_version() {
                n_versions += 1;
            }
        } else {
            let mut latest_count = 0;
            for (i, ver) in tops.iter().enumerate() {
                if ver.header == latest.header {
                    latest_count += 1;
                    continue;
                }

                if i == 0 || ver.header.sorts_before(&latest.header) {
                    if i == 0 || latest_count == 0 {
                        latest_count = 1;
                    } else if !strict && ver.header.matches_not_strict(&latest.header) {
                        latest_count += 1;
                    } else {
                        latest_count = 1;
                    }
                    latest = ver.clone();
                    continue;
                }

                // Mismatch, but older.
                if latest_count > 0 && !strict && ver.header.matches_not_strict(&latest.header) {
                    latest_count += 1;
                    continue;
                }

                if latest_count > 0 && ver.header.version_id == latest.header.version_id {
                    let mut x: HashMap<FileMetaVersionHeader, usize> = HashMap::new();
                    for a in tops.iter() {
                        if a.header.version_id != ver.header.version_id {
                            continue;
                        }
                        let mut a_clone = a.clone();
                        if !strict {
                            a_clone.header.signature = [0; 4];
                        }
                        *x.entry(a_clone.header).or_insert(0) += 1;
                    }
                    latest_count = 0;
                    for (k, v) in x.iter() {
                        if *v < latest_count {
                            continue;
                        }
                        if *v == latest_count && latest.header.sorts_before(k) {
                            continue;
                        }
                        tops.iter().for_each(|a| {
                            let mut hdr = a.header.clone();
                            if !strict {
                                hdr.signature = [0; 4];
                            }
                            if hdr == *k {
                                latest = a.clone();
                            }
                        });

                        latest_count = *v;
                    }
                    break;
                }
            }
            if latest_count >= quorum {
                if !latest.header.free_version() {
                    n_versions += 1;
                }
                merged.push(latest.clone());
            }
        }

        // Remove from all streams up until latest modtime or if selected.
        versions.iter_mut().for_each(|vers| {
            // // Keep top entry (and remaining)...
            let mut bre = false;
            vers.retain(|ver| {
                if bre {
                    return true;
                }
                if let Ordering::Greater = ver.header.mod_time.cmp(&latest.header.mod_time) {
                    bre = true;
                    return false;
                }
                if ver.header == latest.header {
                    bre = true;
                    return false;
                }
                if let Ordering::Equal = latest.header.version_id.cmp(&ver.header.version_id) {
                    bre = true;
                    return false;
                }
                for merged_v in merged.iter() {
                    if let Ordering::Equal = ver.header.version_id.cmp(&merged_v.header.version_id) {
                        bre = true;
                        return false;
                    }
                }
                true
            });
        });
        if requested_versions > 0 && requested_versions == n_versions {
            merged.append(&mut versions[0]);
            break;
        }
    }

    // Sanity check. Enable if duplicates show up.
    // todo
    merged
}

pub fn file_info_from_raw(
    ri: RawFileInfo,
    bucket: &str,
    object: &str,
    read_data: bool,
    include_free_versions: bool,
) -> Result<FileInfo> {
    get_file_info(
        &ri.buf,
        bucket,
        object,
        "",
        FileInfoOpts {
            data: read_data,
            include_free_versions,
        },
    )
}

pub struct FileInfoOpts {
    pub data: bool,
    pub include_free_versions: bool,
}

pub fn get_file_info(buf: &[u8], volume: &str, path: &str, version_id: &str, opts: FileInfoOpts) -> Result<FileInfo> {
    let vid = {
        if version_id.is_empty() {
            None
        } else {
            Some(Uuid::parse_str(version_id)?)
        }
    };

    let meta = FileMeta::load(buf)?;
    if meta.versions.is_empty() {
        return Ok(FileInfo {
            volume: volume.to_owned(),
            name: path.to_owned(),
            version_id: vid,
            is_latest: true,
            deleted: true,
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1)?),
            ..Default::default()
        });
    }

    let fi = meta.into_fileinfo(volume, path, version_id, opts.data, opts.include_free_versions, true)?;
    Ok(fi)
}

async fn read_more<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    total_size: usize,
    read_size: usize,
    has_full: bool,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let has = buf.len();

    if has >= read_size {
        return Ok(());
    }

    if has_full || read_size > total_size {
        return Err(Error::other(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF")));
    }

    let extra = read_size - has;
    if buf.capacity() >= read_size {
        // Extend the buffer if we have enough space.
        buf.resize(read_size, 0);
    } else {
        buf.extend(vec![0u8; extra]);
    }

    reader.read_exact(&mut buf[has..]).await?;
    Ok(())
}

pub async fn read_xl_meta_no_data<R: AsyncRead + Unpin>(reader: &mut R, size: usize) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut initial = size;
    let mut has_full = true;

    if initial > META_DATA_READ_DEFAULT {
        initial = META_DATA_READ_DEFAULT;
        has_full = false;
    }

    let mut buf = vec![0u8; initial];
    reader.read_exact(&mut buf).await?;

    let (tmp_buf, major, minor) = FileMeta::check_xl2_v1(&buf)?;

    match major {
        1 => match minor {
            0 => {
                read_more(reader, &mut buf, size, size, has_full).await?;
                Ok(buf)
            }
            1..=3 => {
                let (sz, tmp_buf) = FileMeta::read_bytes_header(tmp_buf)?;
                let mut want = sz as usize + (buf.len() - tmp_buf.len());

                if minor < 2 {
                    read_more(reader, &mut buf, size, want, has_full).await?;
                    buf.truncate(want);
                    return Ok(buf);
                }

                let want_max = usize::min(want + MSGP_UINT32_SIZE, size);
                read_more(reader, &mut buf, size, want_max, has_full).await?;

                if buf.len() < want {
                    return Err(Error::FileCorrupt);
                }

                let tmp = &buf[want..];
                let crc_size = 5;
                let other_size = tmp.len() - crc_size;

                want += tmp.len() - other_size;

                buf.truncate(want);
                Ok(buf)
            }
            _ => Err(Error::other(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unknown minor metadata version",
            ))),
        },
        _ => Err(Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unknown major metadata version",
        ))),
    }
}
