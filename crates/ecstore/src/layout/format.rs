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

use crate::disk::error::{Error, Result};
use crate::disk::{DiskInfo, error::DiskError};
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum FormatMetaVersion {
    #[serde(rename = "1")]
    V1,

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum FormatBackend {
    #[serde(rename = "xl")]
    Erasure,
    #[serde(rename = "xl-single")]
    ErasureSingle,

    #[serde(other)]
    Unknown,
}

/// Represents the V3 backend disk structure version
/// under `.rustfs.sys` and actual data namespace.
///
/// FormatErasureV3 - structure holds format config version '3'.
///
/// The V3 format to support "large bucket" support where a bucket
/// can span multiple erasure sets.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FormatErasureV3 {
    /// Version of 'xl' format.
    pub version: FormatErasureVersion,

    /// This field carries assigned disk uuid.
    pub this: Uuid,

    /// Sets field carries the input disk order generated the first
    /// time when fresh disks were supplied, it is a two-dimensional
    /// array second dimension represents list of disks used per set.
    pub sets: Vec<Vec<Uuid>>,

    /// Distribution algorithm represents the hashing algorithm
    /// to pick the right set index for an object.
    #[serde(rename = "distributionAlgo")]
    pub distribution_algo: DistributionAlgoVersion,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum FormatErasureVersion {
    #[serde(rename = "1")]
    V1,
    #[serde(rename = "2")]
    V2,
    #[serde(rename = "3")]
    V3,

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum DistributionAlgoVersion {
    #[serde(rename = "CRCMOD")]
    V1,
    #[serde(rename = "SIPMOD")]
    V2,
    #[serde(rename = "SIPMOD+PARITY")]
    V3,
}

/// format.json currently has the format:
///
/// ```json
/// {
///   "version": "1",
///   "format": "XXXXX",
///   "id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
///   "XXXXX": {
//
///   }
/// }
/// ```
///
/// Ideally we will never have a situation where we will have to change the
/// fields of this struct and deal with related migration.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FormatV3 {
    /// Version of the format config.
    pub version: FormatMetaVersion,

    /// Format indicates the backend format type, supports two values 'xl' and 'xl-single'.
    pub format: FormatBackend,

    /// ID is the identifier for the rustfs deployment
    pub id: Uuid,

    #[serde(rename = "xl")]
    pub erasure: FormatErasureV3,
    // /// DiskInfo is an extended type which returns current
    // /// disk usage per path.
    #[serde(skip)]
    pub disk_info: Option<DiskInfo>,
}

pub(crate) type SharedFormatIdentity<'a> = (
    &'a FormatMetaVersion,
    &'a FormatBackend,
    &'a Uuid,
    &'a FormatErasureVersion,
    &'a [Vec<Uuid>],
    &'a DistributionAlgoVersion,
);

impl TryFrom<&[u8]> for FormatV3 {
    type Error = JsonError;

    fn try_from(data: &[u8]) -> std::result::Result<Self, Self::Error> {
        serde_json::from_slice(data)
    }
}

impl TryFrom<&str> for FormatV3 {
    type Error = JsonError;

    fn try_from(data: &str) -> std::result::Result<Self, Self::Error> {
        serde_json::from_str(data)
    }
}

impl FormatV3 {
    /// Create a new format config with the given number of sets and set length.
    pub fn new(num_sets: usize, set_len: usize) -> Self {
        let format = if set_len == 1 {
            FormatBackend::ErasureSingle
        } else {
            FormatBackend::Erasure
        };

        let erasure = FormatErasureV3 {
            version: FormatErasureVersion::V3,
            this: Uuid::nil(),
            sets: (0..num_sets)
                .map(|_| (0..set_len).map(|_| Uuid::new_v4()).collect())
                .collect(),
            distribution_algo: DistributionAlgoVersion::V3,
        };

        Self {
            version: FormatMetaVersion::V1,
            format,
            id: Uuid::new_v4(),
            erasure,
            disk_info: None,
        }
    }

    /// Returns the number of drives in the erasure set.
    pub fn drives(&self) -> usize {
        self.erasure.sets.iter().map(|v| v.len()).sum()
    }

    pub fn to_json(&self) -> std::result::Result<String, JsonError> {
        serde_json::to_string(self)
    }

    /// returns the i,j'th position of the input `diskID` against the reference
    ///
    /// format, after successful validation.
    ///   - i'th position is the set index
    ///   - j'th position is the disk index in the current set
    pub fn find_disk_index_by_disk_id(&self, disk_id: Uuid) -> Result<(usize, usize)> {
        if disk_id == Uuid::nil() {
            return Err(Error::from(DiskError::DiskNotFound));
        }
        if disk_id == Uuid::max() {
            return Err(Error::other("disk offline"));
        }

        for (i, set) in self.erasure.sets.iter().enumerate() {
            for (j, d) in set.iter().enumerate() {
                if disk_id.eq(d) {
                    return Ok((i, j));
                }
            }
        }

        Err(Error::other(format!("disk id not found {disk_id}")))
    }

    pub fn check_other(&self, other: &FormatV3) -> Result<()> {
        if self.shared_identity() != other.shared_identity() {
            return Err(Error::other("storage formats do not match"));
        }

        self.find_disk_index_by_disk_id(other.erasure.this).map(|_| ())
    }

    /// Fields that must agree across every disk in one erasure format,
    /// excluding the disk-specific `this` UUID and runtime-only `disk_info`.
    pub(crate) fn shared_identity(&self) -> SharedFormatIdentity<'_> {
        (
            &self.version,
            &self.format,
            &self.id,
            &self.erasure.version,
            &self.erasure.sets,
            &self.erasure.distribution_algo,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_v1() {
        // A freshly created format must survive a serialize -> parse roundtrip
        // unchanged (identity on every on-disk field).
        let format = FormatV3::new(1, 4);
        let serialized = serde_json::to_string(&format).expect("FormatV3 must serialize to JSON");
        let reparsed = FormatV3::try_from(serialized.as_str()).expect("serialized FormatV3 must parse back");
        assert_eq!(reparsed, format);

        // minio-file-format-compat: this literal pins the on-disk format.json
        // shape (erasure version "1", distributionAlgo "CRCMOD"). `this` always
        // carries the disk's own UUID in real format.json files; a JSON null
        // there was never parseable and never written by MinIO or RustFS.
        let data = r#"
        {
            "version": "1",
            "format": "xl",
            "id": "321b3874-987d-4c15-8fa5-757c956b1243",
            "xl": {
                "version": "1",
                "this": "8ab9a908-f869-4f1f-8e42-eb067ffa7eb5",
                "sets": [
                    [
                        "8ab9a908-f869-4f1f-8e42-eb067ffa7eb5",
                        "c26315da-05cf-4778-a9ea-b44ea09f58c5",
                        "fb87a891-18d3-44cf-a46f-bcc15093a038",
                        "356a925c-57b9-4313-88b3-053edf1104dc"
                    ]
                ],
                "distributionAlgo": "CRCMOD"
            }
        }"#;

        let parsed = FormatV3::try_from(data).expect("pinned v1 format.json literal must keep parsing");

        assert_eq!(parsed.version, FormatMetaVersion::V1);
        assert_eq!(parsed.format, FormatBackend::Erasure);
        assert_eq!(
            parsed.id,
            Uuid::parse_str("321b3874-987d-4c15-8fa5-757c956b1243").expect("literal id is a valid UUID")
        );
        assert_eq!(parsed.erasure.version, FormatErasureVersion::V1);
        assert_eq!(
            parsed.erasure.this,
            Uuid::parse_str("8ab9a908-f869-4f1f-8e42-eb067ffa7eb5").expect("literal this is a valid UUID")
        );
        assert_eq!(parsed.erasure.sets.len(), 1);
        assert_eq!(parsed.erasure.sets[0].len(), 4);
        assert_eq!(parsed.erasure.sets[0][0], parsed.erasure.this);
        assert_eq!(parsed.erasure.distribution_algo, DistributionAlgoVersion::V1);
    }

    #[test]
    fn test_format_v3_new_single_disk() {
        let format = FormatV3::new(1, 1);

        assert_eq!(format.version, FormatMetaVersion::V1);
        assert_eq!(format.format, FormatBackend::ErasureSingle);
        assert_eq!(format.erasure.version, FormatErasureVersion::V3);
        assert_eq!(format.erasure.sets.len(), 1);
        assert_eq!(format.erasure.sets[0].len(), 1);
        assert_eq!(format.erasure.distribution_algo, DistributionAlgoVersion::V3);
        assert_eq!(format.erasure.this, Uuid::nil());
    }

    #[test]
    fn test_format_v3_new_multiple_sets() {
        let format = FormatV3::new(2, 4);

        assert_eq!(format.version, FormatMetaVersion::V1);
        assert_eq!(format.format, FormatBackend::Erasure);
        assert_eq!(format.erasure.version, FormatErasureVersion::V3);
        assert_eq!(format.erasure.sets.len(), 2);
        assert_eq!(format.erasure.sets[0].len(), 4);
        assert_eq!(format.erasure.sets[1].len(), 4);
        assert_eq!(format.erasure.distribution_algo, DistributionAlgoVersion::V3);
    }

    #[test]
    fn test_format_v3_drives() {
        let format = FormatV3::new(2, 4);
        assert_eq!(format.drives(), 8); // 2 sets * 4 drives each

        let format_single = FormatV3::new(1, 1);
        assert_eq!(format_single.drives(), 1); // 1 set * 1 drive
    }

    #[test]
    fn test_format_v3_to_json() {
        let format = FormatV3::new(1, 2);
        let json_result = format.to_json();

        assert!(json_result.is_ok());
        let json_str = json_result.unwrap();
        assert!(json_str.contains("\"version\":\"1\""));
        assert!(json_str.contains("\"format\":\"xl\""));
    }

    #[test]
    fn test_format_v3_from_json() {
        let json_data = r#"{
            "version": "1",
            "format": "xl-single",
            "id": "321b3874-987d-4c15-8fa5-757c956b1243",
            "xl": {
                "version": "3",
                "this": "8ab9a908-f869-4f1f-8e42-eb067ffa7eb5",
                "sets": [
                    [
                        "8ab9a908-f869-4f1f-8e42-eb067ffa7eb5"
                    ]
                ],
                "distributionAlgo": "SIPMOD+PARITY"
            }
        }"#;

        let format = FormatV3::try_from(json_data);
        assert!(format.is_ok());

        let format = format.unwrap();
        assert_eq!(format.format, FormatBackend::ErasureSingle);
        assert_eq!(format.erasure.version, FormatErasureVersion::V3);
        assert_eq!(format.erasure.distribution_algo, DistributionAlgoVersion::V3);
        assert_eq!(format.erasure.sets.len(), 1);
        assert_eq!(format.erasure.sets[0].len(), 1);
    }

    #[test]
    fn test_format_v3_from_bytes() {
        let json_data = r#"{
            "version": "1",
            "format": "xl",
            "id": "321b3874-987d-4c15-8fa5-757c956b1243",
            "xl": {
                "version": "2",
                "this": "00000000-0000-0000-0000-000000000000",
                "sets": [
                    [
                        "8ab9a908-f869-4f1f-8e42-eb067ffa7eb5",
                        "c26315da-05cf-4778-a9ea-b44ea09f58c5"
                    ]
                ],
                "distributionAlgo": "SIPMOD"
            }
        }"#;

        let format = FormatV3::try_from(json_data.as_bytes());
        assert!(format.is_ok());

        let format = format.unwrap();
        assert_eq!(format.erasure.version, FormatErasureVersion::V2);
        assert_eq!(format.erasure.distribution_algo, DistributionAlgoVersion::V2);
        assert_eq!(format.erasure.sets[0].len(), 2);
    }

    #[test]
    fn test_format_v3_invalid_json() {
        let invalid_json = r#"{"invalid": "json"}"#;
        let format = FormatV3::try_from(invalid_json);
        assert!(format.is_err());
    }

    #[test]
    fn test_find_disk_index_by_disk_id() {
        let mut format = FormatV3::new(2, 2);
        let target_disk_id = Uuid::new_v4();
        format.erasure.sets[1][0] = target_disk_id;

        let result = format.find_disk_index_by_disk_id(target_disk_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (1, 0));
    }

    #[test]
    fn test_find_disk_index_nil_uuid() {
        let format = FormatV3::new(1, 2);
        let result = format.find_disk_index_by_disk_id(Uuid::nil());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::DiskNotFound));
    }

    #[test]
    fn test_find_disk_index_max_uuid() {
        let format = FormatV3::new(1, 2);
        let result = format.find_disk_index_by_disk_id(Uuid::max());
        assert!(result.is_err());
    }

    #[test]
    fn test_find_disk_index_not_found() {
        let format = FormatV3::new(1, 2);
        let non_existent_id = Uuid::new_v4();
        let result = format.find_disk_index_by_disk_id(non_existent_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_other_identical() {
        let format1 = FormatV3::new(2, 4);
        let mut format2 = format1.clone();
        format2.erasure.this = format1.erasure.sets[0][0];

        let result = format1.check_other(&format2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_other_rejects_shared_identity_mismatches() {
        type FormatMutation = (&'static str, fn(&mut FormatV3));

        let format = FormatV3::new(1, 2);
        let mutations: [FormatMutation; 5] = [
            ("meta version", |other| other.version = FormatMetaVersion::Unknown),
            ("backend", |other| other.format = FormatBackend::ErasureSingle),
            ("deployment id", |other| other.id = Uuid::new_v4()),
            ("erasure version", |other| other.erasure.version = FormatErasureVersion::V2),
            ("distribution algorithm", |other| {
                other.erasure.distribution_algo = DistributionAlgoVersion::V2
            }),
        ];

        for (field, mutate) in mutations {
            let mut other = format.clone();
            other.erasure.this = format.erasure.sets[0][0];
            mutate(&mut other);

            assert!(format.check_other(&other).is_err(), "{field} mismatch must be rejected");
        }
    }

    #[test]
    fn test_check_other_different_set_count() {
        let format1 = FormatV3::new(2, 4);
        let format2 = FormatV3::new(3, 4);

        let result = format1.check_other(&format2);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_other_different_set_size() {
        let format1 = FormatV3::new(2, 4);
        let format2 = FormatV3::new(2, 6);

        let result = format1.check_other(&format2);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_other_different_disk_id() {
        let format1 = FormatV3::new(1, 2);
        let mut format2 = format1.clone();
        format2.erasure.sets[0][0] = Uuid::new_v4();

        let result = format1.check_other(&format2);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_other_disk_not_in_sets() {
        let format1 = FormatV3::new(1, 2);
        let mut format2 = format1.clone();
        format2.erasure.this = Uuid::new_v4(); // Set to a UUID not in any set

        let result = format1.check_other(&format2);
        assert!(result.is_err());
    }

    #[test]
    fn test_format_meta_version_serialization() {
        let v1 = FormatMetaVersion::V1;
        let json = serde_json::to_string(&v1).unwrap();
        assert_eq!(json, "\"1\"");

        let unknown = FormatMetaVersion::Unknown;
        let deserialized: FormatMetaVersion = serde_json::from_str("\"unknown\"").unwrap();
        assert_eq!(deserialized, unknown);
    }

    #[test]
    fn test_format_backend_serialization() {
        let erasure = FormatBackend::Erasure;
        let json = serde_json::to_string(&erasure).unwrap();
        assert_eq!(json, "\"xl\"");

        let single = FormatBackend::ErasureSingle;
        let json = serde_json::to_string(&single).unwrap();
        assert_eq!(json, "\"xl-single\"");

        let unknown = FormatBackend::Unknown;
        let deserialized: FormatBackend = serde_json::from_str("\"unknown\"").unwrap();
        assert_eq!(deserialized, unknown);
    }

    #[test]
    fn test_format_erasure_version_serialization() {
        let v1 = FormatErasureVersion::V1;
        let json = serde_json::to_string(&v1).unwrap();
        assert_eq!(json, "\"1\"");

        let v2 = FormatErasureVersion::V2;
        let json = serde_json::to_string(&v2).unwrap();
        assert_eq!(json, "\"2\"");

        let v3 = FormatErasureVersion::V3;
        let json = serde_json::to_string(&v3).unwrap();
        assert_eq!(json, "\"3\"");
    }

    #[test]
    fn test_distribution_algo_version_serialization() {
        let v1 = DistributionAlgoVersion::V1;
        let json = serde_json::to_string(&v1).unwrap();
        assert_eq!(json, "\"CRCMOD\"");

        let v2 = DistributionAlgoVersion::V2;
        let json = serde_json::to_string(&v2).unwrap();
        assert_eq!(json, "\"SIPMOD\"");

        let v3 = DistributionAlgoVersion::V3;
        let json = serde_json::to_string(&v3).unwrap();
        assert_eq!(json, "\"SIPMOD+PARITY\"");
    }

    #[test]
    fn test_format_v3_round_trip_serialization() {
        let original = FormatV3::new(2, 3);
        let json = original.to_json().unwrap();
        let deserialized = FormatV3::try_from(json.as_str()).unwrap();

        assert_eq!(original.version, deserialized.version);
        assert_eq!(original.format, deserialized.format);
        assert_eq!(original.erasure.version, deserialized.erasure.version);
        assert_eq!(original.erasure.sets.len(), deserialized.erasure.sets.len());
        assert_eq!(original.erasure.distribution_algo, deserialized.erasure.distribution_algo);
    }
}
