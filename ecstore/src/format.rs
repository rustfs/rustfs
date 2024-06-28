use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use uuid::Uuid;

use crate::disk;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum FormatMetaVersion {
    #[serde(rename = "1")]
    V1,

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FormatErasureV3 {
    /// Version of 'xl' format.
    pub version: FormatErasureVersion,

    /// This field carries assigned disk uuid.
    pub this: Uuid,

    /// Sets field carries the input disk order generated the first
    /// time when fresh disks were supplied, it is a two dimensional
    /// array second dimension represents list of disks used per set.
    pub sets: Vec<Vec<Uuid>>,

    /// Distribution algorithm represents the hashing algorithm
    /// to pick the right set index for an object.
    #[serde(rename = "distributionAlgo")]
    pub distribution_algo: DistributionAlgoVersion,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
#[derive(Debug, Serialize, Deserialize, Clone)]
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
    // #[serde(skip)]
    // pub disk_info: Option<data_types::DeskInfo>,
}

impl TryFrom<&[u8]> for FormatV3 {
    type Error = JsonError;

    fn try_from(data: &[u8]) -> Result<Self, JsonError> {
        serde_json::from_slice(data)
    }
}

impl TryFrom<&str> for FormatV3 {
    type Error = JsonError;

    fn try_from(data: &str) -> Result<Self, JsonError> {
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
            // disk_info: None,
        }
    }

    /// Returns the number of drives in the erasure set.
    pub fn drives(&self) -> usize {
        self.erasure.sets.iter().map(|v| v.len()).sum()
    }

    pub fn to_json(&self) -> Result<String, JsonError> {
        serde_json::to_string(self)
    }

    /// returns the i,j'th position of the input `diskID` against the reference
    ///
    /// format, after successful validation.
    ///   - i'th position is the set index
    ///   - j'th position is the disk index in the current set
    pub fn find_disk_index_by_disk_id(&self, disk_id: Uuid) -> Result<(usize, usize)> {
        if disk_id == Uuid::nil() {
            return Err(Error::new(disk::DiskError::DiskNotFound));
        }
        if disk_id == Uuid::max() {
            return Err(Error::msg("disk offline"));
        }

        for (i, set) in self.erasure.sets.iter().enumerate() {
            for (j, d) in set.iter().enumerate() {
                if disk_id.eq(d) {
                    return Ok((i, j));
                }
            }
        }

        Err(Error::msg(format!("disk id not found {}", disk_id)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_format_v1() {
        let format = FormatV3::new(1, 4);

        let str = serde_json::to_string(&format);
        println!("{:?}", str);

        let data = r#"
        {
            "version": "1",
            "format": "xl",
            "id": "321b3874-987d-4c15-8fa5-757c956b1243",
            "xl": {
                "version": "1",
                "this": null,
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

        let p = FormatV3::try_from(data);

        println!("{:?}", p);
    }
}
