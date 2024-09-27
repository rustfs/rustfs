use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::Result;

use crate::disk::BUCKET_META_PREFIX;

pub const BUCKET_METADATA_FILE: &str = ".metadata.bin";
pub const BUCKET_METADATA_FORMAT: u16 = 1;
pub const BUCKET_METADATA_VERSION: u16 = 1;

#[derive(Debug, PartialEq, Deserialize, Serialize, Default)]
pub struct BucketMetadata {
    format: u16,
    version: u16,
    pub name: String,
    pub created: Option<OffsetDateTime>,
}

// impl Default for BucketMetadata {
//     fn default() -> Self {
//         Self {
//             format: Default::default(),
//             version: Default::default(),
//             name: Default::default(),
//             created: OffsetDateTime::now_utc(),
//         }
//     }
// }

impl BucketMetadata {
    pub fn new(name: &str) -> Self {
        BucketMetadata {
            format: BUCKET_METADATA_FORMAT,
            version: BUCKET_METADATA_VERSION,
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn save_file_path(&self) -> String {
        format!("{}/{}/{}", BUCKET_META_PREFIX, self.name.as_str(), BUCKET_METADATA_FILE)
        // PathBuf::new()
        //     .join(BUCKET_META_PREFIX)
        //     .join(self.name.as_str())
        //     .join(BUCKET_METADATA_FILE)
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut Serializer::new(&mut buf))?;

        Ok(buf)
    }
}
