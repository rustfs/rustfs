use uuid::Uuid;

use super::endpoint::Endpoint;
use super::error::Result;

use std::fmt::Debug;

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<Box<dyn DiskAPI>>,
    pub pools: Vec<Sets>,
    pub peer: Vec<String>,
}

impl ECStore {
    pub fn new(endpoints: Vec<String>) -> Result<Self> {
        Ok(ECStore {
            id: Uuid::nil(),
            pools: Vec::new(),
            peer: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub struct Sets {
    pub sets: Vec<Objects>,
}

#[derive(Debug)]
pub struct Objects {
    pub endpoints: Vec<Endpoint>,
    pub disks: Vec<usize>,
    pub set_index: usize,
    pub pool_index: usize,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
}

#[async_trait::async_trait]
trait DiskAPI: Debug + Send + Sync + 'static {}

pub trait StorageAPI {}
