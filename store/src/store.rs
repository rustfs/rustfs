use super::endpoint::Endpoint;

pub struct Store {
    pub id: uuid::Uuid,
    pub disks: Vec<Box<dyn DiskAPI>>,
    pub pools: Vec<Sets>,
    pub peer: Vec<String>,
}

impl Store {}

pub struct Sets {
    pub sets: Vec<Objects>,
}

pub struct Objects {
    pub endpoints: Vec<Endpoint>,
    pub disks: Vec<usize>,
    pub set_index: usize,
    pub pool_index: usize,
    pub set_drive_count: usize,
    pub default_parity_count: usize,
}

trait DiskAPI {}

pub trait StorageAPI {}
