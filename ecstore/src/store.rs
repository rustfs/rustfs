use uuid::Uuid;

use crate::{disks_layout::DisksLayout, endpoint::create_server_endpoints};

use super::endpoint::Endpoint;
use anyhow::Result;

use std::fmt::Debug;

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<Box<dyn DiskAPI>>,
    pub pools: Vec<Sets>,
    pub peer: Vec<String>,
}

impl ECStore {
    pub fn new(address: String, endpoints: Vec<String>) -> Result<Self> {
        let layouts = DisksLayout::new(endpoints)?;

        let (pools, _) = create_server_endpoints(address, &layouts.pools, layouts.legacy)?;

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

pub trait StorageAPI: Debug + Send + Sync + 'static {}
