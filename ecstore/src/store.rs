use uuid::Uuid;

use crate::{
    disk::{self, DiskAPI, DiskOption},
    disks_layout::DisksLayout,
    endpoint::create_server_endpoints,
    format::FormatV3,
};

use super::endpoint::Endpoint;
use anyhow::{Error, Result};

use std::fmt::Debug;

#[derive(Debug)]
pub struct ECStore {
    pub id: uuid::Uuid,
    // pub disks: Vec<Box<dyn DiskAPI>>,
    pub pools: Vec<Sets>,
    pub peer: Vec<String>,
}

impl ECStore {
    pub async fn new(address: String, endpoints: Vec<String>) -> Result<Self> {
        let layouts = DisksLayout::new(endpoints)?;

        let (pools, _) = create_server_endpoints(address, &layouts.pools, layouts.legacy)?;
        for (i, pool_eps) in pools.iter().enumerate() {
            let (disks, errs) = disk::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: true,
                },
            )
            .await;
        }

        Ok(ECStore {
            id: Uuid::nil(),
            pools: Vec::new(),
            peer: Vec::new(),
        })
    }

    async fn load_formats(
        disks: Vec<Option<impl DiskAPI>>,
        heal: bool,
    ) -> (Vec<FormatV3>, Vec<Error>) {
        unimplemented!()
    }

    fn default_partiy_blocks(drive: usize) -> usize {
        match drive {
            1 => 0,
            2 | 3 => 1,
            4 | 5 => 2,
            6 | 7 => 3,
            _ => 4,
        }
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

pub trait StorageAPI: Debug + Send + Sync + 'static {}
