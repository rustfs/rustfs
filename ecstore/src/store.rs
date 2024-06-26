use futures::future::join_all;
use uuid::Uuid;

use crate::{
    disk::{self, DiskAPI, DiskError, DiskOption, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET},
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

    async fn init_format(disks: Vec<Option<impl DiskAPI>>) -> Result<FormatV3, Error> {
        let (formats, errs) = Self::load_format_all(&disks, false).await;
        unimplemented!()
    }

    async fn load_format_all(
        disks: &Vec<Option<impl DiskAPI>>,
        heal: bool,
    ) -> (Vec<Option<FormatV3>>, Vec<Option<Error>>) {
        let mut futures = Vec::with_capacity(disks.len());

        for ep in disks.iter() {
            futures.push(Self::load_format(ep, heal));
        }

        let mut datas = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(s) => {
                    datas.push(Some(s));
                    errors.push(None);
                }
                Err(e) => {
                    datas.push(None);
                    errors.push(Some(e));
                }
            }
        }

        (datas, errors)
    }

    async fn load_format(disk: &Option<impl DiskAPI>, heal: bool) -> Result<FormatV3, Error> {
        if disk.is_none() {
            return Err(Error::new(DiskError::DiskNotFound));
        }
        let disk = disk.as_ref().unwrap();

        let data = disk
            .read_all(RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
            .await
            .map_err(|e| match &e.downcast_ref::<DiskError>() {
                Some(DiskError::FileNotFound) => Error::new(DiskError::UnformattedDisk),
                Some(DiskError::DiskNotFound) => Error::new(DiskError::UnformattedDisk),
                Some(_) => e,
                None => e,
            })?;

        let fm = FormatV3::try_from(data.as_slice())?;

        // TODO: heal

        Ok(fm)
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
