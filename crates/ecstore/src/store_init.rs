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

use crate::config::{KVS, storageclass};
use crate::disk::error_reduce::{count_errs, reduce_write_quorum_errs};
use crate::disk::{self, DiskAPI};
use crate::error::{Error, Result};
use crate::{
    disk::{
        DiskInfoOptions, DiskOption, DiskStore, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET,
        error::DiskError,
        format::{FormatErasureVersion, FormatMetaVersion, FormatV3},
        new_disk,
    },
    endpoints::Endpoints,
};
use futures::future::join_all;
use std::collections::{HashMap, hash_map::Entry};
use tracing::{info, warn};
use uuid::Uuid;

pub async fn init_disks(eps: &Endpoints, opt: &DiskOption) -> (Vec<Option<DiskStore>>, Vec<Option<DiskError>>) {
    let mut futures = Vec::with_capacity(eps.as_ref().len());

    for ep in eps.as_ref().iter() {
        futures.push(new_disk(ep, opt));
    }

    let mut res = Vec::with_capacity(eps.as_ref().len());
    let mut errors = Vec::with_capacity(eps.as_ref().len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(s) => {
                res.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                res.push(None);
                errors.push(Some(e));
            }
        }
    }

    (res, errors)
}

pub async fn connect_load_init_formats(
    first_disk: bool,
    disks: &[Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let (formats, errs) = load_format_erasure_all(disks, false).await;

    // debug!("load_format_erasure_all errs {:?}", &errs);

    check_disk_fatal_errs(&errs)?;

    check_format_erasure_values(&formats, set_drive_count)?;

    if first_disk && should_init_erasure_disks(&errs) {
        //  UnformattedDisk, not format file create
        info!("first_disk && should_init_erasure_disks");
        // new format and save
        let fm = init_format_erasure(disks, set_count, set_drive_count, deployment_id).await?;

        return Ok(fm);
    }

    info!(
        "first_disk: {}, should_init_erasure_disks: {}",
        first_disk,
        should_init_erasure_disks(&errs)
    );

    let unformatted = quorum_unformatted_disks(&errs);
    if unformatted && !first_disk {
        return Err(Error::NotFirstDisk);
    }

    if unformatted && first_disk {
        return Err(Error::FirstDiskWait);
    }

    let fm = get_format_erasure_in_quorum(&formats)?;

    Ok(fm)
}

pub fn quorum_unformatted_disks(errs: &[Option<DiskError>]) -> bool {
    count_errs(errs, &DiskError::UnformattedDisk) > (errs.len() / 2)
}

pub fn should_init_erasure_disks(errs: &[Option<DiskError>]) -> bool {
    count_errs(errs, &DiskError::UnformattedDisk) == errs.len()
}

pub fn check_disk_fatal_errs(errs: &[Option<DiskError>]) -> disk::error::Result<()> {
    if count_errs(errs, &DiskError::UnsupportedDisk) == errs.len() {
        return Err(DiskError::UnsupportedDisk);
    }

    if count_errs(errs, &DiskError::FileAccessDenied) == errs.len() {
        return Err(DiskError::FileAccessDenied);
    }

    if count_errs(errs, &DiskError::DiskNotDir) == errs.len() {
        return Err(DiskError::DiskNotDir);
    }

    Ok(())
}

async fn init_format_erasure(
    disks: &[Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3> {
    let fm = FormatV3::new(set_count, set_drive_count);
    let mut fms = vec![None; disks.len()];
    for i in 0..set_count {
        for j in 0..set_drive_count {
            let idx = i * set_drive_count + j;
            let mut newfm = fm.clone();
            newfm.erasure.this = fm.erasure.sets[i][j];
            if let Some(id) = deployment_id {
                newfm.id = id;
            }

            fms[idx] = Some(newfm);
        }
    }

    save_format_file_all(disks, &fms).await?;

    get_format_erasure_in_quorum(&fms)
}

pub fn get_format_erasure_in_quorum(formats: &[Option<FormatV3>]) -> Result<FormatV3> {
    let mut countmap = HashMap::new();

    for f in formats.iter() {
        if f.is_some() {
            let ds = f.as_ref().unwrap().drives();
            let v = countmap.entry(ds);
            match v {
                Entry::Occupied(mut entry) => *entry.get_mut() += 1,
                Entry::Vacant(vacant) => {
                    vacant.insert(1);
                }
            };
        }
    }

    let (max_drives, max_count) = countmap.iter().max_by_key(|&(_, c)| c).unwrap_or((&0, &0));

    if *max_drives == 0 || *max_count <= formats.len() / 2 {
        warn!("get_format_erasure_in_quorum fi: {:?}", &formats);
        return Err(Error::ErasureReadQuorum);
    }

    let format = formats
        .iter()
        .find(|f| f.as_ref().is_some_and(|v| v.drives().eq(max_drives)))
        .ok_or(Error::ErasureReadQuorum)?;

    let mut format = format.as_ref().unwrap().clone();
    format.erasure.this = Uuid::nil();

    Ok(format)
}

pub fn check_format_erasure_values(
    formats: &[Option<FormatV3>],
    // disks: &Vec<Option<DiskStore>>,
    set_drive_count: usize,
) -> Result<()> {
    for f in formats.iter() {
        if f.is_none() {
            continue;
        }

        let f = f.as_ref().unwrap();

        check_format_erasure_value(f)?;

        if formats.len() != f.erasure.sets.len() * f.erasure.sets[0].len() {
            return Err(Error::other("formats length for erasure.sets not mtach"));
        }

        if f.erasure.sets[0].len() != set_drive_count {
            return Err(Error::other("erasure set length not match set_drive_count"));
        }
    }
    Ok(())
}
fn check_format_erasure_value(format: &FormatV3) -> Result<()> {
    if format.version != FormatMetaVersion::V1 {
        return Err(Error::other("invalid FormatMetaVersion"));
    }

    if format.erasure.version != FormatErasureVersion::V3 {
        return Err(Error::other("invalid FormatErasureVersion"));
    }
    Ok(())
}

// load_format_erasure_all reads all format.json files
pub async fn load_format_erasure_all(disks: &[Option<DiskStore>], heal: bool) -> (Vec<Option<FormatV3>>, Vec<Option<DiskError>>) {
    let mut futures = Vec::with_capacity(disks.len());
    let mut datas = Vec::with_capacity(disks.len());
    let mut errors = Vec::with_capacity(disks.len());

    for disk in disks.iter() {
        futures.push(async move {
            if let Some(disk) = disk {
                load_format_erasure(disk, heal).await
            } else {
                Err(DiskError::DiskNotFound)
            }
        });
    }

    let results = join_all(futures).await;
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(s) => {
                if !heal {
                    let _ = disks[i].as_ref().unwrap().set_disk_id(Some(s.erasure.this)).await;
                }

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

pub async fn load_format_erasure(disk: &DiskStore, heal: bool) -> disk::error::Result<FormatV3> {
    let data = disk
        .read_all(RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await
        .map_err(|e| match e {
            DiskError::FileNotFound => DiskError::UnformattedDisk,
            DiskError::DiskNotFound => DiskError::UnformattedDisk,
            _ => {
                warn!("load_format_erasure err: {:?} {:?}", disk.to_string(), e);
                e
            }
        })?;

    let mut fm = FormatV3::try_from(data.as_ref())?;

    if heal {
        let info = disk
            .disk_info(&DiskInfoOptions {
                noop: heal,
                ..Default::default()
            })
            .await?;
        fm.disk_info = Some(info);
    }

    Ok(fm)
}

async fn save_format_file_all(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> disk::error::Result<()> {
    let mut futures = Vec::with_capacity(disks.len());

    for (i, disk) in disks.iter().enumerate() {
        futures.push(save_format_file(disk, &formats[i]));
    }

    let mut errors = Vec::with_capacity(disks.len());

    let results = join_all(futures).await;
    for result in results {
        match result {
            Ok(_) => {
                errors.push(None);
            }
            Err(e) => {
                errors.push(Some(e));
            }
        }
    }

    if let Some(e) = reduce_write_quorum_errs(&errors, &[], disks.len()) {
        return Err(e);
    }

    Ok(())
}

pub async fn save_format_file(disk: &Option<DiskStore>, format: &Option<FormatV3>) -> disk::error::Result<()> {
    let Some(disk) = disk else {
        return Err(DiskError::DiskNotFound);
    };

    let Some(format) = format else {
        return Err(DiskError::other("format is none"));
    };

    let json_data = format.to_json()?;

    let tmpfile = Uuid::new_v4().to_string();

    disk.write_all(RUSTFS_META_BUCKET, tmpfile.as_str(), json_data.into_bytes().into())
        .await?;

    disk.rename_file(RUSTFS_META_BUCKET, tmpfile.as_str(), RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await?;

    disk.set_disk_id(Some(format.erasure.this)).await?;

    Ok(())
}

pub fn ec_drives_no_config(set_drive_count: usize) -> Result<usize> {
    let sc = storageclass::lookup_config(&KVS::new(), set_drive_count)?;
    Ok(sc.get_parity_for_sc(storageclass::STANDARD).unwrap_or_default())
}

// #[derive(Debug, PartialEq, thiserror::Error)]
// pub enum ErasureError {
//     #[error("erasure read quorum")]
//     ErasureReadQuorum,

//     #[error("erasure write quorum")]
//     _ErasureWriteQuorum,

//     #[error("not first disk")]
//     NotFirstDisk,

//     #[error("first disk wait")]
//     FirstDiskWait,

//     #[error("invalid part id {0}")]
//     InvalidPart(usize),
// }

// impl ErasureError {
//     pub fn is(&self, err: &Error) -> bool {
//         if let Some(e) = err.downcast_ref::<ErasureError>() {
//             return self == e;
//         }

//         false
//     }
// }

// impl ErasureError {
//     pub fn to_u32(&self) -> u32 {
//         match self {
//             ErasureError::ErasureReadQuorum => 0x01,
//             ErasureError::_ErasureWriteQuorum => 0x02,
//             ErasureError::NotFirstDisk => 0x03,
//             ErasureError::FirstDiskWait => 0x04,
//             ErasureError::InvalidPart(_) => 0x05,
//         }
//     }

//     pub fn from_u32(error: u32) -> Option<Self> {
//         match error {
//             0x01 => Some(ErasureError::ErasureReadQuorum),
//             0x02 => Some(ErasureError::_ErasureWriteQuorum),
//             0x03 => Some(ErasureError::NotFirstDisk),
//             0x04 => Some(ErasureError::FirstDiskWait),
//             0x05 => Some(ErasureError::InvalidPart(Default::default())),
//             _ => None,
//         }
//     }
// }
