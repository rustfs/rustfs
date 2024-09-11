use crate::{
    disk::{
        error::DiskError,
        format::{FormatErasureVersion, FormatMetaVersion, FormatV3},
        new_disk, DiskOption, DiskStore, FORMAT_CONFIG_FILE, RUSTFS_META_BUCKET,
    },
    endpoints::Endpoints,
    error::{Error, Result},
};
use futures::future::join_all;
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
};

use tracing::warn;
use uuid::Uuid;

pub async fn init_disks(eps: &Endpoints, opt: &DiskOption) -> (Vec<Option<DiskStore>>, Vec<Option<Error>>) {
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

pub async fn do_init_format_file(
    first_disk: bool,
    disks: &[Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Result<FormatV3, Error> {
    let (formats, errs) = read_format_file_all(disks, false).await;

    DiskError::check_disk_fatal_errs(&errs)?;

    check_format_erasure_values(&formats, set_drive_count)?;

    if first_disk && DiskError::should_init_erasure_disks(&errs) {
        //  UnformattedDisk, not format file create
        // new format and save
        let fms = init_format_files(disks, set_count, set_drive_count, deployment_id);

        let _errs = save_format_file_all(disks, &fms).await;

        // TODO: check quorum
        // reduceWriteQuorumErrs(&errs)?;

        let fm = get_format_file_in_quorum(&fms)?;

        return Ok(fm);
    }

    let unformatted = DiskError::quorum_unformatted_disks(&errs);
    if unformatted && !first_disk {
        return Err(Error::new(ErasureError::NotFirstDisk));
    }

    if unformatted && first_disk {
        return Err(Error::new(ErasureError::FirstDiskWait));
    }

    let fm = get_format_file_in_quorum(&formats)?;

    Ok(fm)
}

fn init_format_files(
    disks: &[Option<DiskStore>],
    set_count: usize,
    set_drive_count: usize,
    deployment_id: Option<Uuid>,
) -> Vec<Option<FormatV3>> {
    let fm = FormatV3::new(set_count, set_drive_count);
    let mut fms = vec![None; disks.len()];
    for i in 0..set_count {
        for j in 0..set_drive_count {
            let idx = i * set_drive_count + j;
            let mut newfm = fm.clone();
            newfm.erasure.this = fm.erasure.sets[i][j];
            if deployment_id.is_some() {
                newfm.id = deployment_id.unwrap();
            }

            fms[idx] = Some(newfm);
        }
    }

    fms
}

fn get_format_file_in_quorum(formats: &[Option<FormatV3>]) -> Result<FormatV3> {
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

    warn!("get_format_file_in_quorum fi: {:?}", &formats);

    if *max_drives == 0 || *max_count < formats.len() / 2 {
        warn!(
            "*max_drives == 0 || *max_count < formats.len() / 2, {} || {}<{}",
            max_drives,
            max_count,
            formats.len() / 2
        );
        return Err(Error::new(ErasureError::ErasureReadQuorum));
    }

    let format = formats
        .iter()
        .find(|f| f.as_ref().map_or(false, |v| v.drives().eq(max_drives)))
        .ok_or(Error::new(ErasureError::ErasureReadQuorum))?;

    let mut format = format.as_ref().unwrap().clone();
    format.erasure.this = Uuid::nil();

    Ok(format)
}

fn check_format_erasure_values(
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
            return Err(Error::msg("formats length for erasure.sets not mtach"));
        }

        if f.erasure.sets[0].len() != set_drive_count {
            return Err(Error::msg("erasure set length not match set_drive_count"));
        }
    }
    Ok(())
}
fn check_format_erasure_value(format: &FormatV3) -> Result<()> {
    if format.version != FormatMetaVersion::V1 {
        return Err(Error::msg("invalid FormatMetaVersion"));
    }

    if format.erasure.version != FormatErasureVersion::V3 {
        return Err(Error::msg("invalid FormatErasureVersion"));
    }
    Ok(())
}

pub fn default_partiy_count(drive: usize) -> usize {
    match drive {
        1 => 0,
        2 | 3 => 1,
        4 | 5 => 2,
        6 | 7 => 3,
        _ => 4,
    }
}
// read_format_file_all 读取所有foramt.json
async fn read_format_file_all(disks: &[Option<DiskStore>], heal: bool) -> (Vec<Option<FormatV3>>, Vec<Option<Error>>) {
    let mut futures = Vec::with_capacity(disks.len());

    for disk in disks.iter() {
        futures.push(read_format_file(disk, heal));
    }

    let mut datas = Vec::with_capacity(disks.len());
    let mut errors = Vec::with_capacity(disks.len());

    let results = join_all(futures).await;
    let mut i = 0;
    for result in results {
        match result {
            Ok(s) => {
                if !heal {
                    let _ = disks[i].as_ref().unwrap().set_disk_id(Some(s.erasure.this.clone())).await;
                }

                datas.push(Some(s));
                errors.push(None);
            }
            Err(e) => {
                datas.push(None);
                errors.push(Some(e));
            }
        }

        i += 1;
    }

    (datas, errors)
}

async fn read_format_file(disk: &Option<DiskStore>, _heal: bool) -> Result<FormatV3, Error> {
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

    let fm = FormatV3::try_from(data.as_ref())?;

    // TODO: heal

    Ok(fm)
}

async fn save_format_file_all(disks: &[Option<DiskStore>], formats: &[Option<FormatV3>]) -> Vec<Option<Error>> {
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

    errors
}

async fn save_format_file(disk: &Option<DiskStore>, format: &Option<FormatV3>) -> Result<()> {
    if disk.is_none() {
        return Err(Error::new(DiskError::DiskNotFound));
    }

    let format = format.as_ref().unwrap();

    let json_data = format.to_json()?;

    let tmpfile = Uuid::new_v4().to_string();

    let disk = disk.as_ref().unwrap();
    disk.write_all(RUSTFS_META_BUCKET, tmpfile.as_str(), json_data.into_bytes())
        .await?;

    disk.rename_file(RUSTFS_META_BUCKET, tmpfile.as_str(), RUSTFS_META_BUCKET, FORMAT_CONFIG_FILE)
        .await?;

    disk.set_disk_id(Some(format.erasure.this)).await?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ErasureError {
    #[error("erasure read quorum")]
    ErasureReadQuorum,

    #[error("erasure write quorum")]
    _ErasureWriteQuorum,

    #[error("not first disk")]
    NotFirstDisk,

    #[error("first disk wiat")]
    FirstDiskWait,

    #[error("invalid part id {0}")]
    InvalidPart(usize),
}
