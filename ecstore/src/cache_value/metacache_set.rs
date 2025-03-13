use crate::{
    disk::error::{is_err_eof, is_err_file_not_found, is_err_volume_not_found, DiskError},
    metacache::writer::MetacacheReader,
};
use crate::{
    disk::{DiskAPI, DiskStore, MetaCacheEntries, MetaCacheEntry, WalkDirOptions},
    error::{Error, Result},
};
use futures::future::join_all;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{spawn, sync::broadcast::Receiver as B_Receiver};
use tracing::{error, info};

type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
type PartialFn = Box<dyn Fn(MetaCacheEntries, &[Option<Error>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
type FinishedFn = Box<dyn Fn(&[Option<Error>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

#[derive(Default)]
pub struct ListPathRawOptions {
    pub disks: Vec<Option<DiskStore>>,
    pub fallback_disks: Vec<Option<DiskStore>>,
    pub bucket: String,
    pub path: String,
    pub recursice: bool,
    pub filter_prefix: Option<String>,
    pub forward_to: Option<String>,
    pub min_disks: usize,
    pub report_not_found: bool,
    pub per_disk_limit: i32,
    pub agreed: Option<AgreedFn>,
    pub partial: Option<PartialFn>,
    pub finished: Option<FinishedFn>,
    // pub agreed: Option<Arc<dyn Fn(MetaCacheEntry) + Send + Sync>>,
    // pub partial: Option<Arc<dyn Fn(MetaCacheEntries, &[Option<Error>]) + Send + Sync>>,
    // pub finished: Option<Arc<dyn Fn(&[Option<Error>]) + Send + Sync>>,
}

impl Clone for ListPathRawOptions {
    fn clone(&self) -> Self {
        Self {
            disks: self.disks.clone(),
            fallback_disks: self.fallback_disks.clone(),
            bucket: self.bucket.clone(),
            path: self.path.clone(),
            recursice: self.recursice,
            filter_prefix: self.filter_prefix.clone(),
            forward_to: self.forward_to.clone(),
            min_disks: self.min_disks,
            report_not_found: self.report_not_found,
            per_disk_limit: self.per_disk_limit,
            ..Default::default()
        }
    }
}

pub async fn list_path_raw(mut rx: B_Receiver<bool>, opts: ListPathRawOptions) -> Result<()> {
    // println!("list_path_raw {},{}", &opts.bucket, &opts.path);
    if opts.disks.is_empty() {
        info!("list_path_raw 0 drives provided");
        return Err(Error::from_string("list_path_raw: 0 drives provided"));
    }

    let mut jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), Error>>> = Vec::new();
    let mut readers = Vec::with_capacity(opts.disks.len());
    let fds = Arc::new(opts.fallback_disks.clone());

    for disk in opts.disks.iter() {
        let opdisk = disk.clone();
        let opts_clone = opts.clone();
        let fds_clone = fds.clone();
        // let (m_tx, m_rx) = mpsc::channel::<MetaCacheEntry>(100);
        // readers.push(m_rx);
        let (rd, mut wr) = tokio::io::duplex(64);
        readers.push(MetacacheReader::new(rd));
        jobs.push(spawn(async move {
            let wakl_opts = WalkDirOptions {
                bucket: opts_clone.bucket.clone(),
                base_dir: opts_clone.path.clone(),
                recursive: opts_clone.recursice,
                report_notfound: opts_clone.report_not_found,
                filter_prefix: opts_clone.filter_prefix.clone(),
                forward_to: opts_clone.forward_to.clone(),
                limit: opts_clone.per_disk_limit,
                ..Default::default()
            };

            let mut need_fallback = false;
            if let Some(disk) = opdisk {
                match disk.walk_dir(wakl_opts, &mut wr).await {
                    Ok(_res) => {}
                    Err(err) => {
                        error!("walk dir err {:?}", &err);
                        need_fallback = true;
                    }
                }
            } else {
                need_fallback = true;
            }

            while need_fallback {
                let disk = match fds_clone.iter().find(|d| d.is_some()) {
                    Some(d) => {
                        if let Some(disk) = d.clone() {
                            disk
                        } else {
                            break;
                        }
                    }
                    None => break,
                };
                match disk
                    .as_ref()
                    .walk_dir(
                        WalkDirOptions {
                            bucket: opts_clone.bucket.clone(),
                            base_dir: opts_clone.path.clone(),
                            recursive: opts_clone.recursice,
                            report_notfound: opts_clone.report_not_found,
                            filter_prefix: opts_clone.filter_prefix.clone(),
                            forward_to: opts_clone.forward_to.clone(),
                            limit: opts_clone.per_disk_limit,
                            ..Default::default()
                        },
                        &mut wr,
                    )
                    .await
                {
                    Ok(_r) => {
                        need_fallback = false;
                    }
                    Err(err) => {
                        error!("walk dir2 err {:?}", &err);
                        break;
                    }
                }
            }

            Ok(())
        }));
    }

    let revjob = spawn(async move {
        let mut errs: Vec<Option<Error>> = vec![None; readers.len()];
        loop {
            let mut current = MetaCacheEntry::default();

            if rx.try_recv().is_ok() {
                return Err(Error::from_string("canceled"));
            }
            let mut top_entries: Vec<Option<MetaCacheEntry>> = vec![None; readers.len()];

            let mut at_eof = 0;
            let mut fnf = 0;
            let mut vnf = 0;
            let mut has_err = 0;
            let mut agree = 0;

            for (i, r) in readers.iter_mut().enumerate() {
                if errs[i].is_some() {
                    has_err += 1;
                    continue;
                }

                let entry = match r.peek().await {
                    Ok(res) => {
                        if let Some(entry) = res {
                            // info!("read entry disk: {}, name: {}", i, entry.name);
                            entry
                        } else {
                            // eof
                            at_eof += 1;

                            continue;
                        }
                    }
                    Err(err) => {
                        if is_err_eof(&err) {
                            at_eof += 1;
                            continue;
                        } else if is_err_file_not_found(&err) {
                            at_eof += 1;
                            fnf += 1;
                            continue;
                        } else if is_err_volume_not_found(&err) {
                            at_eof += 1;
                            fnf += 1;
                            vnf += 1;
                            continue;
                        } else {
                            has_err += 1;
                            errs[i] = Some(err);
                            continue;
                        }
                    }
                };

                // If no current, add it.
                if current.name.is_empty() {
                    top_entries[i] = Some(entry.clone());
                    current = entry;
                    agree += 1;

                    continue;
                }
                // If exact match, we agree.
                if let Ok((_, true)) = current.matches(&entry, true) {
                    top_entries[i] = Some(entry);
                    agree += 1;

                    continue;
                }
                // If only the name matches we didn't agree, but add it for resolution.
                if entry.name == current.name {
                    top_entries[i] = Some(entry);

                    continue;
                }
                // We got different entries
                if entry.name > current.name {
                    continue;
                }
                // We got a new, better current.
                // Clear existing entries.
                top_entries = vec![None; top_entries.len()];
                agree += 1;
                top_entries[i] = Some(entry.clone());
                current = entry;
            }

            if vnf > 0 && vnf >= (readers.len() - opts.min_disks) {
                return Err(Error::new(DiskError::VolumeNotFound));
            }

            if fnf > 0 && fnf >= (readers.len() - opts.min_disks) {
                return Err(Error::new(DiskError::FileNotFound));
            }

            if has_err > 0 && has_err > opts.disks.len() - opts.min_disks {
                if let Some(finished_fn) = opts.finished.as_ref() {
                    finished_fn(&errs).await;
                }
                let mut combined_err = Vec::new();
                errs.iter().zip(opts.disks.iter()).for_each(|(err, disk)| match (err, disk) {
                    (Some(err), Some(disk)) => {
                        combined_err.push(format!("drive {} returned: {}", disk.to_string(), err));
                    }
                    (Some(err), None) => {
                        combined_err.push(err.to_string());
                    }
                    _ => {}
                });

                return Err(Error::from_string(combined_err.join(", ")));
            }

            // Break if all at EOF or error.
            if at_eof + has_err == readers.len() {
                if has_err > 0 {
                    if let Some(finished_fn) = opts.finished.as_ref() {
                        if has_err > 0 {
                            finished_fn(&errs).await;
                        }
                    }
                }

                break;
            }

            if agree == readers.len() {
                for r in readers.iter_mut() {
                    let _ = r.skip(1).await;
                }
                if let Some(agreed_fn) = opts.agreed.as_ref() {
                    agreed_fn(current).await;
                }

                continue;
            }

            for (i, r) in readers.iter_mut().enumerate() {
                if top_entries[i].is_some() {
                    let _ = r.skip(1).await;
                }
            }

            if let Some(partial_fn) = opts.partial.as_ref() {
                partial_fn(MetaCacheEntries(top_entries), &errs).await;
            }
        }
        Ok(())
    });

    jobs.push(revjob);

    let _ = join_all(jobs).await;

    Ok(())
}
