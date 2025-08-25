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

use crate::disk::error::DiskError;
use crate::disk::{self, DiskAPI, DiskStore, WalkDirOptions};
use futures::future::join_all;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetacacheReader, is_io_eof};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

pub type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
pub type PartialFn =
    Box<dyn Fn(MetaCacheEntries, &[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
type FinishedFn = Box<dyn Fn(&[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

#[derive(Default)]
pub struct ListPathRawOptions {
    pub disks: Vec<Option<DiskStore>>,
    pub fallback_disks: Vec<Option<DiskStore>>,
    pub bucket: String,
    pub path: String,
    pub recursive: bool,
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
            recursive: self.recursive,
            filter_prefix: self.filter_prefix.clone(),
            forward_to: self.forward_to.clone(),
            min_disks: self.min_disks,
            report_not_found: self.report_not_found,
            per_disk_limit: self.per_disk_limit,
            ..Default::default()
        }
    }
}

pub async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> disk::error::Result<()> {
    if opts.disks.is_empty() {
        return Err(DiskError::other("list_path_raw: 0 drives provided"));
    }

    let mut jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), DiskError>>> = Vec::new();
    let mut readers = Vec::with_capacity(opts.disks.len());
    let fds = Arc::new(opts.fallback_disks.clone());

    let cancel_rx = CancellationToken::new();

    for disk in opts.disks.iter() {
        let opdisk = disk.clone();
        let opts_clone = opts.clone();
        let fds_clone = fds.clone();
        let cancel_rx_clone = cancel_rx.clone();
        let (rd, mut wr) = tokio::io::duplex(64);
        readers.push(MetacacheReader::new(rd));
        jobs.push(spawn(async move {
            let wakl_opts = WalkDirOptions {
                bucket: opts_clone.bucket.clone(),
                base_dir: opts_clone.path.clone(),
                recursive: opts_clone.recursive,
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

            if cancel_rx_clone.is_cancelled() {
                // warn!("list_path_raw: cancel_rx_clone.try_recv().await.is_ok()");
                return Ok(());
            }

            while need_fallback {
                // warn!("list_path_raw: while need_fallback start");
                let disk = match fds_clone.iter().find(|d| d.is_some()) {
                    Some(d) => {
                        if let Some(disk) = d.clone() {
                            disk
                        } else {
                            warn!("list_path_raw: fallback disk is none");
                            break;
                        }
                    }
                    None => {
                        warn!("list_path_raw: fallback disk is none2");
                        break;
                    }
                };
                match disk
                    .as_ref()
                    .walk_dir(
                        WalkDirOptions {
                            bucket: opts_clone.bucket.clone(),
                            base_dir: opts_clone.path.clone(),
                            recursive: opts_clone.recursive,
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

            // warn!("list_path_raw: while need_fallback done");
            Ok(())
        }));
    }

    let revjob = spawn(async move {
        let mut errs: Vec<Option<DiskError>> = Vec::with_capacity(readers.len());
        for _ in 0..readers.len() {
            errs.push(None);
        }

        loop {
            let mut current = MetaCacheEntry::default();

            // warn!(
            //     "list_path_raw: loop start, bucket: {}, path: {}, current: {:?}",
            //     opts.bucket, opts.path, &current.name
            // );

            if rx.is_cancelled() {
                return Err(DiskError::other("canceled"));
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
                            // warn!("list_path_raw: peek eof, disk: {}", i);
                            continue;
                        }
                    }
                    Err(err) => {
                        if err == rustfs_filemeta::Error::Unexpected {
                            at_eof += 1;
                            // warn!("list_path_raw: peek err eof, disk: {}", i);
                            continue;
                        }

                        // warn!("list_path_raw: peek err00, err: {:?}", err);

                        if is_io_eof(&err) {
                            at_eof += 1;
                            // warn!("list_path_raw: peek eof, disk: {}", i);
                            continue;
                        }

                        if err == rustfs_filemeta::Error::FileNotFound {
                            at_eof += 1;
                            fnf += 1;
                            // warn!("list_path_raw: peek fnf, disk: {}", i);
                            continue;
                        } else if err == rustfs_filemeta::Error::VolumeNotFound {
                            at_eof += 1;
                            fnf += 1;
                            vnf += 1;
                            // warn!("list_path_raw: peek vnf, disk: {}", i);
                            continue;
                        } else {
                            has_err += 1;
                            errs[i] = Some(err.into());
                            // warn!("list_path_raw: peek err, disk: {}", i);
                            continue;
                        }
                    }
                };

                // warn!("list_path_raw: loop entry: {:?}, disk: {}", &entry.name, i);

                // If no current, add it.
                if current.name.is_empty() {
                    top_entries[i] = Some(entry.clone());
                    current = entry;
                    agree += 1;

                    continue;
                }
                // If exact match, we agree.
                if let (_, true) = current.matches(Some(&entry), true) {
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

                for item in top_entries.iter_mut().take(i) {
                    *item = None;
                }

                agree = 1;
                top_entries[i] = Some(entry.clone());
                current = entry;
            }

            if vnf > 0 && vnf >= (readers.len() - opts.min_disks) {
                // warn!("list_path_raw: vnf > 0 && vnf >= (readers.len() - opts.min_disks) break");
                return Err(DiskError::VolumeNotFound);
            }

            if fnf > 0 && fnf >= (readers.len() - opts.min_disks) {
                // warn!("list_path_raw: fnf > 0 && fnf >= (readers.len() - opts.min_disks) break");
                return Err(DiskError::FileNotFound);
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

                error!(
                    "list_path_raw: has_err > 0 && has_err > opts.disks.len() - opts.min_disks break, err: {:?}",
                    &combined_err.join(", ")
                );
                return Err(DiskError::other(combined_err.join(", ")));
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

                // error!("list_path_raw: at_eof + has_err == readers.len() break {:?}", &errs);
                break;
            }

            if agree == readers.len() {
                for r in readers.iter_mut() {
                    let _ = r.skip(1).await;
                }

                if let Some(agreed_fn) = opts.agreed.as_ref() {
                    // warn!("list_path_raw: agreed_fn start, current: {:?}", &current.name);
                    agreed_fn(current).await;
                    // warn!("list_path_raw: agreed_fn done");
                }

                continue;
            }

            // warn!("list_path_raw: skip start, current: {:?}", &current.name);

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

    if let Err(err) = revjob.await.map_err(std::io::Error::other)? {
        error!("list_path_raw: revjob err {:?}", err);
        cancel_rx.cancel();

        return Err(err);
    }

    let results = join_all(jobs).await;
    for result in results {
        if let Err(err) = result {
            error!("list_path_raw err {:?}", err);
        }
    }

    // warn!("list_path_raw: done");
    Ok(())
}
