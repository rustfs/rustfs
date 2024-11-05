use std::sync::Arc;

use tokio::{
    spawn,
    sync::{
        broadcast::Receiver as B_Receiver,
        mpsc::{self},
        RwLock,
    },
};

use crate::{
    disk::{DiskAPI, DiskStore, MetaCacheEntries, MetaCacheEntry, WalkDirOptions},
    error::{Error, Result},
};

#[derive(Default)]
pub struct ListPathRawOptions {
    pub disks: Vec<Option<DiskStore>>,
    pub fallback_disks: Vec<Option<DiskStore>>,
    pub bucket: String,
    pub path: String,
    pub recursice: bool,
    pub filter_prefix: String,
    pub forward_to: String,
    pub min_disks: usize,
    pub report_not_found: bool,
    pub per_disk_limit: i32,
    pub agreed: Option<Arc<dyn Fn(MetaCacheEntry) + Send + Sync>>,
    pub partial: Option<Arc<dyn Fn(MetaCacheEntries, &[Option<Error>]) + Send + Sync>>,
    pub finished: Option<Arc<dyn Fn(&[Option<Error>]) + Send + Sync>>,
}

impl Clone for ListPathRawOptions {
    fn clone(&self) -> Self {
        Self {
            disks: self.disks.clone(),
            fallback_disks: self.fallback_disks.clone(),
            bucket: self.bucket.clone(),
            path: self.path.clone(),
            recursice: self.recursice.clone(),
            filter_prefix: self.filter_prefix.clone(),
            forward_to: self.forward_to.clone(),
            min_disks: self.min_disks.clone(),
            report_not_found: self.report_not_found.clone(),
            per_disk_limit: self.per_disk_limit.clone(),
            ..Default::default()
        }
    }
}

pub async fn list_path_raw(mut rx: B_Receiver<bool>, opts: ListPathRawOptions) -> Result<()> {
    if opts.disks.is_empty() {
        return Err(Error::from_string("list_path_raw: 0 drives provided"));
    }

    let mut readers = Vec::with_capacity(opts.disks.len());
    let fds = Arc::new(RwLock::new(opts.fallback_disks.clone()));
    for disk in opts.disks.iter() {
        let disk = disk.clone();
        let opts_clone = opts.clone();
        let fds_clone = fds.clone();
        let (m_tx, m_rx) = mpsc::channel::<MetaCacheEntry>(100);
        readers.push(m_rx);
        spawn(async move {
            let mut need_fallback = false;
            if disk.is_none() {
                need_fallback = true;
            } else {
                match disk
                    .as_ref()
                    .unwrap()
                    .walk_dir(WalkDirOptions {
                        bucket: opts_clone.bucket.clone(),
                        base_dir: opts_clone.path.clone(),
                        recursive: opts_clone.recursice.clone(),
                        report_notfound: opts_clone.report_not_found,
                        filter_prefix: opts_clone.filter_prefix.clone(),
                        forward_to: opts_clone.forward_to.clone(),
                        limit: opts_clone.per_disk_limit,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(r) => {
                        for v in r.iter() {
                            let _ = m_tx.send(v.to_owned()).await;
                        }
                    }
                    Err(_) => need_fallback = true,
                }
            }

            while need_fallback {
                let f_disk = loop {
                    let mut fds_w = fds_clone.write().await;
                    if fds_w.is_empty() {
                        break None;
                    }
                    let fd = fds_w.remove(0);
                    if fd.is_some() && fd.as_ref().unwrap().is_online().await {
                        break fd;
                    }
                };
                if f_disk.is_none() {
                    break;
                }
                match disk
                    .as_ref()
                    .unwrap()
                    .walk_dir(WalkDirOptions {
                        bucket: opts_clone.bucket.clone(),
                        base_dir: opts_clone.path.clone(),
                        recursive: opts_clone.recursice,
                        report_notfound: opts_clone.report_not_found,
                        filter_prefix: opts_clone.filter_prefix.clone(),
                        forward_to: opts_clone.forward_to.clone(),
                        limit: opts_clone.per_disk_limit,
                        ..Default::default()
                    })
                    .await
                {
                    Ok(r) => {
                        for v in r.iter() {
                            let _ = m_tx.send(v.to_owned()).await;
                        }
                        need_fallback = false;
                    }
                    Err(_) => break,
                }
            }
        });
    }

    let errs: Vec<Option<Error>> = Vec::with_capacity(readers.len());
    loop {
        let mut current = MetaCacheEntry::default();
        let (mut at_eof, mut has_err, mut agree) = (0, 0, 0);
        if rx.try_recv().is_ok() {
            return Err(Error::from_string("canceled"));
        }
        let mut top_entries: Vec<MetaCacheEntry> = Vec::with_capacity(readers.len());
        // top_entries.clear();

        for (i, r) in readers.iter_mut().enumerate() {
            if errs[i].is_none() {
                has_err += 1;
                continue;
            }
            let entry = match r.recv().await {
                Some(entry) => entry,
                None => {
                    at_eof += 1;
                    continue;
                }
            };
            // If no current, add it.
            if current.name.is_empty() {
                top_entries.insert(i, entry.clone());
                current = entry;
                agree += 1;
                continue;
            }
            // If exact match, we agree.
            if let Ok((_, true)) = current.matches(&entry, true) {
                top_entries.insert(i, entry);
                agree += 1;
                continue;
            }
            // If only the name matches we didn't agree, but add it for resolution.
            if entry.name == current.name {
                top_entries.insert(i, entry);
                continue;
            }
            // We got different entries
            if entry.name > current.name {
                continue;
            }
            // We got a new, better current.
            // Clear existing entries.
            top_entries.clear();
            agree += 1;
            top_entries.insert(i, entry.clone());
            current = entry;
        }

        if has_err > 0 && has_err > opts.disks.len() - opts.min_disks {
            if let Some(finished_fn) = opts.finished.clone() {
                finished_fn(&errs);
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
                if let Some(finished_fn) = opts.finished.clone() {
                    finished_fn(&errs);
                }
                break;
            }
        }

        if agree == readers.len() {
            if let Some(agreed_fn) = opts.agreed.clone() {
                agreed_fn(current);
            }
            continue;
        }

        if let Some(partial_fn) = opts.partial.clone() {
            partial_fn(MetaCacheEntries(top_entries), &errs);
        }
    }

    Ok(())
}
