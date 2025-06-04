use crate::{
    api::{DiskAPI, DiskStore, WalkDirOptions, STORAGE_FORMAT_FILE},
    local::LocalDisk,
    os::is_empty_dir,
    path::{self, decode_dir_object, GLOBAL_DIR_SUFFIX_WITH_SLASH, SLASH_SEPARATOR},
};
use futures::future::join_all;
use rustfs_error::{Error, Result};
use rustfs_metacache::{MetaCacheEntries, MetaCacheEntry, MetacacheReader, MetacacheWriter};
use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};
use tokio::{io::AsyncWrite, spawn, sync::broadcast::Receiver as B_Receiver};
use tracing::{error, info, warn};

pub type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
pub type PartialFn = Box<dyn Fn(MetaCacheEntries, &[Option<Error>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
type FinishedFn = Box<dyn Fn(&[Option<Error>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

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

pub async fn list_path_raw(mut rx: B_Receiver<bool>, opts: ListPathRawOptions) -> Result<()> {
    if opts.disks.is_empty() {
        return Err(Error::msg("list_path_raw: 0 drives provided"));
    }

    let mut jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), Error>>> = Vec::new();
    let mut readers = Vec::with_capacity(opts.disks.len());
    let fds = Arc::new(opts.fallback_disks.clone());

    for disk in opts.disks.iter() {
        let opdisk = disk.clone();
        let opts_clone = opts.clone();
        let fds_clone = fds.clone();
        let (rd, mut wr) = tokio::io::duplex(64);
        readers.push(MetacacheReader::new(rd));

        jobs.push(spawn(async move {
            let walk_opts = WalkDirOptions {
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
                match disk.walk_dir(walk_opts, &mut wr).await {
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

            Ok(())
        }));
    }

    let revjob = spawn(async move {
        let mut errs: Vec<Option<Error>> = Vec::with_capacity(readers.len());
        for _ in 0..readers.len() {
            errs.push(None);
        }

        loop {
            let mut current = MetaCacheEntry::default();

            if rx.try_recv().is_ok() {
                return Err(Error::msg("canceled"));
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
                            entry
                        } else {
                            at_eof += 1;
                            continue;
                        }
                    }
                    Err(err) => {
                        if err == Error::FaultyDisk {
                            at_eof += 1;
                            continue;
                        } else if err == Error::FileNotFound {
                            at_eof += 1;
                            fnf += 1;
                            continue;
                        } else if err == Error::VolumeNotFound {
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
                return Err(Error::VolumeNotFound);
            }

            if fnf > 0 && fnf >= (readers.len() - opts.min_disks) {
                return Err(Error::FileNotFound);
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

                return Err(Error::msg(combined_err.join(", ")));
            }

            // Break if all at EOF or error.
            if at_eof + has_err == readers.len() {
                if has_err > 0 {
                    if let Some(finished_fn) = opts.finished.as_ref() {
                        finished_fn(&errs).await;
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

    let results = join_all(jobs).await;
    for result in results {
        if let Err(err) = result {
            error!("list_path_raw err {:?}", err);
        }
    }

    Ok(())
}

impl LocalDisk {
    pub(crate) async fn scan_dir<W: AsyncWrite + Unpin>(
        &self,
        current: &mut String,
        opts: &WalkDirOptions,
        out: &mut MetacacheWriter<W>,
        objs_returned: &mut i32,
    ) -> Result<()> {
        let forward = {
            opts.forward_to.as_ref().filter(|v| v.starts_with(&*current)).map(|v| {
                let forward = v.trim_start_matches(&*current);
                if let Some(idx) = forward.find('/') {
                    forward[..idx].to_owned()
                } else {
                    forward.to_owned()
                }
            })
            // if let Some(forward_to) = &opts.forward_to {

            // } else {
            //     None
            // }
            // if !opts.forward_to.is_empty() && opts.forward_to.starts_with(&*current) {
            //     let forward = opts.forward_to.trim_start_matches(&*current);
            //     if let Some(idx) = forward.find('/') {
            //         &forward[..idx]
            //     } else {
            //         forward
            //     }
            // } else {
            //     ""
            // }
        };

        if opts.limit > 0 && *objs_returned >= opts.limit {
            return Ok(());
        }

        let mut entries = match self.list_dir("", &opts.bucket, current, -1).await {
            Ok(res) => res,
            Err(e) => {
                if e != Error::VolumeNotFound && e != Error::FileNotFound {
                    info!("scan list_dir {}, err {:?}", &current, &e);
                }

                if opts.report_notfound && (e == Error::VolumeNotFound || e == Error::FileNotFound) && current == &opts.base_dir {
                    return Err(Error::FileNotFound);
                }

                return Ok(());
            }
        };

        if entries.is_empty() {
            return Ok(());
        }

        let s = SLASH_SEPARATOR.chars().next().unwrap_or_default();
        *current = current.trim_matches(s).to_owned();

        let bucket = opts.bucket.as_str();

        let mut dir_objes = HashSet::new();

        // 第一层过滤
        for item in entries.iter_mut() {
            let entry = item.clone();
            // check limit
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }
            // check prefix
            if let Some(filter_prefix) = &opts.filter_prefix {
                if !entry.starts_with(filter_prefix) {
                    *item = "".to_owned();
                    continue;
                }
            }

            if let Some(forward) = &forward {
                if &entry < forward {
                    *item = "".to_owned();
                    continue;
                }
            }

            if entry.ends_with(SLASH_SEPARATOR) {
                if entry.ends_with(GLOBAL_DIR_SUFFIX_WITH_SLASH) {
                    let entry = format!("{}{}", entry.as_str().trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH), SLASH_SEPARATOR);
                    dir_objes.insert(entry.clone());
                    *item = entry;
                    continue;
                }

                *item = entry.trim_end_matches(SLASH_SEPARATOR).to_owned();
                continue;
            }

            *item = "".to_owned();

            if entry.ends_with(STORAGE_FORMAT_FILE) {
                //
                let metadata = self
                    .read_metadata(self.get_object_path(bucket, format!("{}/{}", &current, &entry).as_str())?)
                    .await?;

                // 用 strip_suffix 只删除一次
                let entry = entry.strip_suffix(STORAGE_FORMAT_FILE).unwrap_or_default().to_owned();
                let name = entry.trim_end_matches(SLASH_SEPARATOR);
                let name = decode_dir_object(format!("{}/{}", &current, &name).as_str());

                out.write_obj(&MetaCacheEntry {
                    name,
                    metadata,
                    ..Default::default()
                })
                .await?;
                *objs_returned += 1;

                return Ok(());
            }
        }

        entries.sort();

        let mut entries = entries.as_slice();
        if let Some(forward) = &forward {
            for (i, entry) in entries.iter().enumerate() {
                if entry >= forward || forward.starts_with(entry.as_str()) {
                    entries = &entries[i..];
                    break;
                }
            }
        }

        let mut dir_stack: Vec<String> = Vec::with_capacity(5);

        for entry in entries.iter() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            if entry.is_empty() {
                continue;
            }

            let name = path::path_join_buf(&[current, entry]);

            if !dir_stack.is_empty() {
                if let Some(pop) = dir_stack.pop() {
                    if pop < name {
                        //
                        out.write_obj(&MetaCacheEntry {
                            name: pop.clone(),
                            ..Default::default()
                        })
                        .await?;

                        if opts.recursive {
                            let mut opts = opts.clone();
                            opts.filter_prefix = None;
                            if let Err(er) = Box::pin(self.scan_dir(&mut pop.clone(), &opts, out, objs_returned)).await {
                                error!("scan_dir err {:?}", er);
                            }
                        }
                    }
                }
            }

            let mut meta = MetaCacheEntry {
                name,
                ..Default::default()
            };

            let mut is_dir_obj = false;

            if let Some(_dir) = dir_objes.get(entry) {
                is_dir_obj = true;
                meta.name
                    .truncate(meta.name.len() - meta.name.chars().last().unwrap().len_utf8());
                meta.name.push_str(GLOBAL_DIR_SUFFIX_WITH_SLASH);
            }

            let fname = format!("{}/{}", &meta.name, STORAGE_FORMAT_FILE);

            match self.read_metadata(self.get_object_path(&opts.bucket, fname.as_str())?).await {
                Ok(res) => {
                    if is_dir_obj {
                        meta.name = meta.name.trim_end_matches(GLOBAL_DIR_SUFFIX_WITH_SLASH).to_owned();
                        meta.name.push_str(SLASH_SEPARATOR);
                    }

                    meta.metadata = res;

                    out.write_obj(&meta).await?;
                    *objs_returned += 1;
                }
                Err(err) => {
                    if err == Error::FileNotFound || err == Error::IsNotRegular {
                        // NOT an object, append to stack (with slash)
                        // If dirObject, but no metadata (which is unexpected) we skip it.
                        if !is_dir_obj && !is_empty_dir(self.get_object_path(&opts.bucket, &meta.name)?).await {
                            meta.name.push_str(SLASH_SEPARATOR);
                            dir_stack.push(meta.name);
                        }
                    }

                    continue;
                }
            };
        }

        while let Some(dir) = dir_stack.pop() {
            if opts.limit > 0 && *objs_returned >= opts.limit {
                return Ok(());
            }

            out.write_obj(&MetaCacheEntry {
                name: dir.clone(),
                ..Default::default()
            })
            .await?;
            *objs_returned += 1;

            if opts.recursive {
                let mut dir = dir;
                let mut opts = opts.clone();
                opts.filter_prefix = None;
                if let Err(er) = Box::pin(self.scan_dir(&mut dir, &opts, out, objs_returned)).await {
                    warn!("scan_dir err {:?}", &er);
                }
            }
        }

        Ok(())
    }
}
