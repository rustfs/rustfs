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

use crate::disk::disk_store::get_drive_walkdir_stall_timeout;
use crate::disk::error::DiskError;
use crate::disk::{self, DiskAPI, DiskStore, WalkDirOptions};
use futures::future::join_all;
use metrics::counter;
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetacacheReader, is_io_eof};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncRead;
use tokio::spawn;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
pub type PartialFn =
    Box<dyn Fn(MetaCacheEntries, &[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
type FinishedFn = Box<dyn Fn(&[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

#[derive(Debug)]
enum PeekOutcome {
    Ready(Option<MetaCacheEntry>),
    Error(rustfs_filemeta::Error),
    TimedOut,
}

async fn peek_with_timeout<R: AsyncRead + Unpin>(reader: &mut MetacacheReader<R>, timeout_duration: Duration) -> PeekOutcome {
    match timeout(timeout_duration, reader.peek()).await {
        Ok(Ok(entry)) => PeekOutcome::Ready(entry),
        Ok(Err(err)) => PeekOutcome::Error(err),
        Err(_) => PeekOutcome::TimedOut,
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) enum TestReaderBehavior {
    Eof,
    Stall,
    PartialThenTimeout(Vec<MetaCacheEntry>),
}

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
    #[cfg(test)]
    pub(crate) test_reader_behaviors: Vec<TestReaderBehavior>,
    #[cfg(test)]
    pub(crate) peek_timeout: Option<Duration>,
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
            #[cfg(test)]
            test_reader_behaviors: self.test_reader_behaviors.clone(),
            #[cfg(test)]
            peek_timeout: self.peek_timeout,
            ..Default::default()
        }
    }
}

pub async fn list_path_raw(rx: CancellationToken, opts: ListPathRawOptions) -> disk::error::Result<()> {
    if opts.disks.is_empty() {
        return Err(DiskError::ErasureReadQuorum);
    }

    let mut jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), DiskError>>> = Vec::new();
    let mut readers = Vec::with_capacity(opts.disks.len());
    let fds = opts.fallback_disks.iter().flatten().cloned().collect::<Vec<_>>();
    let producer_errs = Arc::new(Mutex::new(vec![None; opts.disks.len()]));

    let cancel_rx = CancellationToken::new();

    for (disk_idx, disk) in opts.disks.iter().enumerate() {
        let opdisk = disk.clone();
        let opts_clone = opts.clone();
        let mut fds_clone = fds.clone();
        let cancel_rx_clone = cancel_rx.clone();
        let producer_errs_clone = producer_errs.clone();
        let (rd, wr) = tokio::io::duplex(64);
        readers.push(MetacacheReader::new(rd));
        jobs.push(spawn(async move {
            #[cfg(test)]
            if let Some(behavior) = opts_clone.test_reader_behaviors.get(disk_idx).cloned() {
                match behavior {
                    TestReaderBehavior::Eof => return Ok(()),
                    TestReaderBehavior::Stall => {
                        let _held_writer = wr;
                        cancel_rx_clone.cancelled().await;
                        return Ok(());
                    }
                    TestReaderBehavior::PartialThenTimeout(entries) => {
                        let mut wr = wr;
                        let mut out = rustfs_filemeta::MetacacheWriter::new(&mut wr);
                        let err = DiskError::Timeout;
                        producer_errs_clone.lock().expect("producer error mutex poisoned")[disk_idx] = Some(err.clone());
                        let _ = out.write(&entries).await;
                        drop(out);
                        return Err(err);
                    }
                }
            }

            let mut wr = wr;
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
            let mut last_err = None;
            if let Some(disk) = opdisk {
                match disk.walk_dir(wakl_opts, &mut wr).await {
                    Ok(_res) => {}
                    Err(err) => {
                        info!("walk dir err {:?}", &err);
                        last_err = Some(err);
                        need_fallback = true;
                    }
                }
            } else {
                last_err = Some(DiskError::DiskNotFound);
                need_fallback = true;
            }

            if cancel_rx_clone.is_cancelled() {
                // warn!("list_path_raw: cancel_rx_clone.is_cancelled()");
                return Ok(());
            }

            while need_fallback {
                let disk_op = {
                    if fds_clone.is_empty() {
                        None
                    } else {
                        let disk = fds_clone.remove(0);
                        if disk.is_online().await { Some(disk.clone()) } else { None }
                    }
                };

                let Some(disk) = disk_op else {
                    warn!("list_path_raw: fallback disk is none");
                    let err = last_err.unwrap_or(DiskError::DiskNotFound);
                    producer_errs_clone.lock().expect("producer error mutex poisoned")[disk_idx] = Some(err.clone());
                    return Err(err);
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
                        last_err = None;
                    }
                    Err(err) => {
                        error!("walk dir2 err {:?}", &err);
                        last_err = Some(err);
                    }
                }
            }

            // warn!("list_path_raw: while need_fallback done");
            Ok(())
        }));
    }

    let revjob = spawn(async move {
        #[cfg(test)]
        let peek_timeout = opts.peek_timeout.unwrap_or_else(get_drive_walkdir_stall_timeout);
        #[cfg(not(test))]
        let peek_timeout = get_drive_walkdir_stall_timeout();
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

                let entry = match peek_with_timeout(r, peek_timeout).await {
                    PeekOutcome::Ready(res) => {
                        if let Some(entry) = res {
                            // info!("read entry disk: {}, name: {}", i, entry.name);
                            entry
                        } else {
                            if let Some(err) = producer_errs.lock().expect("producer error mutex poisoned")[i].clone() {
                                has_err += 1;
                                errs[i] = Some(err);
                                continue;
                            }
                            // eof
                            at_eof += 1;
                            // warn!("list_path_raw: peek eof, disk: {}", i);
                            continue;
                        }
                    }
                    PeekOutcome::Error(err) => {
                        if let Some(err) = producer_errs.lock().expect("producer error mutex poisoned")[i].clone() {
                            has_err += 1;
                            errs[i] = Some(err);
                            continue;
                        }

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
                    PeekOutcome::TimedOut => {
                        has_err += 1;
                        errs[i] = Some(DiskError::Timeout);
                        let endpoint = opts
                            .disks
                            .get(i)
                            .and_then(|disk| disk.as_ref().map(|disk| disk.endpoint().to_string()))
                            .unwrap_or_else(|| "missing".to_string());
                        counter!(
                            "rustfs_list_path_raw_stall_total",
                            "drive" => endpoint.clone()
                        )
                        .increment(1);
                        warn!(
                            drive = %endpoint,
                            bucket = %opts.bucket,
                            path = %opts.path,
                            timeout_ms = peek_timeout.as_millis(),
                            "list_path_raw reader peek timed out; excluding drive from current merge"
                        );
                        let (detached_rd, write_half) = tokio::io::duplex(1);
                        drop(write_half);
                        *r = MetacacheReader::new(detached_rd);
                        continue;
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
                if errs.iter().flatten().any(|err| *err == DiskError::Timeout) {
                    return Err(DiskError::Timeout);
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
                if has_err > 0
                    && let Some(finished_fn) = opts.finished.as_ref()
                {
                    finished_fn(&errs).await;
                }
                if errs.iter().flatten().any(|err| *err == DiskError::Timeout) {
                    return Err(DiskError::Timeout);
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
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!("list_path_raw producer err {:?}", err);
            }
            Err(err) => {
                error!("list_path_raw err {:?}", err);
                return Err(DiskError::other(err.to_string()));
            }
        }
    }

    // warn!("list_path_raw: done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_filemeta::MetacacheWriter;

    #[tokio::test]
    async fn list_path_raw_empty_disks_returns_read_quorum() {
        let err = list_path_raw(CancellationToken::new(), ListPathRawOptions::default())
            .await
            .expect_err("empty drive list should fail");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[tokio::test]
    async fn list_path_raw_returns_timeout_when_reader_stalls_before_completion() {
        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None, None],
                min_disks: 1,
                test_reader_behaviors: vec![TestReaderBehavior::Stall, TestReaderBehavior::Eof],
                peek_timeout: Some(Duration::from_millis(20)),
                ..Default::default()
            },
        )
        .await
        .expect_err("stalled reader should make listing fail explicitly");

        assert_eq!(err, DiskError::Timeout);
    }

    #[tokio::test]
    async fn list_path_raw_returns_timeout_when_producer_fails_after_partial_entry() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();

        let err = list_path_raw(
            CancellationToken::new(),
            ListPathRawOptions {
                disks: vec![None],
                min_disks: 1,
                test_reader_behaviors: vec![TestReaderBehavior::PartialThenTimeout(vec![MetaCacheEntry {
                    name: "bucket/object".to_string(),
                    metadata: vec![1, 2, 3],
                    cached: None,
                    reusable: false,
                }])],
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let seen = seen_clone.clone();
                    Box::pin(async move {
                        seen.lock().expect("seen mutex poisoned").push(entry.name);
                    })
                })),
                ..Default::default()
            },
        )
        .await
        .expect_err("producer timeout after partial output must fail the listing");

        assert_eq!(err, DiskError::Timeout);
        assert_eq!(seen.lock().expect("seen mutex poisoned").as_slice(), &["bucket/object".to_string()]);
    }

    #[tokio::test]
    async fn peek_with_timeout_times_out_on_silent_reader() {
        let (_writer, reader) = tokio::io::duplex(64);
        let mut reader = MetacacheReader::new(reader);

        let outcome = peek_with_timeout(&mut reader, Duration::from_millis(20)).await;
        assert!(matches!(outcome, PeekOutcome::TimedOut));
    }

    #[tokio::test]
    async fn peek_with_timeout_reads_entry_before_deadline() {
        let (reader, writer) = tokio::io::duplex(256);
        let mut metacache_reader = MetacacheReader::new(reader);

        tokio::spawn(async move {
            let mut writer = MetacacheWriter::new(writer);
            let entry = MetaCacheEntry {
                name: "bucket/object".to_string(),
                metadata: vec![1, 2, 3],
                cached: None,
                reusable: false,
            };
            writer.write(&[entry]).await.expect("entry should be written");
            writer.close().await.expect("writer should close");
        });

        let outcome = peek_with_timeout(&mut metacache_reader, Duration::from_secs(1)).await;
        match outcome {
            PeekOutcome::Ready(Some(entry)) => assert_eq!(entry.name, "bucket/object"),
            other => panic!("expected ready entry, got {other:?}"),
        }
    }
}
