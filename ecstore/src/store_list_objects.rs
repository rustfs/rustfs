use crate::cache_value::metacache_set::{list_path_raw, ListPathRawOptions};
use crate::disk::{DiskInfo, DiskStore, MetaCacheEntries, MetaCacheEntry, MetadataResolutionParams, WalkDirOptions};
use crate::error::{Error, Result};
use crate::peer::is_reserved_or_invalid_bucket;
use crate::set_disk::SetDisks;
use crate::store::check_list_objs_args;
use crate::store_api::{ListObjectsInfo, ObjectInfo};
use crate::utils::path::{base_dir_from_prefix, SLASH_SEPARATOR};
use crate::{store::ECStore, store_api::ListObjectsV2Info};
use futures::future::join_all;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::error;

const MAX_OBJECT_LIST: i32 = 1000;
const MAX_DELETE_LIST: i32 = 1000;
const MAX_UPLOADS_LIST: i32 = 10000;
const MAX_PARTS_LIST: i32 = 10000;

const METACACHE_SHARE_PREFIX: bool = false;

fn max_keys_plus_one(max_keys: i32, add_one: bool) -> i32 {
    let mut max_keys = max_keys;
    if max_keys > MAX_OBJECT_LIST {
        max_keys = MAX_OBJECT_LIST;
    }
    if add_one {
        max_keys += 1;
    }
    max_keys
}

#[derive(Debug, Default, Clone)]
pub struct ListPathOptions {
    pub id: String,

    // Bucket of the listing.
    pub bucket: String,

    // Directory inside the bucket.
    // When unset listPath will set this based on Prefix
    pub base_dir: String,

    // Scan/return only content with prefix.
    pub prefix: String,

    // FilterPrefix will return only results with this prefix when scanning.
    // Should never contain a slash.
    // Prefix should still be set.
    pub filter_prefix: String,

    // Marker to resume listing.
    // The response will be the first entry >= this object name.
    pub marker: String,

    // Limit the number of results.
    pub limit: i32,

    // The number of disks to ask.
    pub ask_disks: String,

    // InclDeleted will keep all entries where latest version is a delete marker.
    pub incl_deleted: bool,

    // Scan recursively.
    // If false only main directory will be scanned.
    // Should always be true if Separator is n SlashSeparator.
    pub recursive: bool,

    // Separator to use.
    pub separator: String,

    // Create indicates that the lister should not attempt to load an existing cache.
    pub create: bool,

    // Include pure directories.
    pub include_directories: bool,

    // Transient is set if the cache is transient due to an error or being a reserved bucket.
    // This means the cache metadata will not be persisted on disk.
    // A transient result will never be returned from the cache so knowing the list id is required.
    pub transient: bool,

    // Versioned is this a ListObjectVersions call.
    pub versioned: bool,

    pub stop_disk_at_limit: bool,
}

impl ListPathOptions {
    pub fn set_filter(&mut self) {
        if METACACHE_SHARE_PREFIX {
            return;
        }
        if self.prefix == self.base_dir {
            return;
        }

        let s = SLASH_SEPARATOR.chars().next().unwrap_or_default();
        self.filter_prefix = self.prefix.trim_start_matches(&self.base_dir).trim_matches(s).to_owned();

        if self.filter_prefix.contains(s) {
            self.filter_prefix = "".to_owned();
        }
    }
}

impl ECStore {
    #[allow(clippy::too_many_arguments)]
    pub async fn inner_list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i32,
        _fetch_owner: bool,
        start_after: &str,
    ) -> Result<ListObjectsV2Info> {
        let marker = {
            if continuation_token.is_empty() {
                start_after
            } else {
                continuation_token
            }
        };

        self.list_objects_generic(bucket, prefix, marker, delimiter, max_keys).await?;
        // FIXME:TODO:
        unimplemented!()
    }

    pub async fn list_objects_generic(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsInfo> {
        let opts = ListPathOptions {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            separator: delimiter.to_owned(),
            limit: max_keys_plus_one(max_keys, !marker.is_empty()),
            marker: marker.to_owned(),
            incl_deleted: false,
            ask_disks: "strict".to_owned(), //TODO: from config
            ..Default::default()
        };

        let merged = self.list_path(&opts).await?;

        // FIXME:TODO:

        todo!()
    }

    pub async fn list_path(&self, o: &ListPathOptions) -> Result<ListObjectsInfo> {
        check_list_objs_args(&o.bucket, &o.prefix, &o.marker)?;
        // if opts.prefix.ends_with(SLASH_SEPARATOR) {
        //     return Err(Error::msg("eof"));
        // }

        let mut o = o.clone();
        if o.marker < o.prefix {
            o.marker = "".to_owned();
        }

        if !o.marker.is_empty() && !o.prefix.is_empty() && !o.marker.starts_with(&o.prefix) {
            return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        if o.limit == 0 {
            return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        if o.prefix.ends_with(SLASH_SEPARATOR) {
            return Err(Error::new(std::io::Error::from(ErrorKind::UnexpectedEof)));
        }

        o.include_directories = o.separator == SLASH_SEPARATOR;

        if (o.separator == SLASH_SEPARATOR || o.separator.is_empty()) && !o.recursive {
            o.recursive = o.separator != SLASH_SEPARATOR;
            o.separator = SLASH_SEPARATOR.to_owned();
        } else {
            o.recursive = true
        }

        // TODO: parseMarker

        if o.base_dir.is_empty() {
            o.base_dir = base_dir_from_prefix(&o.prefix);
        }

        o.transient = o.transient || is_reserved_or_invalid_bucket(&o.bucket, false);
        o.set_filter();
        if o.transient {
            o.create = false;
        }

        // FIXME:TODO:
        todo!()

        // let mut opts = opts.clone();

        // if opts.base_dir.is_empty() {
        //     opts.base_dir = base_dir_from_prefix(&opts.prefix);
        // }

        // let objects = self.list_merged(&opts).await?;

        // let info = ListObjectsInfo {
        //     objects,
        //     ..Default::default()
        // };
        // Ok(info)
    }

    // 读所有
    async fn list_merged(&self, opts: &ListPathOptions) -> Result<Vec<ObjectInfo>> {
        let walk_opts = WalkDirOptions {
            bucket: opts.bucket.clone(),
            base_dir: opts.base_dir.clone(),
            ..Default::default()
        };

        // let (mut wr, mut rd) = tokio::io::duplex(1024);

        let mut futures = Vec::new();

        for sets in self.pools.iter() {
            for set in sets.disk_set.iter() {
                futures.push(set.walk_dir(&walk_opts));
            }
        }

        let results = join_all(futures).await;

        // let mut errs = Vec::new();
        let mut ress = Vec::new();
        let mut uniq = HashSet::new();

        for (disks_ress, _disks_errs) in results {
            for disks_res in disks_ress.iter() {
                if disks_res.is_none() {
                    // TODO handle errs
                    continue;
                }
                let entrys = disks_res.as_ref().unwrap();

                for entry in entrys {
                    // warn!("lst_merged entry---- {}", &entry.name);

                    if !opts.prefix.is_empty() && !entry.name.starts_with(&opts.prefix) {
                        continue;
                    }

                    if !uniq.contains(&entry.name) {
                        uniq.insert(entry.name.clone());
                        // TODO: 过滤

                        if opts.limit > 0 && ress.len() as i32 >= opts.limit {
                            return Ok(ress);
                        }

                        if entry.is_object() {
                            // if !opts.delimiter.is_empty() {
                            //     // entry.name.trim_start_matches(pat)
                            // }

                            let fi = entry.to_fileinfo(&opts.bucket)?;
                            if let Some(f) = fi {
                                ress.push(f.to_object_info(&opts.bucket, &entry.name, false));
                            }
                            continue;
                        }

                        if entry.is_dir() {
                            ress.push(ObjectInfo {
                                is_dir: true,
                                bucket: opts.bucket.clone(),
                                name: entry.name.clone(),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }

        // warn!("list_merged errs {:?}", errs);

        Ok(ress)
    }
}

impl SetDisks {
    pub async fn list_path(&self, rx: Receiver<bool>, opts: ListPathOptions, sender: Sender<MetaCacheEntry>) -> Result<()> {
        let (mut disks, infos, _) = self.get_online_disks_with_healing_and_info(true).await;

        let mut ask_disks = get_list_quorum(&opts.ask_disks, self.set_drive_count as i32);
        if ask_disks == -1 {
            let new_disks = get_quorum_disks(&disks, &infos, (disks.len() + 1) / 2);
            if !new_disks.is_empty() {
                disks = new_disks;
                ask_disks = 1;
            } else {
                ask_disks = get_list_quorum("strict", self.set_drive_count as i32);
            }
        }

        if self.set_drive_count == 4 || ask_disks > disks.len() as i32 {
            ask_disks = disks.len() as i32;
        }

        let listing_quorum = ((ask_disks + 1) / 2) as usize;

        let mut fallback_disks = Vec::new();

        if ask_disks > 0 && disks.len() > ask_disks as usize {
            let mut rand = thread_rng();
            disks.shuffle(&mut rand);

            fallback_disks = disks.split_off(ask_disks as usize);
        }

        let mut resolver = MetadataResolutionParams {
            dir_quorum: listing_quorum,
            obj_quorum: listing_quorum,
            bucket: opts.bucket.clone(),
            ..Default::default()
        };

        if opts.versioned {
            resolver.requested_versions = 1;
        }

        let limit = {
            if opts.limit > 0 && opts.stop_disk_at_limit {
                opts.limit + 4 + (opts.limit / 16)
            } else {
                0
            }
        };

        let tx1 = sender.clone();
        let tx2 = sender.clone();

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks: disks.iter().cloned().map(Some).collect(),
                fallback_disks: fallback_disks.iter().cloned().map(Some).collect(),
                bucket: opts.bucket,
                path: opts.base_dir,
                recursice: opts.recursive,
                filter_prefix: opts.filter_prefix,
                forward_to: opts.marker,
                min_disks: listing_quorum,
                per_disk_limit: limit,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin({
                        let value = tx1.clone();
                        async move {
                            if let Err(err) = value.send(entry).await {
                                error!("list_path send fail {:?}", err);
                            }
                        }
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                    Box::pin({
                        let value = tx2.clone();
                        let resolver = resolver.clone();
                        async move {
                            if let Ok(Some(entry)) = entries.resolve(resolver) {
                                if let Err(err) = value.send(entry).await {
                                    error!("list_path send fail {:?}", err);
                                }
                            }
                        }
                    })
                })),
                finished: None,
                ..Default::default()
            },
        )
        .await
    }
}

fn get_list_quorum(quorum: &str, drive_count: i32) -> i32 {
    match quorum {
        "disk" => 1,
        "reduced" => 2,
        "optimal" => (drive_count + 1) / 2,
        "auto" => -1,
        _ => drive_count, // defaults to 'strict'
    }
}

fn get_quorum_disk_infos(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> (Vec<DiskStore>, Vec<DiskInfo>) {
    let common_mutations = calc_common_counter(infos, read_quorum);
    let mut new_disks = Vec::new();
    let mut new_infos = Vec::new();

    for (i, info) in infos.iter().enumerate() {
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        if mutations >= common_mutations {
            new_disks.push(disks[i].clone()); // Assuming StorageAPI derives Clone
            new_infos.push(infos[i].clone()); // Assuming DiskInfo derives Clone
        }
    }

    (new_disks, new_infos)
}

fn get_quorum_disks(disks: &[DiskStore], infos: &[DiskInfo], read_quorum: usize) -> Vec<DiskStore> {
    let (new_disks, _) = get_quorum_disk_infos(disks, infos, read_quorum);
    new_disks
}

fn calc_common_counter(infos: &[DiskInfo], read_quorum: usize) -> u64 {
    let mut max = 0;
    let mut common_count = 0;
    let mut signature_map: HashMap<u64, usize> = HashMap::new();

    for info in infos {
        if !info.error.is_empty() {
            continue;
        }
        let mutations = info.metrics.total_deletes + info.metrics.total_writes;
        *signature_map.entry(mutations).or_insert(0) += 1;
    }

    for (&ops, &count) in &signature_map {
        if max < count && common_count < ops {
            max = count;
            common_count = ops;
        }
    }

    if max < read_quorum {
        return 0;
    }
    common_count
}

// list_path_raw

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::cache_value::metacache_set::list_path_raw;
    use crate::cache_value::metacache_set::ListPathRawOptions;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::error::is_err_eof;
    use crate::disk::format::FormatV3;
    use crate::disk::new_disk;
    use crate::disk::DiskAPI;
    use crate::disk::DiskOption;
    use crate::disk::MetaCacheEntries;
    use crate::disk::MetaCacheEntry;
    use crate::disk::WalkDirOptions;
    use crate::error::Error;
    use crate::metacache::writer::MetacacheReader;
    use crate::set_disk::SetDisks;
    use crate::store_list_objects::ListPathOptions;
    use futures::future::join_all;
    use lock::namespace_lock::NsLockMap;
    use tokio::sync::broadcast;
    use tokio::sync::mpsc;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_walk_dir() {
        let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
        ep.pool_idx = 0;
        ep.set_idx = 0;
        ep.disk_idx = 0;
        ep.is_local = true;

        let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

        // let disk = match LocalDisk::new(&ep, false).await {
        //     Ok(res) => res,
        //     Err(err) => {
        //         println!("LocalDisk::new err {:?}", err);
        //         return;
        //     }
        // };

        let (rd, mut wr) = tokio::io::duplex(64);

        let job = tokio::spawn(async move {
            let opts = WalkDirOptions {
                bucket: "dada".to_owned(),
                base_dir: "".to_owned(),
                recursive: true,
                ..Default::default()
            };

            println!("walk opts {:?}", opts);
            if let Err(err) = disk.walk_dir(opts, &mut wr).await {
                println!("walk_dir err {:?}", err);
            }
        });

        let job2 = tokio::spawn(async move {
            let mut mrd = MetacacheReader::new(rd);

            loop {
                match mrd.peek().await {
                    Ok(res) => {
                        if let Some(info) = res {
                            println!("info {:?}", info.name)
                        } else {
                            break;
                        }
                    }
                    Err(err) => {
                        if is_err_eof(&err) {
                            break;
                        }

                        println!("get err {:?}", err);
                        break;
                    }
                }
            }
        });
        join_all(vec![job, job2]).await;
    }

    #[tokio::test]
    async fn test_list_path_raw() {
        let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
        ep.pool_idx = 0;
        ep.set_idx = 0;
        ep.disk_idx = 0;
        ep.is_local = true;

        let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");

        // let disk = match LocalDisk::new(&ep, false).await {
        //     Ok(res) => res,
        //     Err(err) => {
        //         println!("LocalDisk::new err {:?}", err);
        //         return;
        //     }
        // };

        let (_, rx) = broadcast::channel(1);
        let bucket = "dada".to_owned();
        let forward_to = "".to_owned();
        let disks = vec![Some(disk)];
        let fallback_disks = Vec::new();

        list_path_raw(
            rx,
            ListPathRawOptions {
                disks,
                fallback_disks,
                bucket,
                path: "".to_owned(),
                recursice: true,
                forward_to,
                min_disks: 1,
                report_not_found: false,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    Box::pin(async move { println!("get entry: {}", entry.name) })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<Error>]| {
                    Box::pin(async move { println!("get entries: {:?}", entries) })
                })),
                finished: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_list_path() {
        let mut ep = Endpoint::try_from("/Users/weisd/project/weisd/s3-rustfs/target/volume/test").unwrap();
        ep.pool_idx = 0;
        ep.set_idx = 0;
        ep.disk_idx = 0;
        ep.is_local = true;

        let disk = new_disk(&ep, &DiskOption::default()).await.expect("init disk fail");
        let _ = disk.set_disk_id(Some(Uuid::new_v4())).await;

        let set = SetDisks {
            lockers: Vec::new(),
            locker_owner: String::new(),
            ns_mutex: Arc::new(RwLock::new(NsLockMap::new(false))),
            disks: RwLock::new(vec![Some(disk)]),
            set_endpoints: Vec::new(),
            set_drive_count: 1,
            default_parity_count: 0,
            set_index: 0,
            pool_index: 0,
            format: FormatV3::new(1, 1),
        };

        let (_, rx) = broadcast::channel(1);
        let bucket = "dada".to_owned();

        let opts = ListPathOptions {
            bucket,
            recursive: true,
            ..Default::default()
        };

        let (sender, mut recv) = mpsc::channel(10);

        set.list_path(rx, opts, sender).await.unwrap();

        while let Some(entry) = recv.recv().await {
            println!("get entry {:?}", entry.name)
        }
    }
}
