use crate::disk::WalkDirOptions;
use crate::error::{Error, Result};
use crate::peer::is_reserved_or_invalid_bucket;
use crate::store::check_list_objs_args;
use crate::store_api::{ListObjectsInfo, ObjectInfo};
use crate::utils::path::{base_dir_from_prefix, SLASH_SEPARATOR};
use crate::{store::ECStore, store_api::ListObjectsV2Info};
use futures::future::join_all;
use std::collections::HashSet;
use std::io::ErrorKind;

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

    async fn list_path(&self, o: &ListPathOptions) -> Result<ListObjectsInfo> {
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
