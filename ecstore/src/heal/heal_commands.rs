pub type HealScanMode = usize;
pub type HealItemType = String;

#[derive(Clone, Copy, Debug, Default)]
pub struct HealOpts {
    pub recursive: bool,
    pub dry_run: bool,
    pub remove: bool,
    pub recreate: bool,
    pub scan_mode: HealScanMode,
    pub update_parity: bool,
    pub no_lock: bool,
    pub pool: Option<usize>,
    pub set: Option<usize>,
}

#[derive(Debug)]
struct HealDriveInfo {
    uuid: String,
    endpoint: String,
    state: String,
}

#[derive(Debug)]
pub struct HealResultItem {
    pub result_index: usize,
    pub heal_item_type: HealItemType,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub detail: String,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub disk_count: usize,
    pub set_count: usize,
    pub before: Vec<HealDriveInfo>,
    pub after: Vec<HealDriveInfo>,
    pub object_size: usize,
}
