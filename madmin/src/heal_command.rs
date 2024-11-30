use std::collections::{HashMap, HashSet};

use ecstore::{
    config::storageclass::{RRS, STANDARD},
    global::GLOBAL_BackgroundHealState,
    heal::{background_heal_ops::get_local_disks_to_heal, heal_ops::BG_HEALING_UUID},
    new_object_layer_fn,
    store_api::{StorageAPI, StorageDisk},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MRFStatus {
    bytes_healed: u64,
    items_healed: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SetStatus {
    pub id: String,
    pub pool_index: i32,
    pub set_index: i32,
    pub heal_status: String,
    pub heal_priority: String,
    pub total_objects: usize,
    pub disks: Vec<StorageDisk>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BgHealState {
    offline_endpoints: Vec<String>,
    scanned_items_count: u64,
    heal_disks: Vec<String>,
    sets: Vec<SetStatus>,
    mrf: HashMap<String, MRFStatus>,
    scparity: HashMap<String, usize>,
}

pub async fn get_local_background_heal_status() -> (BgHealState, bool) {
    let (bg_seq, ok) = GLOBAL_BackgroundHealState
        .read()
        .await
        .get_heal_sequence_by_token(BG_HEALING_UUID)
        .await;
    if !ok {
        return (BgHealState::default(), false);
    }
    let bg_seq = bg_seq.unwrap();
    let mut status = BgHealState {
        scanned_items_count: bg_seq.get_scanned_items_count().await as u64,
        ..Default::default()
    };
    let mut heal_disks_map = HashSet::new();
    for ep in get_local_disks_to_heal().await.iter() {
        heal_disks_map.insert(ep.to_string());
    }

    let Some(store) = new_object_layer_fn() else {
        let healing = GLOBAL_BackgroundHealState.read().await.get_local_healing_disks().await;
        for disk in healing.values() {
            status.heal_disks.push(disk.endpoint.clone());
        }
        return (status, true);
    };

    let si = store.local_storage_info().await;
    let mut indexed = HashMap::new();
    for disk in si.disks.iter() {
        let set_idx = format!("{}-{}", disk.pool_index, disk.set_index);
        // indexed.insert(set_idx, disk);
        indexed.entry(set_idx).or_insert(Vec::new()).push(disk);
    }

    for (id, disks) in indexed {
        let mut ss = SetStatus {
            id,
            set_index: disks[0].set_index,
            pool_index: disks[0].pool_index,
            ..Default::default()
        };
        for disk in disks {
            ss.disks.push(disk.clone());
            if disk.healing {
                ss.heal_status = "healing".to_string();
                ss.heal_priority = "high".to_string();
                status.heal_disks.push(disk.endpoint.clone());
            }
        }
        ss.disks.sort_by(|a, b| {
            if a.pool_index != b.pool_index {
                return a.pool_index.cmp(&b.pool_index);
            }
            if a.set_index != b.set_index {
                return a.set_index.cmp(&b.set_index);
            }
            a.disk_index.cmp(&b.disk_index)
        });
        status.sets.push(ss);
    }
    status.sets.sort_by(|a, b| a.id.cmp(&b.id));
    let backend_info = store.backend_info().await;
    status
        .scparity
        .insert(STANDARD.to_string(), backend_info.standard_sc_parity.unwrap_or_default());
    status
        .scparity
        .insert(RRS.to_string(), backend_info.rr_sc_parity.unwrap_or_default());

    (status, true)
}
