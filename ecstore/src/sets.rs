use anyhow::Error;
use uuid::Uuid;

use crate::{endpoint::PoolEndpoints, format::FormatV3};

#[derive(Debug)]
pub struct Sets {
    pub id: Uuid,
    // pub sets: Vec<Objects>,
    pub disk_indexs: Vec<Vec<usize>>, // [set_count_idx][set_drive_count_idx] = disk_idx
    pub pool_idx: usize,
    pub endpoints: PoolEndpoints,
    pub format: FormatV3,
    pub partiy_count: usize,
    pub set_count: usize,
    pub set_drive_count: usize,
}

impl Sets {
    pub fn new(
        endpoints: &PoolEndpoints,
        fm: &FormatV3,
        pool_idx: usize,
        partiy_count: usize,
    ) -> Result<Self, Error> {
        let set_count = fm.erasure.sets.len();
        let set_drive_count = fm.erasure.sets[0].len();

        let mut disk_indexs = Vec::with_capacity(set_count);

        for i in 0..set_count {
            let mut set_indexs = Vec::with_capacity(set_drive_count);
            for j in 0..set_drive_count {
                let idx = i * set_drive_count + j;
                set_indexs.push(idx);
            }

            disk_indexs.push(set_indexs);
        }

        let sets = Self {
            id: fm.id.clone(),
            // sets: todo!(),
            disk_indexs,
            pool_idx,
            endpoints: endpoints.clone(),
            format: fm.clone(),
            partiy_count,
            set_count,
            set_drive_count,
        };

        Ok(sets)
    }
    pub fn get_disks(&self, set_idx: usize) -> Vec<usize> {
        self.disk_indexs[set_idx].clone()
    }
}

// #[derive(Debug)]
// pub struct Objects {
//     pub endpoints: Vec<Endpoint>,
//     pub disks: Vec<usize>,
//     pub set_index: usize,
//     pub pool_index: usize,
//     pub set_drive_count: usize,
//     pub default_parity_count: usize,
// }
