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

//! One erasure deployment under a deterministic fault schedule.
//!
//! Disk bytes are produced by `Erasure::encode_data` and checked by
//! `Erasure::decode_data`. Commit and read decisions call
//! `reduce_write_quorum_errs` / `reduce_read_quorum_errs`. Exclusive and
//! shared lock gates use the production quorum sizes. A drive applies a
//! mutation and acknowledges it in the same scheduler event.

use crate::bitrot::{frame_shard, unframe_shard};
use crate::rng::Rng;
use crate::topology::{DST_BUCKET, DrivePlan, FORMAT_ID, SetPlan, Topology};
use rustfs_ecstore::dst_seam::{SimCodec, SimDiskFault, read_quorum_met, write_quorum_met};
use rustfs_filemeta::{BLOCK_SIZE_V2, FileInfo};
use rustfs_utils::sip_hash;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct FaultRates {
    pub drop_numer: u32,
    pub dup_numer: u32,
    pub io_fail_numer: u32,
    pub denom: u32,
    pub max_delay_us: u64,
    pub clock_jump_numer: u32,
    pub crash_numer: u32,
    pub fault_denom: u32,
    pub max_clock_jump_us: u64,
}

impl Default for FaultRates {
    fn default() -> Self {
        Self {
            drop_numer: 3,
            dup_numer: 2,
            io_fail_numer: 2,
            denom: 100,
            max_delay_us: 120_000,
            clock_jump_numer: 8,
            crash_numer: 10,
            fault_denom: 100,
            max_clock_jump_us: 100_000,
        }
    }
}

impl FaultRates {
    pub fn none() -> Self {
        Self {
            drop_numer: 0,
            dup_numer: 0,
            io_fail_numer: 0,
            denom: 100,
            max_delay_us: 0,
            clock_jump_numer: 0,
            crash_numer: 0,
            fault_denom: 100,
            max_clock_jump_us: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorldConfig {
    pub rates: FaultRates,
    pub op_timeout_us: u64,
    pub max_objects: usize,
    pub max_payload: usize,
    pub trace: bool,
}

impl Default for WorldConfig {
    fn default() -> Self {
        Self {
            rates: FaultRates::default(),
            op_timeout_us: 100_000,
            max_objects: 8,
            max_payload: 4096,
            trace: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub step: u32,
    pub invariant: String,
    pub detail: String,
}

#[derive(Clone, Debug)]
struct Replica {
    generation: u64,
    payload_len: usize,
    shard_index: usize,
    framed: Vec<u8>,
}

#[derive(Clone, Debug)]
struct Committed {
    generation: u64,
    payload: Vec<u8>,
}

struct Drive {
    set_index: usize,
    disk_index: usize,
    node_index: usize,
    endpoint: String,
    faulted: bool,
    epoch: u64,
    objects: BTreeMap<String, Replica>,
    seen: BTreeMap<String, u64>,
}

struct Node {
    up: bool,
    isolated: bool,
}

struct Undo {
    op: u64,
    drive: usize,
    key: String,
    previous: Option<Replica>,
    previous_seen: Option<u64>,
}

enum Event {
    Store {
        op: u64,
        drive: usize,
        disk_index: usize,
        key: String,
        generation: u64,
        payload_len: usize,
        shard_index: usize,
        framed: Vec<u8>,
        epoch: u64,
        fail: bool,
    },
    Delete {
        op: u64,
        drive: usize,
        disk_index: usize,
        key: String,
        generation: u64,
        epoch: u64,
        fail: bool,
    },
    Read {
        op: u64,
        drive: usize,
        disk_index: usize,
        key: String,
        epoch: u64,
        fail: bool,
    },
    CrashNode {
        node: usize,
    },
    ClockJump {
        delta_us: u64,
    },
}

enum Hear {
    Mutate(Vec<Option<Result<(), SimDiskFault>>>),
    Read(Vec<Option<Result<Option<Replica>, SimDiskFault>>>),
}

struct Inflight {
    id: u64,
    heard: Hear,
}

pub struct World {
    topology: Topology,
    codecs: Vec<SimCodec>,
    nodes: Vec<Node>,
    drives: Vec<Drive>,
    oracle: BTreeMap<String, Committed>,
    next_generation: BTreeMap<String, u64>,
    rng: Rng,
    config: WorldConfig,
    now: u64,
    seq: u64,
    next_op: u64,
    events: BTreeMap<(u64, u64), Event>,
    inflight: Option<Inflight>,
    closed: BTreeSet<u64>,
    undos: Vec<Undo>,
    sticky: Option<String>,
    last_success: bool,
    step_index: u32,
    pub trace: Vec<String>,
}

impl World {
    pub fn new(topology: Topology, seed: u64, config: WorldConfig) -> Result<Self, String> {
        let mut codecs = Vec::with_capacity(topology.sets.len());
        for set in &topology.sets {
            let codec = SimCodec::try_new(set.data_blocks, set.parity_blocks, BLOCK_SIZE_V2)
                .map_err(|error| format!("erasure codec {}: {error}", topology.name))?;
            codecs.push(codec);
        }
        let nodes = topology
            .nodes
            .iter()
            .map(|_| Node {
                up: true,
                isolated: false,
            })
            .collect();
        let mut drives = Vec::new();
        for (set_index, set) in topology.sets.iter().enumerate() {
            for (disk_index, plan) in set.drives.iter().enumerate() {
                drives.push(drive_from_plan(set_index, disk_index, plan));
            }
        }
        Ok(Self {
            topology,
            codecs,
            nodes,
            drives,
            oracle: BTreeMap::new(),
            next_generation: BTreeMap::new(),
            rng: Rng::from_seed(seed),
            config,
            now: 0,
            seq: 0,
            next_op: 1,
            events: BTreeMap::new(),
            inflight: None,
            closed: BTreeSet::new(),
            undos: Vec::new(),
            sticky: None,
            last_success: false,
            step_index: 0,
            trace: Vec::new(),
        })
    }

    pub fn step(&mut self) -> Result<(), Violation> {
        self.step_index += 1;
        let action = self.rng.gen_range(100);
        let result = match action {
            0..=39 => self.step_put(),
            40..=59 => self.step_get(),
            60..=69 => self.step_list(),
            70..=77 => self.step_delete(),
            78..=83 => self.step_crash(),
            84..=88 => self.step_restart(),
            89..=92 => self.step_isolate(),
            93..=95 => self.step_clear_isolation(),
            96..=97 => self.step_corrupt(),
            98 => self.step_wipe(),
            _ => self.step_heal(),
        };
        if let Err(detail) = result {
            return Err(self.violation("step", detail));
        }
        self.check()
    }

    pub fn crash_every_node(&mut self) {
        for node in 0..self.nodes.len() {
            self.nodes[node].up = false;
            self.bump_epoch_for_node(node);
        }
    }

    pub fn put_quiet(&mut self, object: &str, payload: &[u8]) -> Result<(), String> {
        let saved = self.config.rates.clone();
        self.config.rates = FaultRates::none();
        let result = self.put(object, payload);
        self.config.rates = saved;
        result
    }

    pub fn check(&self) -> Result<(), Violation> {
        if let Some(detail) = &self.sticky {
            return Err(self.violation("quorum_reducer", detail.clone()));
        }
        let mut readable = BTreeSet::new();
        for object in self.media_keys() {
            match self.inspect(&object) {
                Ok(bytes) => {
                    match self.oracle.get(&object) {
                        Some(committed) if committed.payload == bytes => {}
                        Some(_) => {
                            return Err(self.violation(
                                "silent_corruption",
                                format!("{object} reconstructed bytes differ from the committed payload"),
                            ));
                        }
                        None => {
                            return Err(
                                self.violation("visible_without_commit", format!("{object} is readable but was never committed"))
                            );
                        }
                    }
                    readable.insert(object);
                }
                Err(error) => {
                    if let Some(committed) = self.oracle.get(&object).cloned() {
                        let read_quorum = self.set_for(&object).read_quorum;
                        let valid = self.valid_replicas(&object, committed.generation, committed.payload.len());
                        if valid >= read_quorum {
                            return Err(self.violation(
                                "missing_despite_quorum",
                                format!(
                                    "{object} has {valid} valid replicas, read quorum {read_quorum}, inspect failed: {error}"
                                ),
                            ));
                        }
                    }
                }
            }
        }
        let missing: Vec<(String, u64, usize)> = self
            .oracle
            .iter()
            .filter(|(object, _)| !readable.contains(*object))
            .map(|(object, committed)| (object.clone(), committed.generation, committed.payload.len()))
            .collect();
        for (object, generation, payload_len) in missing {
            let read_quorum = self.set_for(&object).read_quorum;
            let valid = self.valid_replicas(&object, generation, payload_len);
            if valid >= read_quorum {
                return Err(self.violation(
                    "missing_despite_quorum",
                    format!("{object} has {valid} valid replicas but is not readable"),
                ));
            }
        }
        Ok(())
    }

    fn step_put(&mut self) -> Result<(), String> {
        let object = self.random_object();
        let payload = self.random_payload();
        self.note(format!("put {object} bytes={}", payload.len()));
        self.put(&object, &payload).map(|_| ())
    }

    fn step_get(&mut self) -> Result<(), String> {
        let Some(object) = self.random_known_object() else {
            self.note("get skipped".to_string());
            return Ok(());
        };
        self.note(format!("get {object}"));
        match self.get(&object)? {
            Some(bytes) => self.expect_oracle(&object, &bytes),
            None => Ok(()),
        }
    }

    fn step_list(&mut self) -> Result<(), String> {
        self.note("list".to_string());
        for object in self.list()? {
            if !self.oracle.contains_key(&object) {
                return Err(format!("list returned uncommitted {object}"));
            }
        }
        Ok(())
    }

    fn step_delete(&mut self) -> Result<(), String> {
        let Some(object) = self.random_known_object() else {
            self.note("delete skipped".to_string());
            return Ok(());
        };
        self.note(format!("delete {object}"));
        self.delete(&object)
    }

    fn step_crash(&mut self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Ok(());
        }
        let node = self.rng.gen_range(self.nodes.len() as u64) as usize;
        self.nodes[node].up = false;
        self.bump_epoch_for_node(node);
        self.note(format!("crash node {node}"));
        Ok(())
    }

    fn step_restart(&mut self) -> Result<(), String> {
        let down: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.up)
            .map(|(index, _)| index)
            .collect();
        if down.is_empty() {
            self.note("restart skipped".to_string());
            return Ok(());
        }
        let node = down[self.rng.gen_range(down.len() as u64) as usize];
        self.nodes[node].up = true;
        self.note(format!("restart node {node}"));
        Ok(())
    }

    fn step_isolate(&mut self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Ok(());
        }
        let node = self.rng.gen_range(self.nodes.len() as u64) as usize;
        self.nodes[node].isolated = true;
        self.note(format!("isolate node {node}"));
        Ok(())
    }

    fn step_clear_isolation(&mut self) -> Result<(), String> {
        for node in &mut self.nodes {
            node.isolated = false;
        }
        self.note("clear isolation".to_string());
        Ok(())
    }

    fn step_corrupt(&mut self) -> Result<(), String> {
        let candidates: Vec<usize> = self
            .drives
            .iter()
            .enumerate()
            .filter(|(_, drive)| !drive.objects.is_empty())
            .map(|(index, _)| index)
            .collect();
        if candidates.is_empty() {
            self.note("corrupt skipped".to_string());
            return Ok(());
        }
        let drive = candidates[self.rng.gen_range(candidates.len() as u64) as usize];
        let key = self.drives[drive].objects.keys().next().cloned();
        if let Some(key) = key
            && let Some(replica) = self.drives[drive].objects.get_mut(&key)
            && !replica.framed.is_empty()
        {
            let index = self.rng.gen_range(replica.framed.len() as u64) as usize;
            replica.framed[index] ^= 0xff;
            self.note(format!("corrupt drive {drive} {key}"));
        }
        Ok(())
    }

    fn step_wipe(&mut self) -> Result<(), String> {
        if self.drives.is_empty() {
            return Ok(());
        }
        let drive = self.rng.gen_range(self.drives.len() as u64) as usize;
        self.drives[drive].objects.clear();
        self.drives[drive].seen.clear();
        self.note(format!("wipe {}", self.drives[drive].endpoint));
        Ok(())
    }

    fn step_heal(&mut self) -> Result<(), String> {
        let Some(object) = self.random_known_object() else {
            self.note("heal skipped".to_string());
            return Ok(());
        };
        self.heal(&object)?;
        self.note(format!("heal {object}"));
        Ok(())
    }

    fn put(&mut self, object: &str, payload: &[u8]) -> Result<(), String> {
        let set_index = self.set_index(object);
        if !self.exclusive_lock_available(set_index) {
            self.note(format!("put {object} rejected by exclusive lock quorum"));
            return Ok(());
        }
        let drive_count = self.topology.sets[set_index].drives.len();
        let write_quorum = self.topology.sets[set_index].write_quorum;
        let distribution = distribution_for(object, &self.topology.sets[set_index]);
        let generation = self.next_generation.get(object).copied().unwrap_or(0).saturating_add(1);
        self.next_generation.insert(object.to_string(), generation);
        let shards = self.codecs[set_index]
            .encode(payload)
            .map_err(|error| format!("encode {object}: {error}"))?;
        if shards.len() != drive_count {
            return Err(format!("encode produced {} shards, expected {drive_count}", shards.len()));
        }
        let op = self.open_mutate(drive_count);
        let deadline = self.now.saturating_add(self.config.op_timeout_us);
        self.schedule_op_faults(set_index);
        for disk_index in 0..drive_count {
            let shard_index = shard_slot(&distribution, disk_index, shards.len())?;
            let framed = frame_shard(&shards[shard_index]);
            let drive = self.flat_drive(set_index, disk_index);
            self.send_store(op, drive, disk_index, object, generation, payload.len(), shard_index, framed);
        }
        self.finish_mutate(op, deadline, write_quorum, true)?;
        if self.mutate_succeeded(op) {
            self.oracle.insert(
                object.to_string(),
                Committed {
                    generation,
                    payload: payload.to_vec(),
                },
            );
        }
        Ok(())
    }

    fn delete(&mut self, object: &str) -> Result<(), String> {
        let set_index = self.set_index(object);
        if !self.exclusive_lock_available(set_index) {
            self.note(format!("delete {object} rejected by exclusive lock quorum"));
            return Ok(());
        }
        let drive_count = self.topology.sets[set_index].drives.len();
        let write_quorum = self.topology.sets[set_index].write_quorum;
        let generation = self.oracle.get(object).map(|committed| committed.generation).unwrap_or(0);
        let op = self.open_mutate(drive_count);
        let deadline = self.now.saturating_add(self.config.op_timeout_us);
        self.schedule_op_faults(set_index);
        for disk_index in 0..drive_count {
            let drive = self.flat_drive(set_index, disk_index);
            self.send_delete(op, drive, disk_index, object, generation);
        }
        self.finish_mutate(op, deadline, write_quorum, true)?;
        if self.mutate_succeeded(op) {
            self.oracle.remove(object);
        }
        Ok(())
    }

    fn get(&mut self, object: &str) -> Result<Option<Vec<u8>>, String> {
        let set_index = self.set_index(object);
        if !self.shared_lock_available(set_index) {
            self.note(format!("get {object} rejected by shared lock quorum"));
            return Ok(None);
        }
        let drive_count = self.topology.sets[set_index].drives.len();
        let op = self.open_read(drive_count);
        let deadline = self.now.saturating_add(self.config.op_timeout_us);
        self.schedule_op_faults(set_index);
        for disk_index in 0..drive_count {
            let drive = self.flat_drive(set_index, disk_index);
            self.send_read(op, drive, disk_index, object);
        }
        self.pump(deadline, op);
        let heard = self.take_read(op);
        self.close_op(op, true);
        self.assemble_read(object, &heard)
    }

    fn list(&mut self) -> Result<Vec<String>, String> {
        let mut listed = BTreeSet::new();
        let objects = self.media_keys();
        for object in objects {
            if let Some(bytes) = self.get(&object)? {
                self.expect_oracle(&object, &bytes)?;
                listed.insert(object);
            }
        }
        Ok(listed.into_iter().collect())
    }

    fn heal(&mut self, object: &str) -> Result<(), String> {
        let set_index = self.set_index(object);
        if !self.exclusive_lock_available(set_index) {
            return Ok(());
        }
        let Some(committed) = self.oracle.get(object).cloned() else {
            return Ok(());
        };
        if self.inspect(object).is_err() {
            return Ok(());
        }
        let drive_count = self.topology.sets[set_index].drives.len();
        let distribution = distribution_for(object, &self.topology.sets[set_index]);
        let shards = self.codecs[set_index]
            .encode(&committed.payload)
            .map_err(|error| format!("heal encode {object}: {error}"))?;
        for disk_index in 0..drive_count {
            let drive = self.flat_drive(set_index, disk_index);
            if !self.drive_reachable(drive) {
                continue;
            }
            if self.valid_replica_on(drive, object, committed.generation, committed.payload.len()) {
                continue;
            }
            let shard_index = shard_slot(&distribution, disk_index, shards.len())?;
            let framed = frame_shard(&shards[shard_index]);
            self.drives[drive].objects.insert(
                object.to_string(),
                Replica {
                    generation: committed.generation,
                    payload_len: committed.payload.len(),
                    shard_index,
                    framed,
                },
            );
            self.drives[drive].seen.insert(object.to_string(), committed.generation);
        }
        Ok(())
    }

    fn expect_oracle(&self, object: &str, bytes: &[u8]) -> Result<(), String> {
        match self.oracle.get(object) {
            Some(committed) if committed.payload == bytes => Ok(()),
            Some(_) => Err(format!("client read of {object} did not match the committed payload")),
            None => Err(format!("client read of {object} returned bytes that were never committed")),
        }
    }

    fn assemble_read(
        &mut self,
        object: &str,
        heard: &[Option<Result<Option<Replica>, SimDiskFault>>],
    ) -> Result<Option<Vec<u8>>, String> {
        let set = self.set_for(object).clone();
        let mut generations = BTreeSet::new();
        for slot in heard {
            if let Some(Ok(Some(replica))) = slot {
                generations.insert((replica.generation, replica.payload_len));
            }
        }
        if generations.is_empty() {
            let errors = heard_to_errors(heard, None);
            let accepted = self.read_accepts(&errors, set.read_quorum)?;
            return Ok(if accepted { Some(Vec::new()) } else { None });
        }
        let mut decoded = Vec::new();
        for (generation, payload_len) in generations {
            let errors = heard_to_errors(heard, Some((generation, payload_len)));
            if !self.read_accepts(&errors, set.read_quorum)? {
                continue;
            }
            let mut slots = vec![None; set.drives.len()];
            for slot in heard {
                if let Some(Ok(Some(replica))) = slot
                    && replica.generation == generation
                    && replica.payload_len == payload_len
                    && let Some(shard) = unframe_shard(&replica.framed)
                    && replica.shard_index < slots.len()
                {
                    slots[replica.shard_index] = Some(shard);
                }
            }
            let set_index = self.set_index(object);
            if let Ok(bytes) = decode_payload_ref(&self.codecs[set_index], set.data_blocks, &mut slots, payload_len) {
                decoded.push(bytes);
            }
        }
        decoded.sort();
        decoded.dedup();
        if decoded.len() > 1 {
            return Err(format!("{object} reconstructed more than one payload"));
        }
        Ok(decoded.into_iter().next())
    }

    fn inspect(&self, object: &str) -> Result<Vec<u8>, String> {
        let set_index = self.set_index(object);
        let set = self.topology.sets[set_index].clone();
        let mut groups: InspectGroups = BTreeMap::new();
        for disk_index in 0..set.drives.len() {
            let drive = self.flat_drive(set_index, disk_index);
            let Some(replica) = self.drives[drive].objects.get(object) else {
                continue;
            };
            let Some(shard) = unframe_shard(&replica.framed) else {
                continue;
            };
            if replica.shard_index != expected_shard_index(object, &set, disk_index) {
                continue;
            }
            groups
                .entry((replica.generation, replica.payload_len))
                .or_default()
                .push((replica.shard_index, shard));
        }
        let mut found = Vec::new();
        for ((_, payload_len), shards) in groups {
            if shards.len() < set.read_quorum {
                continue;
            }
            let mut slots = vec![None; set.drives.len()];
            for (shard_index, shard) in shards {
                if shard_index < slots.len() {
                    slots[shard_index] = Some(shard);
                }
            }
            // inspect borrows self, so decode through a fresh codec call on a local copy of slots
            // using the world's codec requires &mut. Rebuild with try_new is wasteful.
            // Use a scoped raw decode by temporarily... we only have &self.
            // Store codecs in RefCell? Simpler: Erasure::decode_data needs &self, not &mut self.
            // codecs is not mut. decode_data takes &self. So &self.codecs works.
            if let Ok(bytes) = decode_payload_ref(&self.codecs[set_index], set.data_blocks, &mut slots, payload_len) {
                found.push(bytes);
            }
        }
        found.sort();
        found.dedup();
        match found.len() {
            0 => Err(format!("{object} is not readable")),
            1 => Ok(found.remove(0)),
            _ => Err(format!("{object} has conflicting readable payloads")),
        }
    }

    fn valid_replicas(&self, object: &str, generation: u64, payload_len: usize) -> usize {
        let set_index = self.set_index(object);
        let drive_count = self.topology.sets[set_index].drives.len();
        (0..drive_count)
            .filter(|&disk_index| {
                let drive = self.flat_drive(set_index, disk_index);
                self.valid_replica_on(drive, object, generation, payload_len)
            })
            .count()
    }

    fn valid_replica_on(&self, drive: usize, object: &str, generation: u64, payload_len: usize) -> bool {
        let set_index = self.drives[drive].set_index;
        let disk_index = self.drives[drive].disk_index;
        let Some((stored_generation, stored_len, shard_index, framed_ok)) =
            self.drives[drive].objects.get(object).map(|replica| {
                (
                    replica.generation,
                    replica.payload_len,
                    replica.shard_index,
                    unframe_shard(&replica.framed).is_some(),
                )
            })
        else {
            return false;
        };
        let expected = expected_shard_index(object, &self.topology.sets[set_index], disk_index);
        stored_generation == generation && stored_len == payload_len && shard_index == expected && framed_ok
    }

    fn read_accepts(&mut self, errors: &[Option<SimDiskFault>], quorum: usize) -> Result<bool, String> {
        match read_quorum_met(errors, quorum) {
            Ok(accepted) => Ok(accepted),
            Err(detail) => {
                self.sticky = Some(detail.clone());
                Err(detail)
            }
        }
    }

    fn write_accepts(&mut self, errors: &[Option<SimDiskFault>], quorum: usize) -> Result<bool, String> {
        match write_quorum_met(errors, quorum) {
            Ok(accepted) => Ok(accepted),
            Err(detail) => {
                self.sticky = Some(detail.clone());
                Err(detail)
            }
        }
    }

    fn finish_mutate(&mut self, op: u64, deadline: u64, quorum: usize, is_write: bool) -> Result<(), String> {
        self.pump(deadline, op);
        self.fill_mutate_timeouts();
        let errors = self.mutate_errors();
        let accepted = if is_write {
            self.write_accepts(&errors, quorum)
        } else {
            self.read_accepts(&errors, quorum)
        };
        match accepted {
            Ok(accepted) => {
                self.close_op(op, accepted);
                Ok(())
            }
            Err(error) => {
                self.close_op(op, false);
                Err(error)
            }
        }
    }

    fn mutate_succeeded(&self, _op: u64) -> bool {
        self.last_success
    }

    fn open_mutate(&mut self, disks: usize) -> u64 {
        let id = self.next_op;
        self.next_op += 1;
        self.last_success = false;
        self.inflight = Some(Inflight {
            id,
            heard: Hear::Mutate(vec![None; disks]),
        });
        id
    }

    fn open_read(&mut self, disks: usize) -> u64 {
        let id = self.next_op;
        self.next_op += 1;
        self.inflight = Some(Inflight {
            id,
            heard: Hear::Read(vec![None; disks]),
        });
        id
    }

    #[allow(clippy::too_many_arguments)] // scheduler message fields stay explicit at the call site
    fn send_store(
        &mut self,
        op: u64,
        drive: usize,
        disk_index: usize,
        object: &str,
        generation: u64,
        payload_len: usize,
        shard_index: usize,
        framed: Vec<u8>,
    ) {
        if !self.drive_reachable(drive) {
            let error = if self.drives[drive].faulted {
                SimDiskFault::FaultyDisk
            } else {
                SimDiskFault::DiskNotFound
            };
            self.record_mutate(op, disk_index, Err(error));
            return;
        }
        let epoch = self.drives[drive].epoch;
        let fail = self.rng.chance(self.config.rates.io_fail_numer, self.config.rates.denom);
        let delay = self.rng.gen_range(self.config.rates.max_delay_us.saturating_add(1));
        if self.rng.chance(self.config.rates.drop_numer, self.config.rates.denom) {
            return;
        }
        let event = Event::Store {
            op,
            drive,
            disk_index,
            key: object.to_string(),
            generation,
            payload_len,
            shard_index,
            framed: framed.clone(),
            epoch,
            fail,
        };
        self.schedule(self.now.saturating_add(delay), event);
        if self.rng.chance(self.config.rates.dup_numer, self.config.rates.denom) {
            let delay = self.rng.gen_range(self.config.rates.max_delay_us.saturating_add(1));
            self.schedule(
                self.now.saturating_add(delay),
                Event::Store {
                    op,
                    drive,
                    disk_index,
                    key: object.to_string(),
                    generation,
                    payload_len,
                    shard_index,
                    framed,
                    epoch,
                    fail,
                },
            );
        }
    }

    fn send_delete(&mut self, op: u64, drive: usize, disk_index: usize, object: &str, generation: u64) {
        if !self.drive_reachable(drive) {
            self.record_mutate(op, disk_index, Err(SimDiskFault::DiskNotFound));
            return;
        }
        let epoch = self.drives[drive].epoch;
        let fail = self.rng.chance(self.config.rates.io_fail_numer, self.config.rates.denom);
        let delay = self.rng.gen_range(self.config.rates.max_delay_us.saturating_add(1));
        if self.rng.chance(self.config.rates.drop_numer, self.config.rates.denom) {
            return;
        }
        self.schedule(
            self.now.saturating_add(delay),
            Event::Delete {
                op,
                drive,
                disk_index,
                key: object.to_string(),
                generation,
                epoch,
                fail,
            },
        );
    }

    fn send_read(&mut self, op: u64, drive: usize, disk_index: usize, object: &str) {
        if !self.drive_reachable(drive) {
            self.record_read(op, disk_index, Err(SimDiskFault::DiskNotFound));
            return;
        }
        let epoch = self.drives[drive].epoch;
        let fail = self.rng.chance(self.config.rates.io_fail_numer, self.config.rates.denom);
        let delay = self.rng.gen_range(self.config.rates.max_delay_us.saturating_add(1));
        if self.rng.chance(self.config.rates.drop_numer, self.config.rates.denom) {
            return;
        }
        self.schedule(
            self.now.saturating_add(delay),
            Event::Read {
                op,
                drive,
                disk_index,
                key: object.to_string(),
                epoch,
                fail,
            },
        );
    }

    fn schedule_op_faults(&mut self, set_index: usize) {
        let rates = self.config.rates.clone();
        if self.rng.chance(rates.clock_jump_numer, rates.fault_denom) {
            let at = self.now + self.rng.gen_range(self.config.op_timeout_us / 2 + 1);
            let delta = self.config.op_timeout_us + self.rng.gen_range(rates.max_clock_jump_us.saturating_add(1));
            self.schedule(at, Event::ClockJump { delta_us: delta });
        }
        if self.rng.chance(rates.crash_numer, rates.fault_denom) {
            let drive_count = self.topology.sets[set_index].drives.len();
            if drive_count == 0 {
                return;
            }
            let disk = self.rng.gen_range(drive_count as u64) as usize;
            let node = self.topology.sets[set_index].drives[disk].node_index;
            let at = self.now + self.rng.gen_range(self.config.op_timeout_us / 2 + 1);
            self.schedule(at, Event::CrashNode { node });
        }
    }

    fn pump(&mut self, deadline: u64, op: u64) {
        loop {
            if self.now >= deadline || self.heard_complete() {
                return;
            }
            let Some((key, event)) = self.events.pop_first() else {
                return;
            };
            if key.0 > deadline && self.now < deadline {
                self.events.insert(key, event);
                self.now = deadline;
                return;
            }
            if key.0 > self.now {
                self.now = key.0;
            }
            self.handle(event);
            let _ = op;
        }
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::ClockJump { delta_us } => {
                self.now = self.now.saturating_add(delta_us);
            }
            Event::CrashNode { node } => {
                if node < self.nodes.len() {
                    self.nodes[node].up = false;
                    self.bump_epoch_for_node(node);
                }
            }
            Event::Store {
                op,
                drive,
                disk_index,
                key,
                generation,
                payload_len,
                shard_index,
                framed,
                epoch,
                fail,
            } => self.on_store(op, drive, disk_index, key, generation, payload_len, shard_index, framed, epoch, fail),
            Event::Delete {
                op,
                drive,
                disk_index,
                key,
                generation,
                epoch,
                fail,
            } => self.on_delete(op, drive, disk_index, key, generation, epoch, fail),
            Event::Read {
                op,
                drive,
                disk_index,
                key,
                epoch,
                fail,
            } => self.on_read(op, drive, disk_index, key, epoch, fail),
        }
    }

    #[allow(clippy::too_many_arguments)] // scheduler message fields stay explicit at the call site
    fn on_store(
        &mut self,
        op: u64,
        drive: usize,
        disk_index: usize,
        key: String,
        generation: u64,
        payload_len: usize,
        shard_index: usize,
        framed: Vec<u8>,
        epoch: u64,
        fail: bool,
    ) {
        if self.closed.contains(&op) || !self.event_live(drive, epoch) {
            return;
        }
        if fail {
            self.record_mutate(op, disk_index, Err(SimDiskFault::FaultyDisk));
            return;
        }
        if let Some(existing) = self.drives[drive].objects.get(&key) {
            if existing.generation == generation && existing.framed == framed {
                self.record_mutate(op, disk_index, Ok(()));
                return;
            }
            if existing.generation > generation {
                self.record_mutate(op, disk_index, Err(SimDiskFault::FaultyDisk));
                return;
            }
        }
        if self.drives[drive].seen.get(&key).copied().unwrap_or(0) > generation {
            self.record_mutate(op, disk_index, Err(SimDiskFault::FaultyDisk));
            return;
        }
        self.push_undo(op, drive, &key);
        self.drives[drive].objects.insert(
            key.clone(),
            Replica {
                generation,
                payload_len,
                shard_index,
                framed,
            },
        );
        self.drives[drive].seen.insert(key, generation);
        self.record_mutate(op, disk_index, Ok(()));
    }

    #[allow(clippy::too_many_arguments)] // scheduler message fields stay explicit at the call site
    fn on_delete(&mut self, op: u64, drive: usize, disk_index: usize, key: String, generation: u64, epoch: u64, fail: bool) {
        if self.closed.contains(&op) || !self.event_live(drive, epoch) {
            return;
        }
        if fail {
            self.record_mutate(op, disk_index, Err(SimDiskFault::FaultyDisk));
            return;
        }
        let current = self.drives[drive].objects.get(&key).map(|replica| replica.generation);
        if current.is_some_and(|current| current > generation) {
            self.record_mutate(op, disk_index, Err(SimDiskFault::FaultyDisk));
            return;
        }
        if current.is_none() && self.drives[drive].seen.get(&key).copied().unwrap_or(0) >= generation {
            self.record_mutate(op, disk_index, Ok(()));
            return;
        }
        self.push_undo(op, drive, &key);
        self.drives[drive].objects.remove(&key);
        let seen = self.drives[drive].seen.get(&key).copied().unwrap_or(0).max(generation);
        self.drives[drive].seen.insert(key, seen);
        self.record_mutate(op, disk_index, Ok(()));
    }

    fn on_read(&mut self, op: u64, drive: usize, disk_index: usize, key: String, epoch: u64, fail: bool) {
        if self.closed.contains(&op) || !self.event_live(drive, epoch) {
            return;
        }
        if fail {
            self.record_read(op, disk_index, Err(SimDiskFault::FaultyDisk));
            return;
        }
        match self.drives[drive].objects.get(&key) {
            Some(replica) if unframe_shard(&replica.framed).is_some() => {
                self.record_read(op, disk_index, Ok(Some(replica.clone())));
            }
            Some(_) => self.record_read(op, disk_index, Err(SimDiskFault::FileCorrupt)),
            None => self.record_read(op, disk_index, Err(SimDiskFault::FileNotFound)),
        }
    }

    fn event_live(&self, drive: usize, epoch: u64) -> bool {
        let state = &self.drives[drive];
        state.epoch == epoch && self.drive_reachable(drive)
    }

    fn drive_reachable(&self, drive: usize) -> bool {
        let node = self.drives[drive].node_index;
        let faulted = self.drives[drive].faulted;
        !faulted && self.node_reachable(node)
    }

    fn node_reachable(&self, node: usize) -> bool {
        self.nodes.get(node).is_some_and(|node| node.up && !node.isolated)
    }

    fn exclusive_lock_available(&self, set_index: usize) -> bool {
        let required = self.topology.sets[set_index].exclusive_lock_quorum;
        self.reachable_lock_votes(set_index) >= required
    }

    fn shared_lock_available(&self, set_index: usize) -> bool {
        let required = self.topology.sets[set_index].shared_lock_quorum;
        self.reachable_lock_votes(set_index) >= required
    }

    #[allow(clippy::needless_collect)] // copy indexes before node_reachable borrows self
    fn reachable_lock_votes(&self, set_index: usize) -> usize {
        let nodes: Vec<usize> = self.topology.sets[set_index]
            .drives
            .iter()
            .map(|drive| drive.node_index)
            .collect();
        nodes
            .into_iter()
            .filter(|node| self.node_reachable(*node))
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn bump_epoch_for_node(&mut self, node: usize) {
        for drive in &mut self.drives {
            if drive.node_index == node {
                drive.epoch = drive.epoch.saturating_add(1);
            }
        }
    }

    fn schedule(&mut self, at: u64, event: Event) {
        self.seq = self.seq.saturating_add(1);
        self.events.insert((at, self.seq), event);
    }

    fn push_undo(&mut self, op: u64, drive: usize, key: &str) {
        if self
            .undos
            .iter()
            .any(|undo| undo.op == op && undo.drive == drive && undo.key == key)
        {
            return;
        }
        let previous = self.drives[drive].objects.get(key).cloned();
        let previous_seen = self.drives[drive].seen.get(key).copied();
        self.undos.push(Undo {
            op,
            drive,
            key: key.to_string(),
            previous,
            previous_seen,
        });
    }

    fn close_op(&mut self, op: u64, success: bool) {
        self.closed.insert(op);
        if success {
            self.undos.retain(|undo| undo.op != op);
            self.last_success = true;
        } else {
            let mut undos = Vec::new();
            self.undos.retain(|undo| {
                if undo.op == op {
                    undos.push(Undo {
                        op: undo.op,
                        drive: undo.drive,
                        key: undo.key.clone(),
                        previous: undo.previous.clone(),
                        previous_seen: undo.previous_seen,
                    });
                    false
                } else {
                    true
                }
            });
            for undo in undos.into_iter().rev() {
                let drive = &mut self.drives[undo.drive];
                match undo.previous {
                    Some(replica) => {
                        drive.objects.insert(undo.key.clone(), replica);
                    }
                    None => {
                        drive.objects.remove(&undo.key);
                    }
                }
                match undo.previous_seen {
                    Some(seen) => {
                        drive.seen.insert(undo.key, seen);
                    }
                    None => {
                        drive.seen.remove(&undo.key);
                    }
                }
            }
            self.last_success = false;
        }
        self.inflight = None;
    }

    fn record_mutate(&mut self, op: u64, disk_index: usize, result: Result<(), SimDiskFault>) {
        let Some(inflight) = self.inflight.as_mut() else {
            return;
        };
        if inflight.id != op {
            return;
        }
        if let Hear::Mutate(heard) = &mut inflight.heard
            && let Some(slot) = heard.get_mut(disk_index)
            && slot.is_none()
        {
            *slot = Some(result);
        }
    }

    fn record_read(&mut self, op: u64, disk_index: usize, result: Result<Option<Replica>, SimDiskFault>) {
        let Some(inflight) = self.inflight.as_mut() else {
            return;
        };
        if inflight.id != op {
            return;
        }
        if let Hear::Read(heard) = &mut inflight.heard
            && let Some(slot) = heard.get_mut(disk_index)
            && slot.is_none()
        {
            *slot = Some(result);
        }
    }

    fn heard_complete(&self) -> bool {
        match self.inflight.as_ref().map(|inflight| &inflight.heard) {
            Some(Hear::Mutate(heard)) => heard.iter().all(Option::is_some),
            Some(Hear::Read(heard)) => heard.iter().all(Option::is_some),
            None => true,
        }
    }

    fn fill_mutate_timeouts(&mut self) {
        if let Some(Inflight {
            heard: Hear::Mutate(heard),
            ..
        }) = self.inflight.as_mut()
        {
            for slot in heard.iter_mut() {
                if slot.is_none() {
                    *slot = Some(Err(SimDiskFault::DiskNotFound));
                }
            }
        }
    }

    fn mutate_errors(&self) -> Vec<Option<SimDiskFault>> {
        let Some(Inflight {
            heard: Hear::Mutate(heard),
            ..
        }) = self.inflight.as_ref()
        else {
            return Vec::new();
        };
        heard
            .iter()
            .map(|slot| match slot {
                Some(Ok(())) => None,
                Some(Err(error)) => Some(error.clone()),
                None => Some(SimDiskFault::DiskNotFound),
            })
            .collect()
    }

    fn take_read(&mut self, op: u64) -> Vec<Option<Result<Option<Replica>, SimDiskFault>>> {
        let Some(inflight) = self.inflight.as_mut() else {
            return Vec::new();
        };
        if inflight.id != op {
            return Vec::new();
        }
        match &mut inflight.heard {
            Hear::Read(heard) => {
                for slot in heard.iter_mut() {
                    if slot.is_none() {
                        *slot = Some(Err(SimDiskFault::DiskNotFound));
                    }
                }
                heard.clone()
            }
            Hear::Mutate(_) => Vec::new(),
        }
    }

    fn set_index(&self, object: &str) -> usize {
        sip_hash(object, self.topology.sets.len().max(1), &FORMAT_ID)
    }

    fn set_for(&self, object: &str) -> &SetPlan {
        &self.topology.sets[self.set_index(object)]
    }

    fn flat_drive(&self, set_index: usize, disk_index: usize) -> usize {
        self.drives
            .iter()
            .position(|drive| drive.set_index == set_index && drive.disk_index == disk_index)
            .expect("every set drive is registered")
    }

    fn media_keys(&self) -> BTreeSet<String> {
        self.drives.iter().flat_map(|drive| drive.objects.keys().cloned()).collect()
    }

    fn random_object(&mut self) -> String {
        let index = self.rng.gen_range(self.config.max_objects.max(1) as u64);
        format!("obj-{index}")
    }

    fn random_known_object(&mut self) -> Option<String> {
        if self.oracle.is_empty() {
            return None;
        }
        let index = self.rng.gen_range(self.oracle.len() as u64) as usize;
        self.oracle.keys().nth(index).cloned()
    }

    fn random_payload(&mut self) -> Vec<u8> {
        let len = self.rng.gen_range(self.config.max_payload.saturating_add(1) as u64) as usize;
        (0..len).map(|_| self.rng.next_u64() as u8).collect()
    }

    fn note(&mut self, line: String) {
        if self.config.trace {
            self.trace.push(format!("t={} {line}", self.now));
        }
    }

    fn violation(&self, invariant: &str, detail: String) -> Violation {
        Violation {
            step: self.step_index,
            invariant: invariant.to_string(),
            detail,
        }
    }
}

fn drive_from_plan(set_index: usize, disk_index: usize, plan: &DrivePlan) -> Drive {
    Drive {
        set_index,
        disk_index,
        node_index: plan.node_index,
        endpoint: plan.endpoint.clone(),
        faulted: false,
        epoch: 1,
        objects: BTreeMap::new(),
        seen: BTreeMap::new(),
    }
}

fn distribution_for(object: &str, set: &SetPlan) -> Vec<usize> {
    FileInfo::new(&format!("{DST_BUCKET}/{object}"), set.data_blocks, set.parity_blocks)
        .erasure
        .distribution
}

type InspectGroups = BTreeMap<(u64, usize), Vec<(usize, Vec<u8>)>>;

fn shard_slot(distribution: &[usize], disk_index: usize, shard_count: usize) -> Result<usize, String> {
    let Some(slot) = distribution.get(disk_index).copied() else {
        return Err("distribution is shorter than the set".to_string());
    };
    if slot == 0 || slot > shard_count {
        return Err(format!("distribution slot {slot} is outside 1..={shard_count}"));
    }
    Ok(slot - 1)
}

fn expected_shard_index(object: &str, set: &SetPlan, disk_index: usize) -> usize {
    let distribution = distribution_for(object, set);
    distribution.get(disk_index).copied().unwrap_or(1).saturating_sub(1)
}

fn decode_payload_ref(
    codec: &SimCodec,
    data_blocks: usize,
    slots: &mut [Option<Vec<u8>>],
    payload_len: usize,
) -> Result<Vec<u8>, String> {
    codec.decode(slots)?;
    let mut out = Vec::new();
    for slot in slots.iter().take(data_blocks) {
        let Some(shard) = slot else {
            return Err("missing data shard after decode".to_string());
        };
        out.extend_from_slice(shard);
    }
    if out.len() < payload_len {
        return Err(format!("decoded {} bytes, need {payload_len}", out.len()));
    }
    out.truncate(payload_len);
    Ok(out)
}

fn heard_to_errors(
    heard: &[Option<Result<Option<Replica>, SimDiskFault>>],
    want: Option<(u64, usize)>,
) -> Vec<Option<SimDiskFault>> {
    heard
        .iter()
        .map(|slot| match (slot, want) {
            (Some(Ok(Some(replica))), Some((generation, payload_len)))
                if replica.generation == generation
                    && replica.payload_len == payload_len
                    && unframe_shard(&replica.framed).is_some() =>
            {
                None
            }
            (Some(Ok(Some(replica))), Some((generation, payload_len)))
                if replica.generation == generation && replica.payload_len == payload_len =>
            {
                Some(SimDiskFault::FileCorrupt)
            }
            (Some(Ok(_)), _) => Some(SimDiskFault::FileNotFound),
            (Some(Err(error)), _) => Some(error.clone()),
            (None, _) => Some(SimDiskFault::DiskNotFound),
        })
        .collect()
}
