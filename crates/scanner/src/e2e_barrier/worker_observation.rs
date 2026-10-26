// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! N-client evidence only. This recorder never waits for a release or filters buckets.

use super::{read_control, valid_component, write_observation};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

const MAX_EVENTS: usize = 512;
const MAX_CYCLES: usize = 8;
const MAX_BYTES: usize = 1024 * 1024;
static OBSERVER: OnceLock<Mutex<Option<Observer>>> = OnceLock::new();
tokio::task_local! { static INCARNATION: Option<Uuid>; }

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    nonce: String,
    pid: u32,
}

#[derive(Serialize)]
struct Request {
    context: Value,
    opening_sequence: usize,
    complete_sequence: Option<usize>,
    progress: Option<[u64; 3]>,
}

#[derive(Default)]
struct Cycle {
    start_sequence: usize,
    events: Vec<Value>,
    requests: BTreeMap<String, Request>,
    local_walks: Vec<Value>,
    candidate: Option<Value>,
    closed: bool,
}

struct Observer {
    directory: PathBuf,
    nonce: String,
    pid: u32,
    sequence: usize,
    errors: BTreeSet<&'static str>,
    cycles: BTreeMap<(u64, u64), Cycle>,
    manifests: Vec<Value>,
}

fn invalid(code: &'static str) -> io::Error {
    io::Error::other(code)
}

fn cycle_key(detail: &Value) -> io::Result<(u64, u64)> {
    Ok((
        detail["epoch"].as_u64().ok_or_else(|| invalid("malformed"))?,
        detail["cycle"].as_u64().ok_or_else(|| invalid("malformed"))?,
    ))
}

fn uuid_field(detail: &Value, name: &str) -> io::Result<()> {
    let value = detail[name].as_str().ok_or_else(|| invalid("malformed"))?;
    if Uuid::parse_str(value).is_ok_and(|id| !id.is_nil()) {
        Ok(())
    } else {
        Err(invalid("malformed"))
    }
}

impl Observer {
    fn new(directory: PathBuf, control: Control) -> Self {
        Self {
            directory,
            nonce: control.nonce,
            pid: control.pid,
            sequence: 0,
            errors: BTreeSet::new(),
            cycles: BTreeMap::new(),
            manifests: Vec::new(),
        }
    }

    fn health(&self) -> Value {
        json!({"schema": 1, "nonce": self.nonce, "pid": self.pid, "event_count": self.sequence,
            "max_events": MAX_EVENTS, "max_cycles": MAX_CYCLES, "max_bytes": MAX_BYTES,
            "errors": self.errors, "manifests": self.manifests})
    }

    fn persist(&self, name: &str, value: &Value) -> io::Result<String> {
        let bytes = serde_json::to_vec(value)?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("overflow"));
        }
        write_observation(&self.directory, name, &bytes)?;
        Ok(super::hex(&Sha256::digest(&bytes)))
    }

    fn event(&mut self, key: (u64, u64), kind: &str, detail: Value) -> io::Result<usize> {
        if self.sequence >= MAX_EVENTS {
            return Err(invalid("overflow"));
        }
        let sequence = self.sequence;
        self.sequence += 1;
        let event = json!({"schema": 1, "nonce": self.nonce, "pid": self.pid,
            "sequence": sequence, "kind": kind, "epoch": key.0, "cycle": key.1, "detail": detail});
        let name = format!("worker-observed.{}.{}.{sequence}.json", self.nonce, self.pid);
        let sha256 = self.persist(&name, &event)?;
        self.cycles
            .get_mut(&key)
            .ok_or_else(|| invalid("outside_cycle"))?
            .events
            .push(json!({"file": name, "sha256": sha256, "sequence": sequence, "kind": kind}));
        Ok(sequence)
    }

    fn active(&self, key: (u64, u64)) -> io::Result<()> {
        match self.cycles.get(&key) {
            Some(cycle) if !cycle.closed => Ok(()),
            Some(_) => Err(invalid("late")),
            None => Err(invalid("outside_cycle")),
        }
    }

    fn boundary(&mut self, stage: &str, detail: Value) -> io::Result<()> {
        let key = cycle_key(&detail)?;
        if stage == "before_start_snapshot" {
            if self.cycles.len() >= MAX_CYCLES {
                return Err(invalid("overflow"));
            }
            if self.cycles.contains_key(&key) {
                return Err(invalid("duplicate"));
            }
            if self.cycles.values().any(|cycle| !cycle.closed) {
                return Err(invalid("cross_cycle"));
            }
            self.cycles.insert(
                key,
                Cycle {
                    start_sequence: self.sequence,
                    ..Default::default()
                },
            );
        }
        self.active(key)?;
        match stage {
            "before_start_snapshot" => {
                self.event(key, "cycle_begin", detail)?;
            }
            "bucket_walk" => {
                if self.cycles[&key].candidate.is_some() {
                    return Err(invalid("late"));
                }
                uuid_field(&detail, "bucket_incarnation")?;
                uuid_field(&detail, "disk_uuid")?;
                let sequence = self.event(key, "local_walk", detail.clone())?;
                self.cycles
                    .get_mut(&key)
                    .ok_or_else(|| invalid("outside_cycle"))?
                    .local_walks
                    .push(json!({"sequence": sequence, "context": detail}));
            }
            "cycle_candidate" => {
                if self.cycles[&key].candidate.is_some() {
                    return Err(invalid("duplicate"));
                }
                self.event(key, "candidate", detail.clone())?;
                self.cycles.get_mut(&key).ok_or_else(|| invalid("outside_cycle"))?.candidate = Some(detail);
            }
            "root_durable" => self.close(key, detail)?,
            _ => return Err(invalid("malformed")),
        }
        Ok(())
    }

    fn opening(&mut self, mut context: Value, incarnation: Option<Uuid>) -> io::Result<()> {
        let key = cycle_key(&context)?;
        // A removed session ends the measurement window, but never hides a late event for a known cycle.
        if !self.cycles.contains_key(&key) && !self.directory.join("session.json").try_exists()? {
            return Ok(());
        }
        self.active(key)?;
        if self.cycles[&key].candidate.is_some() {
            return Err(invalid("late"));
        }
        context["bucket_incarnation"] = json!(incarnation);
        for field in [
            "request_id",
            "wire_session_id",
            "server_epoch",
            "bucket_incarnation",
            "disk_uuid",
        ] {
            uuid_field(&context, field)?;
        }
        for field in ["pool", "set", "disk_index", "wire_session_sequence"] {
            context[field].as_u64().ok_or_else(|| invalid("malformed"))?;
        }
        for field in [
            "bucket",
            "worker_endpoint",
            "worker_path",
            "reuse_plan_sha256",
            "coverage_plan_sha256",
        ] {
            if context[field].as_str().is_none_or(str::is_empty) {
                return Err(invalid("malformed"));
            }
        }
        for field in ["reuse_plan_sha256", "coverage_plan_sha256"] {
            if context[field]
                .as_str()
                .is_none_or(|value| value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            {
                return Err(invalid("malformed"));
            }
        }
        let id = context["request_id"]
            .as_str()
            .ok_or_else(|| invalid("malformed"))?
            .to_string();
        if self.cycles.values().any(|cycle| cycle.requests.contains_key(&id)) {
            return Err(invalid("duplicate"));
        }
        let sequence = self.event(key, "remote_scan_opening", context.clone())?;
        self.cycles
            .get_mut(&key)
            .ok_or_else(|| invalid("outside_cycle"))?
            .requests
            .insert(
                id,
                Request {
                    context,
                    opening_sequence: sequence,
                    complete_sequence: None,
                    progress: None,
                },
            );
        Ok(())
    }

    fn complete(&mut self, id: Uuid, progress: [u64; 3]) -> io::Result<()> {
        let id = id.to_string();
        let key = self
            .cycles
            .iter()
            .find_map(|(key, cycle)| cycle.requests.contains_key(&id).then_some(*key));
        let Some(key) = key else {
            if !self.directory.join("session.json").try_exists()? {
                return Ok(());
            }
            return Err(invalid("unpaired"));
        };
        self.active(key)?;
        if self.cycles[&key].candidate.is_some() {
            return Err(invalid("late"));
        }
        let request = &self.cycles[&key].requests[&id];
        if request.complete_sequence.is_some() {
            return Err(invalid("duplicate"));
        }
        let detail = json!({"request_id": id, "context": request.context, "progress": progress,
            "eof_verified": true, "mac_source_plan_verified": true});
        let sequence = self.event(key, "remote_scan_complete", detail)?;
        let request = self
            .cycles
            .get_mut(&key)
            .and_then(|cycle| cycle.requests.get_mut(&id))
            .ok_or_else(|| invalid("unpaired"))?;
        request.complete_sequence = Some(sequence);
        request.progress = Some(progress);
        Ok(())
    }

    // Hooks are synchronous; all worker/set tasks join before candidate/root.
    // A closed manifest is per-cycle evidence, not a continuous live-health attestation.
    fn close(&mut self, key: (u64, u64), root: Value) -> io::Result<()> {
        if !self.errors.is_empty() {
            return Err(invalid("unhealthy"));
        }
        let cycle = &self.cycles[&key];
        let candidate = cycle.candidate.as_ref().ok_or_else(|| invalid("unpaired"))?;
        for field in ["candidate_sha256", "coverage_sha256"] {
            if root[field].as_str().is_none() || root[field] != candidate[field] {
                return Err(invalid("coverage"));
            }
        }
        let mut observed = BTreeSet::new();
        for context in cycle
            .requests
            .values()
            .map(|request| &request.context)
            .chain(cycle.local_walks.iter().map(|walk| &walk["context"]))
        {
            let pool = context["pool"].as_u64().ok_or_else(|| invalid("malformed"))?;
            let set = context["set"].as_u64().ok_or_else(|| invalid("malformed"))?;
            let bucket = context["bucket"].as_str().ok_or_else(|| invalid("malformed"))?;
            if !observed.insert((pool, set, bucket)) {
                return Err(invalid("duplicate"));
            }
            let coverage = candidate["set_coverage"].as_array().ok_or_else(|| invalid("coverage"))?;
            if !coverage.iter().any(|row| {
                row["pool"].as_u64() == Some(pool)
                    && row["set"].as_u64() == Some(set)
                    && row["bucket_incarnations"][bucket] == context["bucket_incarnation"]
                    && !context["bucket_incarnation"].is_null()
            }) {
                return Err(invalid("coverage"));
            }
        }
        if cycle.requests.values().any(|request| request.complete_sequence.is_none()) {
            return Err(invalid("unpaired"));
        }
        let sequence = self.event(key, "cycle_close", root.clone())?;
        self.cycles.get_mut(&key).ok_or_else(|| invalid("outside_cycle"))?.closed = true;
        let cycle = &self.cycles[&key];
        let manifest = json!({"schema": 1, "nonce": self.nonce, "pid": self.pid, "epoch": key.0, "cycle": key.1,
            "start_sequence": cycle.start_sequence, "close_sequence": sequence, "events": cycle.events,
            "requests": cycle.requests, "local_walks": cycle.local_walks, "candidate": cycle.candidate,
            "root": root, "errors": self.errors, "outstanding": 0, "closed": true});
        let name = format!("worker-manifest.{}.{}.{}.{}.json", self.nonce, self.pid, key.0, key.1);
        let sha256 = self.persist(&name, &manifest)?;
        self.manifests
            .push(json!({"file": name, "sha256": sha256, "epoch": key.0, "cycle": key.1,
            "close_sequence": sequence}));
        Ok(())
    }
    fn apply(&mut self, action: impl FnOnce(&mut Self) -> io::Result<()>) {
        if let Err(error) = action(self) {
            let code = match error.to_string().as_str() {
                "overflow" => "overflow",
                "late" => "late",
                "duplicate" => "duplicate",
                "coverage" => "coverage",
                "unpaired" => "unpaired",
                "outside_cycle" => "outside_cycle",
                "cross_cycle" => "cross_cycle",
                _ => "io_or_malformed",
            };
            self.errors.insert(code);
        }
        let name = format!("worker-health.{}.{}.json", self.nonce, self.pid);
        if self.persist(&name, &self.health()).is_err() {
            self.errors.insert("health_io");
        }
    }
}

fn control(directory: &Path) -> io::Result<Option<Control>> {
    if !directory.is_absolute() || directory.components().any(|part| matches!(part, Component::ParentDir)) {
        return Err(invalid("malformed"));
    }
    let control = read_control::<Control>(&directory.join("worker-observer.json"))?;
    if control
        .as_ref()
        .is_some_and(|control| !valid_component(&control.nonce) || control.pid == 0)
    {
        return Err(invalid("malformed"));
    }
    Ok(control)
}

/// The scope covers the normal open and consume future; it never consults O again.
pub(crate) async fn with_incarnation<F: Future>(incarnation: Option<Uuid>, future: F) -> F::Output {
    INCARNATION.scope(incarnation, future).await
}

pub(crate) fn enabled() -> bool {
    // Once begun, a transient control read failure must never hide a real opening or completion.
    if OBSERVER.get().is_some_and(|state| {
        state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .is_some_and(|observer| observer.pid == std::process::id())
    }) {
        return true;
    }
    let Some(directory) = std::env::var_os("RUSTFS_E2E_SCANNER_BARRIER_DIR") else { return false };
    control(Path::new(&directory))
        .ok()
        .flatten()
        .is_some_and(|control| control.pid == std::process::id())
}

fn update(action: impl FnOnce(&mut Observer) -> io::Result<()>) {
    let Some(directory) = std::env::var_os("RUSTFS_E2E_SCANNER_BARRIER_DIR") else { return };
    let directory = PathBuf::from(directory);
    let (mut guard, poisoned) = match OBSERVER.get_or_init(|| Mutex::new(None)).lock() {
        Ok(guard) => (guard, false),
        Err(poison) => (poison.into_inner(), true),
    };
    let configured = control(&directory);
    match configured {
        Ok(Some(control)) if control.pid == std::process::id() => {
            if let Some(observer) = guard.as_mut() {
                if observer.nonce != control.nonce {
                    observer.errors.insert("session_changed");
                }
            } else {
                *guard = Some(Observer::new(directory, control));
            }
        }
        Ok(_) => {
            let Some(observer) = guard.as_mut() else { return };
            observer.errors.insert("control_changed");
        }
        Err(_) => {
            if let Some(observer) = guard.as_mut() {
                observer.errors.insert("control_io");
            }
        }
    }
    let Some(observer) = guard.as_mut() else { return };
    if poisoned {
        observer.errors.insert("poisoned");
    }
    observer.apply(action);
}

pub(crate) fn boundary(stage: &str, detail: &Value) {
    if !matches!(stage, "before_start_snapshot" | "cycle_candidate" | "root_durable" | "bucket_walk") {
        return;
    }
    if stage == "bucket_walk" && detail["worker_kind"] != "local" {
        return;
    }
    update(|observer| observer.boundary(stage, detail.clone()));
}

pub(crate) fn opening(context: Value) {
    let incarnation = INCARNATION.try_with(|incarnation| *incarnation).ok().flatten();
    update(|observer| observer.opening(context, incarnation));
}

/// Called only after the ordinary client's authenticated terminal EOF validation.
pub(crate) fn complete(id: Uuid, progress: [u64; 3]) {
    update(|observer| observer.complete(id, progress));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observer(path: &Path) -> Observer {
        std::fs::write(path.join("session.json"), b"{}").expect("active observation window");
        Observer::new(
            path.to_path_buf(),
            Control {
                nonce: "unit".into(),
                pid: 1,
            },
        )
    }
    fn context(id: Uuid, cycle: u64) -> Value {
        json!({"epoch": 1, "cycle": cycle, "request_id": id, "wire_session_id": Uuid::new_v4(),
            "server_epoch": Uuid::new_v4(), "wire_session_sequence": 0, "pool": 0, "set": 0,
            "disk_index": 0, "disk_uuid": Uuid::new_v4(), "bucket": "hot", "worker_endpoint": "http://worker:9000",
            "worker_path": "/disk", "reuse_plan_sha256": "a".repeat(64), "coverage_plan_sha256": "b".repeat(64)})
    }
    fn candidate(incarnation: Uuid) -> Value {
        json!({"epoch":1,"cycle":2,"candidate_sha256":"c".repeat(64),"coverage_sha256":"d".repeat(64),
            "set_coverage":[{"pool":0,"set":0,"bucket_incarnations":{"hot":incarnation}}]})
    }
    fn root() -> Value {
        json!({"epoch":1,"cycle":2,"candidate_sha256":"c".repeat(64),"coverage_sha256":"d".repeat(64),
            "root_sha256":"e".repeat(64),"root_version":"root"})
    }
    #[test]
    fn worker_inventory_rejects_unpaired_partial_and_late_completions() {
        let dir = tempfile::tempdir().expect("temporary evidence directory");
        let mut observer = observer(dir.path());
        observer
            .boundary("before_start_snapshot", json!({"epoch":1,"cycle":2}))
            .expect("begin");
        let id = Uuid::new_v4();
        let incarnation = Uuid::new_v4();
        observer.opening(context(id, 2), Some(incarnation)).expect("opening");
        observer
            .boundary("cycle_candidate", candidate(incarnation))
            .expect("candidate");
        assert_eq!(
            observer
                .close((1, 2), root())
                .expect_err("Partial/MAC/EOF failure cannot pair opening")
                .to_string(),
            "unpaired"
        );
        observer.apply(|state| state.complete(id, [1, 0, 0]));
        assert!(observer.errors.contains("late"));
        assert!(observer.close((1, 2), root()).is_err());
        assert!(observer.complete(Uuid::new_v4(), [1, 0, 0]).is_err());
    }
    #[test]
    fn worker_inventory_preserves_zero_progress_and_rejects_cross_cycle_and_overflow() {
        let dir = tempfile::tempdir().expect("temporary evidence directory");
        let mut observer = observer(dir.path());
        observer
            .boundary("before_start_snapshot", json!({"epoch":1,"cycle":2}))
            .expect("begin");
        let id = Uuid::new_v4();
        assert!(observer.opening(context(id, 3), Some(Uuid::new_v4())).is_err());
        observer.opening(context(id, 2), Some(Uuid::new_v4())).expect("opening");
        observer
            .complete(id, [0, 0, 0])
            .expect("verified Current may make no progress");
        assert_eq!(observer.cycles[&(1, 2)].requests[&id.to_string()].progress, Some([0, 0, 0]));
        let incarnation = observer.cycles[&(1, 2)].requests[&id.to_string()].context["bucket_incarnation"]
            .as_str()
            .expect("captured incarnation")
            .parse()
            .expect("UUID");
        observer
            .boundary("cycle_candidate", candidate(incarnation))
            .expect("candidate");
        observer
            .close((1, 2), root())
            .expect("zero-progress Current is recorded, never silently treated as a walk");
        observer.apply(|state| state.complete(id, [1, 0, 0]));
        assert!(observer.errors.contains("late"));
        // Reserve exhaustion on a new active cycle; closed-cycle rejection must not mask overflow.
        observer
            .boundary("before_start_snapshot", json!({"epoch":1,"cycle":3}))
            .expect("second begin");
        observer.sequence = MAX_EVENTS;
        observer.apply(|state| state.opening(context(Uuid::new_v4(), 3), Some(Uuid::new_v4())));
        assert!(observer.errors.contains("overflow"));
    }
    #[test]
    fn worker_inventory_write_failure_and_missing_incarnation_cannot_close() {
        let dir = tempfile::tempdir().expect("temporary evidence directory");
        let mut observer = observer(dir.path());
        observer
            .boundary("before_start_snapshot", json!({"epoch":1,"cycle":2}))
            .expect("begin");
        observer.apply(|state| state.opening(context(Uuid::new_v4(), 2), None));
        assert!(observer.errors.contains("io_or_malformed"));
        std::fs::remove_dir_all(dir.path()).expect("remove evidence destination");
        observer.apply(|state| state.opening(context(Uuid::new_v4(), 2), Some(Uuid::new_v4())));
        assert!(observer.errors.contains("health_io"));
        assert_eq!(
            observer
                .close((1, 2), root())
                .expect_err("sticky recorder failure prevents closure")
                .to_string(),
            "unhealthy"
        );
        assert!(observer.manifests.is_empty());
    }
}
