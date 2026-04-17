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

use super::{DiskAPI, DiskStore};
use crate::disk::endpoint::Endpoint;
use metrics::{counter, gauge};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum RuntimeDriveHealthState {
    Online = 0,
    Suspect = 1,
    Offline = 2,
    Returning = 3,
}

impl RuntimeDriveHealthState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Suspect => "suspect",
            Self::Offline => "offline",
            Self::Returning => "returning",
        }
    }

    pub fn from_u32(value: u32) -> Self {
        match value {
            1 => Self::Suspect,
            2 => Self::Offline,
            3 => Self::Returning,
            _ => Self::Online,
        }
    }

    pub fn is_snapshot_eligible(self) -> bool {
        matches!(self, Self::Online | Self::Suspect | Self::Returning)
    }

    pub fn is_strictly_online(self) -> bool {
        matches!(self, Self::Online)
    }

    pub fn should_probe_for_admin(self) -> bool {
        matches!(self, Self::Online | Self::Returning)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveRecoveryClass {
    ShortOffline,
    MediumOffline,
    LongOffline,
}

impl DriveRecoveryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShortOffline => "short_offline",
            Self::MediumOffline => "medium_offline",
            Self::LongOffline => "long_offline",
        }
    }
}

pub fn get_drive_suspect_failure_threshold() -> u32 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_SUSPECT_FAILURE_THRESHOLD,
        rustfs_config::DEFAULT_DRIVE_SUSPECT_FAILURE_THRESHOLD,
    ) as u32
}

pub fn get_drive_returning_success_threshold() -> u32 {
    rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_RETURNING_SUCCESS_THRESHOLD,
        rustfs_config::DEFAULT_DRIVE_RETURNING_SUCCESS_THRESHOLD,
    ) as u32
}

pub fn get_drive_returning_probe_interval() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_RETURNING_PROBE_INTERVAL_SECS,
        rustfs_config::DEFAULT_DRIVE_RETURNING_PROBE_INTERVAL_SECS,
    ))
}

pub fn get_drive_offline_grace_period() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_OFFLINE_GRACE_PERIOD_SECS,
        rustfs_config::DEFAULT_DRIVE_OFFLINE_GRACE_PERIOD_SECS,
    ))
}

pub fn get_drive_long_offline_threshold() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_DRIVE_LONG_OFFLINE_THRESHOLD_SECS,
        rustfs_config::DEFAULT_DRIVE_LONG_OFFLINE_THRESHOLD_SECS,
    ))
}

pub fn classify_drive_recovery(duration: Duration) -> DriveRecoveryClass {
    if duration <= get_drive_offline_grace_period() {
        DriveRecoveryClass::ShortOffline
    } else if duration >= get_drive_long_offline_threshold() {
        DriveRecoveryClass::LongOffline
    } else {
        DriveRecoveryClass::MediumOffline
    }
}

pub fn record_drive_runtime_state(endpoint: &Endpoint, state: RuntimeDriveHealthState) {
    let endpoint_label = endpoint.to_string();
    let pool_label = endpoint.pool_idx.to_string();
    let set_label = endpoint.set_idx.to_string();
    let disk_label = endpoint.disk_idx.to_string();

    for candidate in [
        RuntimeDriveHealthState::Online,
        RuntimeDriveHealthState::Suspect,
        RuntimeDriveHealthState::Offline,
        RuntimeDriveHealthState::Returning,
    ] {
        gauge!(
            "rustfs_drive_runtime_state",
            "endpoint" => endpoint_label.clone(),
            "pool" => pool_label.clone(),
            "set" => set_label.clone(),
            "disk" => disk_label.clone(),
            "state" => candidate.as_str().to_string()
        )
        .set(if candidate == state { 1.0 } else { 0.0 });
    }
}

pub fn record_drive_state_transition(
    endpoint: &Endpoint,
    from: RuntimeDriveHealthState,
    to: RuntimeDriveHealthState,
    reason: &'static str,
) {
    counter!(
        "rustfs_drive_state_transition_total",
        "endpoint" => endpoint.to_string(),
        "pool" => endpoint.pool_idx.to_string(),
        "set" => endpoint.set_idx.to_string(),
        "disk" => endpoint.disk_idx.to_string(),
        "from" => from.as_str().to_string(),
        "to" => to.as_str().to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

pub fn record_drive_recovery_class(class: DriveRecoveryClass) {
    counter!(
        "rustfs_drive_recovery_class_total",
        "class" => class.as_str().to_string()
    )
    .increment(1);
}

pub fn record_drive_offline_duration(endpoint: &Endpoint, duration: Duration) {
    gauge!(
        "rustfs_drive_offline_duration_seconds",
        "endpoint" => endpoint.to_string(),
        "pool" => endpoint.pool_idx.to_string(),
        "set" => endpoint.set_idx.to_string(),
        "disk" => endpoint.disk_idx.to_string()
    )
    .set(duration.as_secs_f64());
}

#[derive(Debug, Clone, Default)]
pub struct DriveMembershipSnapshot {
    pub online: Vec<DiskStore>,
    pub suspect: Vec<DiskStore>,
    pub returning: Vec<DiskStore>,
    pub offline: Vec<DiskStore>,
}

impl DriveMembershipSnapshot {
    pub fn from_optional_disks(disks: &[Option<DiskStore>]) -> Self {
        let mut snapshot = Self::default();

        for disk in disks.iter().flatten() {
            match disk.runtime_state() {
                RuntimeDriveHealthState::Online => snapshot.online.push(disk.clone()),
                RuntimeDriveHealthState::Suspect => snapshot.suspect.push(disk.clone()),
                RuntimeDriveHealthState::Returning => snapshot.returning.push(disk.clone()),
                RuntimeDriveHealthState::Offline => snapshot.offline.push(disk.clone()),
            }
        }

        snapshot
    }

    pub fn scanner_heal_candidates(&self) -> Vec<DiskStore> {
        let mut disks = Vec::with_capacity(self.online.len() + self.suspect.len() + self.returning.len());
        disks.extend(self.online.iter().cloned());
        disks.extend(self.suspect.iter().cloned());
        disks.extend(self.returning.iter().cloned());
        disks
    }

    pub fn strict_online_candidates(&self) -> Vec<DiskStore> {
        self.online.clone()
    }

    pub fn strict_online_local_candidates(&self) -> Vec<DiskStore> {
        self.online.iter().filter(|disk| disk.is_local()).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_drive_health_state_snapshot_eligibility_matches_membership_policy() {
        assert!(RuntimeDriveHealthState::Online.is_snapshot_eligible());
        assert!(RuntimeDriveHealthState::Suspect.is_snapshot_eligible());
        assert!(RuntimeDriveHealthState::Returning.is_snapshot_eligible());
        assert!(!RuntimeDriveHealthState::Offline.is_snapshot_eligible());
    }
}
