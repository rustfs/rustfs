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

//! Startup health check of the RDMA devices a NIXL agent may use.
//!
//! UCX keeps using a device whose link is down or flapping, and a transfer
//! striped over such a lane fails; NIXL then drops the whole peer. The check
//! reads sysfs (`/sys/class/infiniband/<dev>/ports/1/...`) and leaves out a
//! device that
//!
//! 1. is missing, or whose port is not `ACTIVE` with physical state `LinkUp`;
//! 2. runs below the best rate among the candidates of its link layer (a
//!    degraded 400G port among 800G rails);
//! 3. has lost its link more than `max_link_drops` times more often since boot
//!    than the quietest device of its link layer (a rail that flaps every few
//!    minutes rarely drops inside a short watch, but its history shows it;
//!    comparing with the quietest peer ignores events that hit every rail,
//!    such as a switch reboot);
//! 4. loses its link (`counters/link_downed` rises) during the watch window.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Where the kernel exposes RDMA devices.
pub const DEFAULT_SYSFS_ROOT: &str = "/sys/class/infiniband";

/// The outcome: devices to use, and devices left out with the reason.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeviceCheck {
    pub kept: Vec<String>,
    pub excluded: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct PortState {
    active: bool,
    link_up: bool,
    rate_gbps: u64,
    link_layer: String,
    link_downed: Option<u64>,
}

fn read_trimmed(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|value| value.trim().to_owned())
}

fn read_port(root: &Path, device: &str) -> Option<PortState> {
    let port = root.join(device).join("ports").join("1");
    let state = read_trimmed(&port.join("state"))?;
    let phys_state = read_trimmed(&port.join("phys_state")).unwrap_or_default();
    let rate_gbps = read_trimmed(&port.join("rate"))
        .and_then(|rate| rate.split_whitespace().next().and_then(|value| value.parse::<f64>().ok()))
        .map(|rate| rate as u64)
        .unwrap_or(0);
    Some(PortState {
        active: state.contains("ACTIVE"),
        link_up: phys_state.contains("LinkUp"),
        rate_gbps,
        link_layer: read_trimmed(&port.join("link_layer")).unwrap_or_default(),
        link_downed: read_trimmed(&port.join("counters").join("link_downed")).and_then(|value| value.parse().ok()),
    })
}

/// Every RDMA device under `root`, sorted.
fn all_devices(root: &Path) -> Vec<String> {
    let mut devices: Vec<String> = std::fs::read_dir(root)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();
    devices.sort();
    devices
}

/// Checks `candidates` (every device under `root` when empty): link history
/// against `max_link_drops` (0 skips that test), then a watch of the link
/// counters for `flap_window` (no watch when zero). Blocks for the window.
pub fn check_devices(root: &Path, candidates: &[String], max_link_drops: u64, flap_window: Duration) -> DeviceCheck {
    let candidates = if candidates.is_empty() {
        all_devices(root)
    } else {
        candidates.to_vec()
    };
    let mut check = DeviceCheck::default();
    let mut healthy: Vec<(String, PortState)> = Vec::new();
    for device in candidates {
        match read_port(root, &device) {
            None => check.excluded.push((device, "missing".to_owned())),
            Some(port) if !port.active => check.excluded.push((device, "port not active".to_owned())),
            Some(port) if !port.link_up => check.excluded.push((device, "physical link not up".to_owned())),
            Some(port) => healthy.push((device, port)),
        }
    }

    let mut best_rate: HashMap<String, u64> = HashMap::new();
    for (_, port) in &healthy {
        let best = best_rate.entry(port.link_layer.clone()).or_default();
        *best = (*best).max(port.rate_gbps);
    }
    let mut rated: Vec<(String, PortState)> = Vec::new();
    for (device, port) in healthy {
        let best = best_rate.get(&port.link_layer).copied().unwrap_or(0);
        if port.rate_gbps < best {
            check
                .excluded
                .push((device, format!("rate {} Gb/s below {} Gb/s of its peers", port.rate_gbps, best)));
        } else {
            rated.push((device, port));
        }
    }

    if max_link_drops > 0 {
        let mut quietest: HashMap<String, u64> = HashMap::new();
        for (_, port) in &rated {
            if let Some(drops) = port.link_downed {
                let entry = quietest.entry(port.link_layer.clone()).or_insert(drops);
                *entry = (*entry).min(drops);
            }
        }
        let mut steady = Vec::with_capacity(rated.len());
        for (device, port) in rated {
            match (port.link_downed, quietest.get(&port.link_layer)) {
                (Some(drops), Some(&floor)) if drops - floor > max_link_drops => check
                    .excluded
                    .push((device, format!("link dropped {drops} times since boot, {floor} on its quietest peer"))),
                _ => steady.push((device, port)),
            }
        }
        rated = steady;
    }

    if !flap_window.is_zero() && !rated.is_empty() {
        std::thread::sleep(flap_window);
    }
    for (device, before) in rated {
        if flap_window.is_zero() {
            check.kept.push(device);
            continue;
        }
        match read_port(root, &device) {
            None => check.excluded.push((device, "disappeared during the link watch".to_owned())),
            Some(after) if !after.active || !after.link_up => check
                .excluded
                .push((device, "link went down during the link watch".to_owned())),
            Some(after) => match (before.link_downed, after.link_downed) {
                (Some(start), Some(end)) if end > start => check.excluded.push((
                    device,
                    format!(
                        "link dropped {} time(s) during the {} ms link watch",
                        end - start,
                        flap_window.as_millis()
                    ),
                )),
                _ => check.kept.push(device),
            },
        }
    }
    check
}

/// `check_devices` on the kernel's sysfs tree.
pub fn check_system_devices(candidates: &[String], max_link_drops: u64, flap_window: Duration) -> DeviceCheck {
    check_devices(&PathBuf::from(DEFAULT_SYSFS_ROOT), candidates, max_link_drops, flap_window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct FakeSysfs(PathBuf);

    impl FakeSysfs {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "rustfs-nixl-devices-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).expect("fake sysfs root");
            Self(root)
        }

        fn device(&self, name: &str, state: &str, phys: &str, rate: &str, layer: &str, downed: u64) {
            let port = self.0.join(name).join("ports").join("1");
            std::fs::create_dir_all(port.join("counters")).expect("port dir");
            std::fs::write(port.join("state"), format!("{state}\n")).expect("state");
            std::fs::write(port.join("phys_state"), format!("{phys}\n")).expect("phys_state");
            std::fs::write(port.join("rate"), format!("{rate}\n")).expect("rate");
            std::fs::write(port.join("link_layer"), format!("{layer}\n")).expect("link_layer");
            self.set_downed(name, downed);
        }

        fn set_downed(&self, name: &str, downed: u64) {
            let path = self.0.join(name).join("ports").join("1").join("counters").join("link_downed");
            std::fs::write(path, format!("{downed}\n")).expect("link_downed");
        }
    }

    impl Drop for FakeSysfs {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    const UP: (&str, &str) = ("4: ACTIVE", "5: LinkUp");

    #[test]
    fn keeps_healthy_rails_and_names_why_others_are_left_out() {
        let fs = FakeSysfs::new();
        fs.device("mlx5_0", UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 0);
        fs.device("mlx5_5", "1: DOWN", "3: Disabled", "10 Gb/sec (4X SDR)", "InfiniBand", 4);
        fs.device("mlx5_7", "2: INIT", UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 0);
        fs.device("mlx5_8", UP.0, UP.1, "400 Gb/sec (4X NDR)", "InfiniBand", 0);
        fs.device("mlx5_6", UP.0, UP.1, "400 Gb/sec (4X NDR)", "Ethernet", 0);
        let wanted: Vec<String> = ["mlx5_0", "mlx5_5", "mlx5_7", "mlx5_8", "mlx5_6", "mlx5_99"]
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let check = check_devices(&fs.0, &wanted, 0, Duration::ZERO);
        assert_eq!(check.kept, vec!["mlx5_0".to_owned(), "mlx5_6".to_owned()]);
        let reasons: HashMap<_, _> = check.excluded.into_iter().collect();
        assert_eq!(reasons["mlx5_5"], "port not active");
        assert_eq!(reasons["mlx5_7"], "port not active");
        assert!(reasons["mlx5_8"].starts_with("rate 400 Gb/s below 800"), "{}", reasons["mlx5_8"]);
        assert_eq!(reasons["mlx5_99"], "missing");
        assert!(!reasons.contains_key("mlx5_6"), "a RoCE port is not compared with InfiniBand rates");
    }

    #[test]
    fn an_empty_list_means_every_device() {
        let fs = FakeSysfs::new();
        fs.device("mlx5_1", UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 0);
        fs.device("mlx5_0", UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 0);
        let check = check_devices(&fs.0, &[], 0, Duration::ZERO);
        assert_eq!(check.kept, vec!["mlx5_0".to_owned(), "mlx5_1".to_owned()]);
        assert!(check.excluded.is_empty());
    }

    #[test]
    fn a_link_that_drops_during_the_watch_is_left_out() {
        let fs = FakeSysfs::new();
        fs.device("mlx5_8", UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 52);
        fs.device("mlx5_9", UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 22);
        let root = fs.0.clone();
        let flapper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let path = root
                .join("mlx5_8")
                .join("ports")
                .join("1")
                .join("counters")
                .join("link_downed");
            std::fs::write(path, "53\n").expect("bump link_downed");
        });
        let wanted = vec!["mlx5_8".to_owned(), "mlx5_9".to_owned()];
        let check = check_devices(&fs.0, &wanted, 0, Duration::from_millis(300));
        flapper.join().expect("flapper");
        assert_eq!(check.kept, vec!["mlx5_9".to_owned()], "an old link_downed count alone does not exclude");
        assert_eq!(check.excluded.len(), 1);
        assert_eq!(check.excluded[0].0, "mlx5_8");
        assert!(check.excluded[0].1.starts_with("link dropped 1 time(s)"), "{}", check.excluded[0].1);
    }

    #[test]
    fn a_rail_with_a_long_drop_history_is_left_out() {
        let fs = FakeSysfs::new();
        for (name, drops) in [
            ("mlx5_0", 0),
            ("mlx5_7", 0),
            ("mlx5_8", 68),
            ("mlx5_9", 22),
            ("mlx5_10", 4),
            ("mlx5_13", 4),
        ] {
            fs.device(name, UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", drops);
        }
        let wanted: Vec<String> = ["mlx5_0", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_13"]
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let check = check_devices(&fs.0, &wanted, 16, Duration::ZERO);
        assert_eq!(check.kept, vec!["mlx5_0", "mlx5_7", "mlx5_10", "mlx5_13"]);
        let reasons: HashMap<_, _> = check.excluded.into_iter().collect();
        assert_eq!(reasons["mlx5_8"], "link dropped 68 times since boot, 0 on its quietest peer");
        assert_eq!(reasons["mlx5_9"], "link dropped 22 times since boot, 0 on its quietest peer");

        // Events that hit every rail (a switch reboot) exclude nothing.
        let fs = FakeSysfs::new();
        for name in ["mlx5_0", "mlx5_7"] {
            fs.device(name, UP.0, UP.1, "800 Gb/sec (4X XDR)", "InfiniBand", 40);
        }
        let wanted = vec!["mlx5_0".to_owned(), "mlx5_7".to_owned()];
        assert_eq!(check_devices(&fs.0, &wanted, 16, Duration::ZERO).kept, wanted);
    }
}
