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

use std::time::{Duration, Instant};

use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::{info, warn};

use crate::{LockApi, Locker, lock_args::LockArgs};

const DRW_MUTEX_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const LOCK_RETRY_MIN_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub struct DRWMutex {
    owner: String,
    names: Vec<String>,
    write_locks: Vec<String>,
    read_locks: Vec<String>,
    cancel_refresh_sender: Option<Sender<bool>>,
    // rng: ThreadRng,
    lockers: Vec<LockApi>,
    refresh_interval: Duration,
    lock_retry_min_interval: Duration,
}

#[derive(Debug, Default, Clone)]
pub struct Granted {
    index: usize,
    lock_uid: String,
}

impl Granted {
    fn is_locked(&self) -> bool {
        is_locked(&self.lock_uid)
    }
}

fn is_locked(uid: &str) -> bool {
    !uid.is_empty()
}

#[derive(Debug, Clone)]
pub struct Options {
    pub timeout: Duration,
    pub retry_interval: Duration,
}

impl DRWMutex {
    pub fn new(owner: String, names: Vec<String>, lockers: Vec<LockApi>) -> Self {
        let mut names = names;
        names.sort();
        Self {
            owner,
            names,
            write_locks: vec![String::new(); lockers.len()],
            read_locks: vec![String::new(); lockers.len()],
            cancel_refresh_sender: None,
            // rng: rand::thread_rng(),
            lockers,
            refresh_interval: DRW_MUTEX_REFRESH_INTERVAL,
            lock_retry_min_interval: LOCK_RETRY_MIN_INTERVAL,
        }
    }

    fn is_locked(&self) -> bool {
        self.write_locks.iter().any(|w_lock| is_locked(w_lock))
    }

    fn is_r_locked(&self) -> bool {
        self.read_locks.iter().any(|r_lock| is_locked(r_lock))
    }
}

impl DRWMutex {
    pub async fn lock(&mut self, id: &String, source: &String) {
        let is_read_lock = false;
        let opts = Options {
            timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(50),
        };
        self.lock_blocking(id, source, is_read_lock, &opts).await;
    }

    pub async fn get_lock(&mut self, id: &String, source: &String, opts: &Options) -> bool {
        let is_read_lock = false;
        self.lock_blocking(id, source, is_read_lock, opts).await
    }

    pub async fn r_lock(&mut self, id: &String, source: &String) {
        let is_read_lock = true;
        let opts = Options {
            timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(50),
        };
        self.lock_blocking(id, source, is_read_lock, &opts).await;
    }

    pub async fn get_r_lock(&mut self, id: &String, source: &String, opts: &Options) -> bool {
        let is_read_lock = true;
        self.lock_blocking(id, source, is_read_lock, opts).await
    }

    pub async fn lock_blocking(&mut self, id: &String, source: &String, is_read_lock: bool, opts: &Options) -> bool {
        let locker_len = self.lockers.len();

        // Handle edge case: no lockers available
        if locker_len == 0 {
            return false;
        }

        let mut tolerance = locker_len / 2;
        let mut quorum = locker_len - tolerance;
        if !is_read_lock {
            // In situations for write locks, as a special case
            // to avoid split brains we make sure to acquire
            // quorum + 1 when tolerance is exactly half of the
            // total locker clients.
            if quorum == tolerance {
                quorum += 1;
            }
        }
        info!(
            "lockBlocking {}/{} for {:?}: lockType readLock({}), additional opts: {:?}, quorum: {}, tolerance: {}, lockClients: {}\n",
            id, source, self.names, is_read_lock, opts, quorum, tolerance, locker_len
        );

        // Recalculate tolerance after potential quorum adjustment
        // Use saturating_sub to prevent underflow
        tolerance = locker_len.saturating_sub(quorum);
        let mut attempt = 0;
        let mut locks = vec!["".to_string(); self.lockers.len()];

        loop {
            if self.inner_lock(&mut locks, id, source, is_read_lock, tolerance, quorum).await {
                if is_read_lock {
                    self.read_locks = locks;
                } else {
                    self.write_locks = locks;
                }

                info!("lock_blocking {}/{} for {:?}: granted", id, source, self.names);

                return true;
            }

            attempt += 1;
            if attempt >= 10 {
                break;
            }
            sleep(opts.retry_interval).await;
        }

        false
    }

    async fn inner_lock(
        &mut self,
        locks: &mut [String],
        id: &String,
        source: &String,
        is_read_lock: bool,
        tolerance: usize,
        quorum: usize,
    ) -> bool {
        locks.iter_mut().for_each(|lock| *lock = "".to_string());

        let mut granteds = Vec::with_capacity(self.lockers.len());
        let args = LockArgs {
            uid: id.to_string(),
            resources: self.names.clone(),
            owner: self.owner.clone(),
            source: source.to_string(),
            quorum,
        };

        for (index, locker) in self.lockers.iter_mut().enumerate() {
            let mut granted = Granted {
                index,
                ..Default::default()
            };

            if is_read_lock {
                match locker.rlock(&args).await {
                    Ok(locked) => {
                        if locked {
                            granted.lock_uid = id.to_string();
                        }
                    }
                    Err(err) => {
                        warn!("Unable to call RLock failed with {} for {} at {:?}", err, args, locker);
                    }
                }
            } else {
                match locker.lock(&args).await {
                    Ok(locked) => {
                        if locked {
                            granted.lock_uid = id.to_string();
                        }
                    }
                    Err(err) => {
                        warn!("Unable to call Lock failed with {} for {} at {:?}", err, args, locker);
                    }
                }
            }

            granteds.push(granted);
        }

        granteds.iter().for_each(|granted| {
            locks[granted.index] = granted.lock_uid.clone();
        });

        let quorum_locked = check_quorum_locked(locks, quorum);
        if !quorum_locked {
            info!("Unable to acquire lock in quorum, {}", args);
            if !self.release_all(tolerance, locks, is_read_lock).await {
                info!("Unable to release acquired locks, these locks will expire automatically {}", args);
            }
        }

        quorum_locked
    }

    pub async fn un_lock(&mut self) {
        if self.write_locks.is_empty() || !self.is_locked() {
            warn!("Trying to un_lock() while no lock() is active, write_locks: {:?}", self.write_locks)
        }

        let tolerance = self.lockers.len() / 2;
        let is_read_lock = false;
        let mut locks = std::mem::take(&mut self.write_locks);
        let start = Instant::now();
        loop {
            if self.release_all(tolerance, &mut locks, is_read_lock).await {
                return;
            }

            sleep(self.lock_retry_min_interval).await;
            if Instant::now().duration_since(start) > Duration::from_secs(30) {
                return;
            }
        }
    }

    pub async fn un_r_lock(&mut self) {
        if self.read_locks.is_empty() || !self.is_r_locked() {
            warn!("Trying to un_r_lock() while no r_lock() is active, read_locks: {:?}", self.read_locks)
        }

        let tolerance = self.lockers.len() / 2;
        let is_read_lock = true;
        let mut locks = std::mem::take(&mut self.read_locks);
        let start = Instant::now();
        loop {
            if self.release_all(tolerance, &mut locks, is_read_lock).await {
                return;
            }

            sleep(self.lock_retry_min_interval).await;
            if Instant::now().duration_since(start) > Duration::from_secs(30) {
                return;
            }
        }
    }

    async fn release_all(&mut self, tolerance: usize, locks: &mut [String], is_read_lock: bool) -> bool {
        for (index, locker) in self.lockers.iter_mut().enumerate() {
            if send_release(locker, &locks[index], &self.owner, &self.names, is_read_lock).await {
                locks[index] = "".to_string();
            }
        }

        !check_failed_unlocks(locks, tolerance)
    }
}

// async fn start_continuous_lock_refresh(lockers: &Vec<&mut LockApi>, id: &String, source: &String, quorum: usize, refresh_interval: Duration, mut cancel_refresh_receiver: Receiver<bool>) {
//     let uid = id.to_string();
//     tokio::spawn(async move {
//         let mut ticker = interval(refresh_interval);
//         let args = LockArgs {
//             uid,
//             ..Default::default()
//         };

//         loop {
//             select! {
//                 _ = ticker.tick() => {
//                     for (index, locker) in lockers.iter().enumerate() {

//                     }
//                 },
//                 _ = cancel_refresh_receiver.recv() => {
//                     return;
//                 }
//             }
//         }
//     });
// }

fn check_failed_unlocks(locks: &[String], tolerance: usize) -> bool {
    let mut un_locks_failed = 0;
    locks.iter().for_each(|lock| {
        if is_locked(lock) {
            un_locks_failed += 1;
        }
    });

    // Handle edge case: if tolerance is greater than or equal to locks.len(),
    // we can tolerate all failures, so return false (no critical failure)
    if tolerance >= locks.len() {
        return false;
    }

    // Special case: when locks.len() - tolerance == tolerance (i.e., locks.len() == 2 * tolerance)
    // This happens when we have an even number of lockers and tolerance is exactly half
    if locks.len() - tolerance == tolerance {
        return un_locks_failed >= tolerance;
    }

    // Normal case: failure if more than tolerance unlocks failed
    un_locks_failed > tolerance
}

async fn send_release(locker: &mut LockApi, uid: &String, owner: &str, names: &[String], is_read_lock: bool) -> bool {
    if uid.is_empty() {
        return false;
    }

    let args = LockArgs {
        uid: uid.to_string(),
        owner: owner.to_owned(),
        resources: names.to_owned(),
        ..Default::default()
    };

    if is_read_lock {
        match locker.runlock(&args).await {
            Ok(locked) => {
                if !locked {
                    warn!("Unable to release runlock, args: {}", args);
                    return false;
                }
            }
            Err(err) => {
                warn!("Unable to call RLock failed with {} for {} at {:?}", err, args, locker);
                return false;
            }
        }
    } else {
        match locker.unlock(&args).await {
            Ok(locked) => {
                if !locked {
                    warn!("Unable to release unlock, args: {}", args);
                    return false;
                }
            }
            Err(err) => {
                warn!("Unable to call Lock failed with {} for {} at {:?}", err, args, locker);
                return false;
            }
        }
    }

    true
}

fn check_quorum_locked(locks: &[String], quorum: usize) -> bool {
    let mut count = 0;
    locks.iter().for_each(|lock| {
        if is_locked(lock) {
            count += 1;
        }
    });

    count >= quorum
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_locker::LocalLocker;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::io::{Error, Result};
    use std::sync::{Arc, Mutex};

    // Mock locker for testing
    #[derive(Debug, Clone)]
    struct MockLocker {
        id: String,
        state: Arc<Mutex<MockLockerState>>,
    }

    #[derive(Debug, Default)]
    struct MockLockerState {
        locks: HashMap<String, String>,      // uid -> owner
        read_locks: HashMap<String, String>, // uid -> owner
        should_fail: bool,
        is_online: bool,
    }

    impl MockLocker {
        fn new(id: String) -> Self {
            Self {
                id,
                state: Arc::new(Mutex::new(MockLockerState {
                    is_online: true,
                    ..Default::default()
                })),
            }
        }

        fn set_should_fail(&self, should_fail: bool) {
            self.state.lock().unwrap().should_fail = should_fail;
        }

        fn set_online(&self, online: bool) {
            self.state.lock().unwrap().is_online = online;
        }

        fn get_lock_count(&self) -> usize {
            self.state.lock().unwrap().locks.len()
        }

        fn get_read_lock_count(&self) -> usize {
            self.state.lock().unwrap().read_locks.len()
        }

        fn has_lock(&self, uid: &str) -> bool {
            self.state.lock().unwrap().locks.contains_key(uid)
        }

        fn has_read_lock(&self, uid: &str) -> bool {
            self.state.lock().unwrap().read_locks.contains_key(uid)
        }
    }

    #[async_trait]
    impl Locker for MockLocker {
        async fn lock(&mut self, args: &LockArgs) -> Result<bool> {
            let mut state = self.state.lock().unwrap();
            if state.should_fail {
                return Err(Error::other("Mock lock failure"));
            }
            if !state.is_online {
                return Err(Error::other("Mock locker offline"));
            }

            // Check if already locked
            if state.locks.contains_key(&args.uid) {
                return Ok(false);
            }

            state.locks.insert(args.uid.clone(), args.owner.clone());
            Ok(true)
        }

        async fn unlock(&mut self, args: &LockArgs) -> Result<bool> {
            let mut state = self.state.lock().unwrap();
            if state.should_fail {
                return Err(Error::other("Mock unlock failure"));
            }

            Ok(state.locks.remove(&args.uid).is_some())
        }

        async fn rlock(&mut self, args: &LockArgs) -> Result<bool> {
            let mut state = self.state.lock().unwrap();
            if state.should_fail {
                return Err(Error::other("Mock rlock failure"));
            }
            if !state.is_online {
                return Err(Error::other("Mock locker offline"));
            }

            // Check if write lock exists
            if state.locks.contains_key(&args.uid) {
                return Ok(false);
            }

            state.read_locks.insert(args.uid.clone(), args.owner.clone());
            Ok(true)
        }

        async fn runlock(&mut self, args: &LockArgs) -> Result<bool> {
            let mut state = self.state.lock().unwrap();
            if state.should_fail {
                return Err(Error::other("Mock runlock failure"));
            }

            Ok(state.read_locks.remove(&args.uid).is_some())
        }

        async fn refresh(&mut self, _args: &LockArgs) -> Result<bool> {
            let state = self.state.lock().unwrap();
            if state.should_fail {
                return Err(Error::other("Mock refresh failure"));
            }
            Ok(true)
        }

        async fn force_unlock(&mut self, args: &LockArgs) -> Result<bool> {
            let mut state = self.state.lock().unwrap();
            let removed_lock = state.locks.remove(&args.uid).is_some();
            let removed_read_lock = state.read_locks.remove(&args.uid).is_some();
            Ok(removed_lock || removed_read_lock)
        }

        async fn close(&self) {}

        async fn is_online(&self) -> bool {
            self.state.lock().unwrap().is_online
        }

        async fn is_local(&self) -> bool {
            true
        }
    }

    fn create_mock_lockers(count: usize) -> Vec<LockApi> {
        // For testing, we'll use Local lockers which use the global local server
        (0..count).map(|_| LockApi::Local).collect()
    }

    #[test]
    fn test_drw_mutex_new() {
        let names = vec!["resource1".to_string(), "resource2".to_string()];
        let lockers = create_mock_lockers(3);
        let mutex = DRWMutex::new("owner1".to_string(), names.clone(), lockers);

        assert_eq!(mutex.owner, "owner1");
        assert_eq!(mutex.names.len(), 2);
        assert_eq!(mutex.lockers.len(), 3);
        assert_eq!(mutex.write_locks.len(), 3);
        assert_eq!(mutex.read_locks.len(), 3);
        assert_eq!(mutex.refresh_interval, DRW_MUTEX_REFRESH_INTERVAL);
        assert_eq!(mutex.lock_retry_min_interval, LOCK_RETRY_MIN_INTERVAL);

        // Names should be sorted
        let mut expected_names = names;
        expected_names.sort();
        assert_eq!(mutex.names, expected_names);
    }

    #[test]
    fn test_drw_mutex_new_empty_names() {
        let names = vec![];
        let lockers = create_mock_lockers(1);
        let mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        assert_eq!(mutex.names.len(), 0);
        assert_eq!(mutex.lockers.len(), 1);
    }

    #[test]
    fn test_drw_mutex_new_single_locker() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1);
        let mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        assert_eq!(mutex.lockers.len(), 1);
        assert_eq!(mutex.write_locks.len(), 1);
        assert_eq!(mutex.read_locks.len(), 1);
    }

    #[test]
    fn test_is_locked_function() {
        assert!(!is_locked(""));
        assert!(is_locked("some-uid"));
        assert!(is_locked("any-non-empty-string"));
    }

    #[test]
    fn test_granted_is_locked() {
        let granted_empty = Granted {
            index: 0,
            lock_uid: "".to_string(),
        };
        assert!(!granted_empty.is_locked());

        let granted_locked = Granted {
            index: 1,
            lock_uid: "test-uid".to_string(),
        };
        assert!(granted_locked.is_locked());
    }

    #[test]
    fn test_drw_mutex_is_locked() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(2);
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        // Initially not locked
        assert!(!mutex.is_locked());
        assert!(!mutex.is_r_locked());

        // Set write locks
        mutex.write_locks[0] = "test-uid".to_string();
        assert!(mutex.is_locked());
        assert!(!mutex.is_r_locked());

        // Clear write locks, set read locks
        mutex.write_locks[0] = "".to_string();
        mutex.read_locks[1] = "read-uid".to_string();
        assert!(!mutex.is_locked());
        assert!(mutex.is_r_locked());
    }

    #[test]
    fn test_options_debug() {
        let opts = Options {
            timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(100),
        };
        let debug_str = format!("{opts:?}");
        assert!(debug_str.contains("timeout"));
        assert!(debug_str.contains("retry_interval"));
    }

    #[test]
    fn test_check_quorum_locked() {
        // Test with empty locks
        assert!(!check_quorum_locked(&[], 1));

        // Test with all empty locks
        let locks = vec!["".to_string(), "".to_string(), "".to_string()];
        assert!(!check_quorum_locked(&locks, 1));
        assert!(!check_quorum_locked(&locks, 2));

        // Test with some locks
        let locks = vec!["uid1".to_string(), "".to_string(), "uid3".to_string()];
        assert!(check_quorum_locked(&locks, 1));
        assert!(check_quorum_locked(&locks, 2));
        assert!(!check_quorum_locked(&locks, 3));

        // Test with all locks
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "uid3".to_string()];
        assert!(check_quorum_locked(&locks, 1));
        assert!(check_quorum_locked(&locks, 2));
        assert!(check_quorum_locked(&locks, 3));
        assert!(!check_quorum_locked(&locks, 4));
    }

    #[test]
    fn test_check_failed_unlocks() {
        // Test with empty locks
        assert!(!check_failed_unlocks(&[], 0)); // tolerance >= locks.len(), so no critical failure
        assert!(!check_failed_unlocks(&[], 1)); // tolerance >= locks.len(), so no critical failure

        // Test with all unlocked
        let locks = vec!["".to_string(), "".to_string(), "".to_string()];
        assert!(!check_failed_unlocks(&locks, 1)); // 0 failed <= tolerance 1
        assert!(!check_failed_unlocks(&locks, 2)); // 0 failed <= tolerance 2

        // Test with some failed unlocks
        let locks = vec!["uid1".to_string(), "".to_string(), "uid3".to_string()];
        assert!(check_failed_unlocks(&locks, 1)); // 2 failed > tolerance 1
        assert!(!check_failed_unlocks(&locks, 2)); // 2 failed <= tolerance 2

        // Test special case: locks.len() - tolerance == tolerance
        // This means locks.len() == 2 * tolerance
        let locks = vec!["uid1".to_string(), "uid2".to_string()]; // len = 2
        let tolerance = 1; // 2 - 1 == 1
        assert!(check_failed_unlocks(&locks, tolerance)); // 2 failed >= tolerance 1

        let locks = vec!["".to_string(), "uid2".to_string()]; // len = 2, 1 failed
        assert!(check_failed_unlocks(&locks, tolerance)); // 1 failed >= tolerance 1

        let locks = vec!["".to_string(), "".to_string()]; // len = 2, 0 failed
        assert!(!check_failed_unlocks(&locks, tolerance)); // 0 failed < tolerance 1
    }

    #[test]
    fn test_check_failed_unlocks_edge_cases() {
        // Test with zero tolerance
        let locks = vec!["uid1".to_string()];
        assert!(check_failed_unlocks(&locks, 0)); // 1 failed > tolerance 0

        // Test with tolerance equal to lock count
        let locks = vec!["uid1".to_string(), "uid2".to_string()];
        assert!(!check_failed_unlocks(&locks, 2)); // 2 failed <= tolerance 2

        // Test with tolerance greater than lock count
        let locks = vec!["uid1".to_string()];
        assert!(!check_failed_unlocks(&locks, 5)); // 1 failed <= tolerance 5
    }

    // Async tests using the local locker infrastructure
    #[tokio::test]
    async fn test_drw_mutex_lock_basic_functionality() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1); // Single locker for simplicity
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id = "test-lock-id".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(10),
        };

        // Test get_lock (result depends on local locker state)
        let _result = mutex.get_lock(&id, &source, &opts).await;
        // Just ensure the method doesn't panic and returns a boolean
        // assert!(result || !result); // This is always true, so removed

        // If lock was acquired, test unlock
        if _result {
            assert!(mutex.is_locked(), "Mutex should be in locked state");
            mutex.un_lock().await;
            assert!(!mutex.is_locked(), "Mutex should be unlocked after un_lock");
        }
    }

    #[tokio::test]
    async fn test_drw_mutex_rlock_basic_functionality() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1); // Single locker for simplicity
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id = "test-rlock-id".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(10),
        };

        // Test get_r_lock (result depends on local locker state)
        let _result = mutex.get_r_lock(&id, &source, &opts).await;
        // Just ensure the method doesn't panic and returns a boolean
        // assert!(result || !result); // This is always true, so removed

        // If read lock was acquired, test runlock
        if _result {
            assert!(mutex.is_r_locked(), "Mutex should be in read locked state");
            mutex.un_r_lock().await;
            assert!(!mutex.is_r_locked(), "Mutex should be unlocked after un_r_lock");
        }
    }

    #[tokio::test]
    async fn test_drw_mutex_lock_with_multiple_lockers() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(3); // 3 lockers, need quorum of 2
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id = "test-lock-id".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(10),
        };

        // With 3 local lockers, the quorum calculation should be:
        // tolerance = 3 / 2 = 1
        // quorum = 3 - 1 = 2
        // Since it's a write lock and quorum != tolerance, quorum stays 2
        // The result depends on the actual locker implementation
        let _result = mutex.get_lock(&id, &source, &opts).await;
        // We don't assert success/failure here since it depends on the local locker state
        // Just ensure the method doesn't panic and returns a boolean
        // assert!(result || !result); // This is always true, so removed
    }

    #[tokio::test]
    async fn test_drw_mutex_unlock_without_lock() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1);
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        // Try to unlock without having a lock - should not panic
        mutex.un_lock().await;
        assert!(!mutex.is_locked());

        // Try to unlock read lock without having one - should not panic
        mutex.un_r_lock().await;
        assert!(!mutex.is_r_locked());
    }

    #[tokio::test]
    async fn test_drw_mutex_multiple_resources() {
        let names = vec!["resource1".to_string(), "resource2".to_string(), "resource3".to_string()];
        let lockers = create_mock_lockers(1);
        let mut mutex = DRWMutex::new("owner1".to_string(), names.clone(), lockers);

        // Names should be sorted
        let mut expected_names = names;
        expected_names.sort();
        assert_eq!(mutex.names, expected_names);

        let id = "test-lock-id".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(10),
        };

        let _result = mutex.get_lock(&id, &source, &opts).await;
        // The result depends on the actual locker implementation
        // Just ensure the method doesn't panic and returns a boolean
        // assert!(result || !result); // This is always true, so removed
    }

    #[tokio::test]
    async fn test_drw_mutex_concurrent_read_locks() {
        // Clear global state before test to avoid interference from other tests
        {
            let mut global_server = crate::GLOBAL_LOCAL_SERVER.write().await;
            *global_server = LocalLocker::new();
        }

        // Use a single mutex with one resource for simplicity
        let names = vec!["test-resource".to_string()];
        let lockers = create_mock_lockers(1);
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id1 = "test-rlock-id1".to_string();
        let id2 = "test-rlock-id2".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(50),
        };

        // First acquire a read lock
        let result1 = mutex.get_r_lock(&id1, &source, &opts).await;
        assert!(result1, "First read lock should succeed");

        // Release the first read lock
        mutex.un_r_lock().await;

        // Then acquire another read lock with different ID - this should succeed
        let result2 = mutex.get_r_lock(&id2, &source, &opts).await;
        assert!(result2, "Second read lock should succeed after first is released");

        // Clean up
        mutex.un_r_lock().await;
    }

    #[tokio::test]
    async fn test_send_release_with_empty_uid() {
        let mut locker = LockApi::Local;
        let result = send_release(&mut locker, &"".to_string(), "owner", &["resource".to_string()], false).await;
        assert!(!result, "send_release should return false for empty uid");
    }

    #[test]
    fn test_drw_mutex_debug() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1);
        let mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let debug_str = format!("{mutex:?}");
        assert!(debug_str.contains("DRWMutex"));
        assert!(debug_str.contains("owner"));
        assert!(debug_str.contains("names"));
    }

    #[test]
    fn test_granted_default() {
        let granted = Granted::default();
        assert_eq!(granted.index, 0);
        assert_eq!(granted.lock_uid, "");
        assert!(!granted.is_locked());
    }

    #[test]
    fn test_granted_clone() {
        let granted = Granted {
            index: 5,
            lock_uid: "test-uid".to_string(),
        };
        let cloned = granted.clone();
        assert_eq!(granted.index, cloned.index);
        assert_eq!(granted.lock_uid, cloned.lock_uid);
    }

    // Test potential bug scenarios
    #[test]
    fn test_potential_bug_check_failed_unlocks_logic() {
        // This test highlights the potentially confusing logic in check_failed_unlocks

        // Case 1: Even number of lockers
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "uid3".to_string(), "uid4".to_string()];
        let tolerance = 2; // locks.len() / 2 = 4 / 2 = 2
        // locks.len() - tolerance = 4 - 2 = 2, which equals tolerance
        // So the special case applies: un_locks_failed >= tolerance

        // All 4 failed unlocks
        assert!(check_failed_unlocks(&locks, tolerance)); // 4 >= 2 = true

        // 2 failed unlocks
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "".to_string(), "".to_string()];
        assert!(check_failed_unlocks(&locks, tolerance)); // 2 >= 2 = true

        // 1 failed unlock
        let locks = vec!["uid1".to_string(), "".to_string(), "".to_string(), "".to_string()];
        assert!(!check_failed_unlocks(&locks, tolerance)); // 1 >= 2 = false

        // Case 2: Odd number of lockers
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "uid3".to_string()];
        let tolerance = 1; // locks.len() / 2 = 3 / 2 = 1
        // locks.len() - tolerance = 3 - 1 = 2, which does NOT equal tolerance (1)
        // So the normal case applies: un_locks_failed > tolerance

        // 3 failed unlocks
        assert!(check_failed_unlocks(&locks, tolerance)); // 3 > 1 = true

        // 2 failed unlocks
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "".to_string()];
        assert!(check_failed_unlocks(&locks, tolerance)); // 2 > 1 = true

        // 1 failed unlock
        let locks = vec!["uid1".to_string(), "".to_string(), "".to_string()];
        assert!(!check_failed_unlocks(&locks, tolerance)); // 1 > 1 = false
    }

    #[test]
    fn test_quorum_calculation_edge_cases() {
        // Test the quorum calculation logic that might have issues

        // For 1 locker: tolerance = 0, quorum = 1
        // Write lock: quorum == tolerance (1 == 0 is false), so quorum stays 1
        // This seems wrong - with 1 locker, we should need that 1 locker

        // For 2 lockers: tolerance = 1, quorum = 1
        // Write lock: quorum == tolerance (1 == 1 is true), so quorum becomes 2
        // This makes sense - we need both lockers for write lock

        // For 3 lockers: tolerance = 1, quorum = 2
        // Write lock: quorum == tolerance (2 == 1 is false), so quorum stays 2

        // For 4 lockers: tolerance = 2, quorum = 2
        // Write lock: quorum == tolerance (2 == 2 is true), so quorum becomes 3

        // The logic seems to be: for write locks, if exactly half the lockers
        // would be tolerance, we need one more to avoid split brain

        // Let's verify this makes sense:
        struct QuorumTest {
            locker_count: usize,
            expected_tolerance: usize,
            expected_write_quorum: usize,
            expected_read_quorum: usize,
        }

        let test_cases = vec![
            QuorumTest {
                locker_count: 1,
                expected_tolerance: 0,
                expected_write_quorum: 1,
                expected_read_quorum: 1,
            },
            QuorumTest {
                locker_count: 2,
                expected_tolerance: 1,
                expected_write_quorum: 2,
                expected_read_quorum: 1,
            },
            QuorumTest {
                locker_count: 3,
                expected_tolerance: 1,
                expected_write_quorum: 2,
                expected_read_quorum: 2,
            },
            QuorumTest {
                locker_count: 4,
                expected_tolerance: 2,
                expected_write_quorum: 3,
                expected_read_quorum: 2,
            },
            QuorumTest {
                locker_count: 5,
                expected_tolerance: 2,
                expected_write_quorum: 3,
                expected_read_quorum: 3,
            },
        ];

        for test_case in test_cases {
            let tolerance = test_case.locker_count / 2;
            let mut write_quorum = test_case.locker_count - tolerance;
            let read_quorum = write_quorum;

            // Apply write lock special case
            if write_quorum == tolerance {
                write_quorum += 1;
            }

            assert_eq!(
                tolerance, test_case.expected_tolerance,
                "Tolerance mismatch for {} lockers",
                test_case.locker_count
            );
            assert_eq!(
                write_quorum, test_case.expected_write_quorum,
                "Write quorum mismatch for {} lockers",
                test_case.locker_count
            );
            assert_eq!(
                read_quorum, test_case.expected_read_quorum,
                "Read quorum mismatch for {} lockers",
                test_case.locker_count
            );
        }
    }

    #[test]
    fn test_potential_integer_overflow() {
        // Test potential issues with tolerance calculation

        // What happens with 0 lockers? This should probably be an error case
        let locker_count = 0;
        let tolerance = locker_count / 2; // 0 / 2 = 0
        let quorum = locker_count - tolerance; // 0 - 0 = 0

        // This would result in quorum = 0, which doesn't make sense
        assert_eq!(tolerance, 0);
        assert_eq!(quorum, 0);

        // The code should probably validate that locker_count > 0
    }

    #[test]
    fn test_drw_mutex_constants() {
        // Test that constants are reasonable
        assert!(DRW_MUTEX_REFRESH_INTERVAL.as_secs() > 0);
        assert!(LOCK_RETRY_MIN_INTERVAL.as_millis() > 0);
        assert!(DRW_MUTEX_REFRESH_INTERVAL > LOCK_RETRY_MIN_INTERVAL);
    }

    #[test]
    fn test_drw_mutex_new_with_unsorted_names() {
        let names = vec!["zebra".to_string(), "alpha".to_string(), "beta".to_string()];
        let lockers = create_mock_lockers(1);
        let mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        // Names should be sorted
        assert_eq!(mutex.names, vec!["alpha", "beta", "zebra"]);
    }

    #[test]
    fn test_drw_mutex_new_with_duplicate_names() {
        let names = vec![
            "resource1".to_string(),
            "resource2".to_string(),
            "resource1".to_string(), // Duplicate
        ];
        let lockers = create_mock_lockers(1);
        let mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        // Should keep duplicates but sort them
        assert_eq!(mutex.names, vec!["resource1", "resource1", "resource2"]);
    }

    #[tokio::test]
    async fn test_drw_mutex_lock_and_rlock_methods() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(1);
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id = "test-id".to_string();
        let source = "test-source".to_string();

        // Test the convenience methods (lock and r_lock)
        // These should not panic and should attempt to acquire locks
        mutex.lock(&id, &source).await;
        // Note: We can't easily test the result since these methods don't return bool

        // Clear any state
        mutex.un_lock().await;

        // Test r_lock
        mutex.r_lock(&id, &source).await;
        mutex.un_r_lock().await;
    }

    #[tokio::test]
    async fn test_drw_mutex_zero_lockers() {
        let names = vec!["resource1".to_string()];
        let lockers = vec![]; // No lockers
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        let id = "test-id".to_string();
        let source = "test-source".to_string();
        let opts = Options {
            timeout: Duration::from_secs(1),
            retry_interval: Duration::from_millis(10),
        };

        // With 0 lockers, quorum calculation:
        // tolerance = 0 / 2 = 0
        // quorum = 0 - 0 = 0
        // This should fail because we can't achieve any quorum
        let _result = mutex.get_lock(&id, &source, &opts).await;
        assert!(!_result, "Should fail with zero lockers");
    }

    #[test]
    fn test_check_quorum_locked_edge_cases() {
        // Test with quorum 0
        let locks = vec!["".to_string()];
        assert!(check_quorum_locked(&locks, 0)); // 0 >= 0

        // Test with quorum larger than locks
        let locks = vec!["uid1".to_string()];
        assert!(!check_quorum_locked(&locks, 5)); // 1 < 5

        // Test with all locks but high quorum
        let locks = vec!["uid1".to_string(), "uid2".to_string(), "uid3".to_string()];
        assert!(!check_quorum_locked(&locks, 4)); // 3 < 4
    }

    #[test]
    fn test_check_failed_unlocks_comprehensive() {
        // Test all combinations for small lock counts

        // 1 lock scenarios
        assert!(!check_failed_unlocks(&["".to_string()], 0)); // 1 success, tolerance 0 -> 1 > 0 = true, but tolerance >= len, so false
        assert!(!check_failed_unlocks(&["".to_string()], 1)); // tolerance >= len
        assert!(!check_failed_unlocks(&["uid".to_string()], 1)); // tolerance >= len
        assert!(check_failed_unlocks(&["uid".to_string()], 0)); // 1 failed > 0

        // 2 lock scenarios
        let two_failed = vec!["uid1".to_string(), "uid2".to_string()];
        let one_failed = vec!["uid1".to_string(), "".to_string()];
        let zero_failed = vec!["".to_string(), "".to_string()];

        // tolerance = 0
        assert!(check_failed_unlocks(&two_failed, 0)); // 2 > 0
        assert!(check_failed_unlocks(&one_failed, 0)); // 1 > 0
        assert!(!check_failed_unlocks(&zero_failed, 0)); // 0 > 0 = false

        // tolerance = 1 (special case: 2 - 1 == 1)
        assert!(check_failed_unlocks(&two_failed, 1)); // 2 >= 1
        assert!(check_failed_unlocks(&one_failed, 1)); // 1 >= 1
        assert!(!check_failed_unlocks(&zero_failed, 1)); // 0 >= 1 = false

        // tolerance = 2
        assert!(!check_failed_unlocks(&two_failed, 2)); // tolerance >= len
        assert!(!check_failed_unlocks(&one_failed, 2)); // tolerance >= len
        assert!(!check_failed_unlocks(&zero_failed, 2)); // tolerance >= len
    }

    #[test]
    fn test_options_clone() {
        let opts = Options {
            timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(100),
        };
        let cloned = opts.clone();
        assert_eq!(opts.timeout, cloned.timeout);
        assert_eq!(opts.retry_interval, cloned.retry_interval);
    }

    #[tokio::test]
    async fn test_drw_mutex_release_all_edge_cases() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(2);
        let mut mutex = DRWMutex::new("owner1".to_string(), names, lockers);

        // Test release_all with empty locks
        let mut empty_locks = vec!["".to_string(), "".to_string()];
        let result = mutex.release_all(1, &mut empty_locks, false).await;
        assert!(result, "Should succeed when releasing empty locks");

        // Test release_all with some locks
        let mut some_locks = vec!["uid1".to_string(), "uid2".to_string()];
        let result = mutex.release_all(1, &mut some_locks, false).await;
        // This should attempt to release the locks and may succeed or fail
        // depending on the local locker state - just ensure it doesn't panic
        let _ = result; // Suppress unused variable warning
    }

    #[test]
    fn test_drw_mutex_struct_fields() {
        let names = vec!["resource1".to_string()];
        let lockers = create_mock_lockers(2);
        let mutex = DRWMutex::new("test-owner".to_string(), names, lockers);

        // Test that all fields are properly initialized
        assert_eq!(mutex.owner, "test-owner");
        assert_eq!(mutex.names, vec!["resource1"]);
        assert_eq!(mutex.write_locks.len(), 2);
        assert_eq!(mutex.read_locks.len(), 2);
        assert_eq!(mutex.lockers.len(), 2);
        assert!(mutex.cancel_refresh_sender.is_none());
        assert_eq!(mutex.refresh_interval, DRW_MUTEX_REFRESH_INTERVAL);
        assert_eq!(mutex.lock_retry_min_interval, LOCK_RETRY_MIN_INTERVAL);

        // All locks should be initially empty
        for lock in &mutex.write_locks {
            assert!(lock.is_empty());
        }
        for lock in &mutex.read_locks {
            assert!(lock.is_empty());
        }
    }
}
