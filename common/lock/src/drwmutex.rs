use std::time::{Duration, Instant};

use tokio::{sync::mpsc::Sender, time::sleep};
use tracing::{info, warn};

use crate::{lock_args::LockArgs, LockApi, Locker};

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
            write_locks: Vec::with_capacity(lockers.len()),
            read_locks: Vec::with_capacity(lockers.len()),
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
        info!("lockBlocking {}/{} for {:?}: lockType readLock({}), additional opts: {:?}, quorum: {}, tolerance: {}, lockClients: {}\n", id, source, self.names, is_read_lock, opts, quorum, tolerance, locker_len);

        tolerance = locker_len - quorum;
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
            panic!("Trying to un_lock() while no lock() is active, write_locks: {:?}", self.write_locks)
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
            panic!("Trying to un_r_lock() while no r_lock() is active, read_locks: {:?}", self.read_locks)
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

    if locks.len() - tolerance == tolerance {
        return un_locks_failed >= tolerance;
    }

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
