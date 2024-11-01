use std::{
    fmt::Debug,
    ptr,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{spawn, sync::Mutex};

use crate::error::Result;

type UpdateFn<T> = Box<dyn Fn() -> Result<T> + Send + Sync>;

#[derive(Clone, Debug, Default)]
pub struct Opts {
    return_last_good: bool,
    no_wait: bool,
}

pub struct Cache<T: Clone + Debug + Send> {
    update_fn: UpdateFn<T>,
    ttl: Duration,
    opts: Opts,
    val: AtomicPtr<T>,
    last_update_ms: AtomicU64,
    updating: Arc<Mutex<bool>>,
}

impl<T: Clone + Debug + Send + 'static> Cache<T> {
    pub fn new(update_fn: UpdateFn<T>, ttl: Duration, opts: Opts) -> Self {
        let val = AtomicPtr::new(ptr::null_mut());
        Self {
            update_fn,
            ttl,
            opts,
            val,
            last_update_ms: AtomicU64::new(0),
            updating: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn get(self: Arc<Self>) -> Result<T> {
        let v_ptr = self.val.load(Ordering::SeqCst);
        let v = if v_ptr.is_null() {
            None
        } else {
            Some(unsafe { (*v_ptr).clone() })
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        if v.is_some() && now - self.last_update_ms.load(Ordering::SeqCst) < self.ttl.as_secs() {
            return Ok(v.unwrap());
        }

        if self.opts.no_wait && v.is_some() && now - self.last_update_ms.load(Ordering::SeqCst) < self.ttl.as_secs() * 2 {
            if self.updating.try_lock().is_ok() {
                let this = Arc::clone(&self);
                spawn(async move {
                    let _ = this.update().await;
                });
            }

            return Ok(v.unwrap());
        }

        let _ = self.updating.lock().await;

        if let Ok(duration) =
            SystemTime::now().duration_since(UNIX_EPOCH + Duration::from_secs(self.last_update_ms.load(Ordering::SeqCst)))
        {
            if duration < self.ttl {
                return Ok(v.unwrap());
            }
        }

        match self.update().await {
            Ok(_) => {
                let v_ptr = self.val.load(Ordering::SeqCst);
                let v = if v_ptr.is_null() {
                    None
                } else {
                    Some(unsafe { (*v_ptr).clone() })
                };
                Ok(v.unwrap())
            }
            Err(err) => Err(err),
        }
    }

    async fn update(&self) -> Result<()> {
        match (self.update_fn)() {
            Ok(val) => {
                self.val.store(Box::into_raw(Box::new(val)), Ordering::SeqCst);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs();
                self.last_update_ms.store(now, Ordering::SeqCst);
                Ok(())
            }
            Err(err) => {
                let v_ptr = self.val.load(Ordering::SeqCst);
                if self.opts.return_last_good && !v_ptr.is_null() {
                    return Ok(());
                }

                return Err(err);
            }
        }
    }
}
