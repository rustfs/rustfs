use crate::config::notifier::EventNotifierConfig;
use crate::event_notifier::EventNotifier;
use common::error::Result;
use ecstore::store::ECStore;
use once_cell::sync::OnceCell;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info};

/// 全局事件系统
pub struct EventSystem {
    /// 事件通知器
    notifier: Mutex<Option<EventNotifier>>,
}

impl EventSystem {
    /// 创建一个新的事件系统
    pub fn new() -> Self {
        Self {
            notifier: Mutex::new(None),
        }
    }

    /// 初始化事件系统
    pub async fn init(&self, store: Arc<ECStore>) -> Result<EventNotifierConfig> {
        info!("初始化事件系统");
        let notifier = EventNotifier::new(store).await?;
        let config = notifier.config().clone();

        let mut guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("获取锁失败：{}", e)))?;

        *guard = Some(notifier);
        debug!("事件系统初始化完成");

        Ok(config)
    }

    /// 发送事件
    pub async fn send_event(&self, event: crate::Event) -> Result<()> {
        let guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("获取锁失败：{}", e)))?;

        if let Some(notifier) = &*guard {
            notifier.send(event).await
        } else {
            error!("事件系统未初始化");
            Err(common::error::Error::msg("事件系统未初始化"))
        }
    }

    /// 关闭事件系统
    pub async fn shutdown(&self) -> Result<()> {
        info!("关闭事件系统");
        let mut guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("获取锁失败：{}", e)))?;

        if let Some(ref mut notifier) = *guard {
            notifier.shutdown().await?;
            *guard = None;
            info!("事件系统已关闭");
            Ok(())
        } else {
            debug!("事件系统已经关闭");
            Ok(())
        }
    }
}

/// 全局事件系统实例
pub static GLOBAL_EVENT_SYS: OnceCell<EventSystem> = OnceCell::new();

/// 初始化全局事件系统
pub fn init_global_event_system() -> &'static EventSystem {
    GLOBAL_EVENT_SYS.get_or_init(EventSystem::new)
}
