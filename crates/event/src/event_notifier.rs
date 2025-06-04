use crate::config::notifier::EventNotifierConfig;
use crate::Event;
use common::error::{Error, Result};
use ecstore::store::ECStore;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::util::SubscriberInitExt;

/// 事件通知器
pub struct EventNotifier {
    /// 事件发送通道
    sender: mpsc::Sender<Event>,
    /// 接收器任务句柄
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// 配置信息
    config: EventNotifierConfig,
    /// 关闭标记
    shutdown: CancellationToken,
    /// 关闭通知通道
    shutdown_complete_tx: Option<broadcast::Sender<()>>,
}

impl EventNotifier {
    /// 创建新的事件通知器
    #[instrument(skip_all)]
    pub async fn new(store: Arc<ECStore>) -> Result<Self> {
        let manager = crate::store::manager::EventManager::new(store);

        // 初始化配置
        let config = manager.init().await?;

        // 创建适配器
        let adapters = manager.create_adapters().await?;
        info!("创建了 {} 个适配器", adapters.len());

        // 创建关闭标记
        let shutdown = CancellationToken::new();
        let (shutdown_complete_tx, _) = broadcast::channel(1);

        // 创建事件通道 - 使用默认容量，因为每个适配器都有自己的队列
        // 这里使用较小的通道容量，因为事件会被快速分发到适配器
        let (sender, mut receiver) = mpsc::channel(100);

        let shutdown_clone = shutdown.clone();
        let shutdown_complete_tx_clone = shutdown_complete_tx.clone();
        let adapters_clone = adapters.clone();

        // 启动事件处理任务
        let task_handle = tokio::spawn(async move {
            debug!("事件处理任务启动");

            loop {
                tokio::select! {
                    Some(event) = receiver.recv() => {
                        debug!("收到事件：{}", event.id);

                        // 分发到所有适配器
                        for adapter in &adapters_clone {
                            let adapter_name = adapter.name();
                            match adapter.send(&event).await {
                                Ok(_) => {
                                    debug!("事件 {} 成功发送到适配器 {}", event.id, adapter_name);
                                }
                                Err(e) => {
                                    error!("事件 {} 发送到适配器 {} 失败：{}", event.id, adapter_name, e);
                                }
                            }
                        }
                    }

                    _ = shutdown_clone.cancelled() => {
                        info!("接收到关闭信号，事件处理任务停止");
                        let _ = shutdown_complete_tx_clone.send(());
                        break;
                    }
                }
            }

            debug!("事件处理任务已停止");
        });

        Ok(Self {
            sender,
            task_handle: Some(task_handle),
            config,
            shutdown,
            shutdown_complete_tx: Some(shutdown_complete_tx),
        })
    }

    /// 关闭事件通知器
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("关闭事件通知器");
        self.shutdown.cancel();

        if let Some(shutdown_tx) = self.shutdown_complete_tx.take() {
            let mut rx = shutdown_tx.subscribe();

            // 等待关闭完成信号或超时
            tokio::select! {
                _ = rx.recv() => {
                    debug!("收到关闭完成信号");
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    warn!("关闭超时，强制终止");
                }
            }
        }

        if let Some(handle) = self.task_handle.take() {
            handle.abort();
            match handle.await {
                Ok(_) => debug!("事件处理任务已正常终止"),
                Err(e) => {
                    if e.is_cancelled() {
                        debug!("事件处理任务已取消");
                    } else {
                        error!("等待事件处理任务终止时出错：{}", e);
                    }
                }
            }
        }

        info!("事件通知器已完全关闭");
        Ok(())
    }

    /// 发送事件
    pub async fn send(&self, event: Event) -> Result<()> {
        self.sender
            .send(event)
            .await
            .map_err(|e| Error::msg(format!("发送事件到通道失败：{}", e)))
    }

    /// 获取当前配置
    pub fn config(&self) -> &EventNotifierConfig {
        &self.config
    }
}
