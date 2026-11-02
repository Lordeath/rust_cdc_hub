use crate::custom_error::{CustomError, CustomErrorType};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::watch;

/// 每次同步运行共享的停止信号；保留首个不可恢复错误供所有任务一致收尾。
#[derive(Clone)]
pub struct SyncControl {
    error: watch::Sender<Option<CustomError>>,
    ready: Arc<AtomicBool>,
}

impl Default for SyncControl {
    fn default() -> Self {
        Self {
            error: watch::channel(None).0,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl SyncControl {
    pub fn reset(&self) {
        self.ready.store(false, Ordering::Release);
        self.error.send_replace(None);
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && self.check().is_ok()
    }

    pub fn stop(&self, error: CustomError) {
        self.error.send_if_modified(|current| {
            // 收尾期间发生的不可恢复写入错误，必须覆盖此前的临时连接错误。
            if current.as_ref().is_some_and(|current| {
                matches!(current.error_type, CustomErrorType::Fatal)
                    || !matches!(error.error_type, CustomErrorType::Fatal)
            }) {
                return false;
            }
            *current = Some(error);
            true
        });
    }

    pub fn check(&self) -> Result<(), CustomError> {
        match self.error.borrow().clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub async fn cancelled(&self) -> CustomError {
        self.error
            .subscribe()
            .wait_for(|error| error.is_some())
            .await
            .expect("同步停止信号仍由当前任务持有")
            .clone()
            .expect("停止错误已设置")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_preserves_first_error_and_wakes_late_subscribers() {
        let control = SyncControl::default();
        let other = control.clone();
        control.stop(CustomError::fatal("binlog 缺失"));
        other.stop(CustomError::restart("其他任务取消"));
        assert_eq!(other.cancelled().await.message, "binlog 缺失");
        assert!(control.check().is_err());
    }

    #[tokio::test]
    async fn fatal_flush_failure_during_shutdown_prevents_network_retry() {
        let control = SyncControl::default();
        control.stop(CustomError::restart("network reset"));
        control.stop(CustomError::fatal("flush failed"));
        let error = control.cancelled().await;
        assert_eq!(error.message, "flush failed");
        assert!(matches!(error.error_type, CustomErrorType::Fatal));
    }
}
