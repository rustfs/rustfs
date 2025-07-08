// Copyright 2024 RustFS Team

use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct LifecycleConfig {}

pub struct LifecycleManager {}

impl LifecycleManager {
    pub async fn new(_config: LifecycleConfig) -> Result<Self> {
        Ok(Self {})
    }
    
    pub async fn start(&self) -> Result<()> {
        Ok(())
    }
    
    pub async fn stop(&self) -> Result<()> {
        Ok(())
    }
} 