use dashmap::DashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct RmnHealth {
    pub datacenter_id: String,
    pub cpu_usage: f32,
    pub memory_usage: f32,
    pub storage_usage: f32,
    pub last_update: DateTime<Utc>,
}

pub struct HealthMonitor {
    rmn_health: Arc<DashMap<String, RmnHealth>>,
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            rmn_health: Arc::new(DashMap::new()),
        }
    }
    
    pub fn update_rmn_status(
        &self,
        datacenter_id: String,
        cpu_usage: f32,
        memory_usage: f32, 
        storage_usage: f32,
    ) {
        let health = RmnHealth {
            datacenter_id: datacenter_id.clone(),
            cpu_usage,
            memory_usage,
            storage_usage,
            last_update: Utc::now(),
        };
        
        self.rmn_health.insert(datacenter_id, health);
    }
    
    pub fn get_healthiest_datacenter(&self, exclude: &[String]) -> Option<String> {
        let mut best_dc = None;
        let mut best_score = f32::MAX;
        
        for entry in self.rmn_health.iter() {
            if exclude.contains(&entry.key().clone()) {
                continue;
            }
            
            let health = entry.value();
            let score = health.cpu_usage + health.memory_usage + health.storage_usage;
            
            if score < best_score {
                best_score = score;
                best_dc = Some(entry.key().clone());
            }
        }
        
        best_dc
    }
    
    pub fn is_healthy(&self, datacenter_id: &str) -> bool {
        if let Some(health) = self.rmn_health.get(datacenter_id) {
            let elapsed = (Utc::now() - health.last_update).num_seconds();
            elapsed < 120 // 2分钟内有心跳
        } else {
            false
        }
    }
}