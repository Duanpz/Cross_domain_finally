#![allow(dead_code)]
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// 文件元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_id: Uuid,
    pub path: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub owner: String,
    pub permissions: u32,
    pub replicas: Vec<ReplicaInfo>,
    pub is_complete: bool,
    pub block_size: u64,
    pub block_count: u64,
}

impl Default for FileMetadata {
    fn default() -> Self {
        Self {
            file_id: Uuid::new_v4(),
            path: String::new(),
            size: 0,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            owner: String::from("system"),
            permissions: 0o644,
            replicas: Vec::new(),
            is_complete: false,
            block_size: 256 * 1024 * 1024, // 256MB
            block_count: 0,
        }
    }
}

/// 副本信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub datacenter_id: String,
    pub rmn_address: String,
    pub dn_addresses: Vec<String>,
    pub is_primary: bool,
    pub status: ReplicaStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplicaStatus {
    Creating,
    Ready,
    Syncing,
    Deleting,
    Error,
}

/// 文件块信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub block_id: Uuid,
    pub file_id: Uuid,
    pub index: u64,
    pub size: u64,
    pub checksum: Vec<u8>,
    pub dn_address: String,
    pub erasure_shards: Vec<ShardInfo>,
}

/// 纠删码分片信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: u32,
    pub dn_address: String,
    pub size: u64,
    pub checksum: Vec<u8>,
}

/// 数据中心信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenter {
    pub id: String,
    pub name: String,
    pub region: String,
    pub rmn_address: String,
    pub dn_addresses: Vec<String>,
    pub capacity: StorageCapacity,
    pub status: DataCenterStatus,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCapacity {
    pub total: u64,
    pub used: u64,
    pub available: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataCenterStatus {
    Online,
    Offline,
    Maintenance,
    Degraded,
}

/// 策略定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub rules: Vec<PolicyRule>,
    pub priority: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyRule {
    pub condition: PolicyCondition,
    pub action: PolicyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyCondition {
    FileTag(String),
    FileSize { min: Option<u64>, max: Option<u64> },
    AccessPattern(String),
    FileExtension(String),
    UserGroup(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolicyAction {
    PlaceIn(Vec<String>),
    ReplicateTo(Vec<String>),
    CreateHotCopy(String),
    SetReplicationFactor(u32),
    UseErasureCoding { data: usize, parity: usize },
}

/// 系统配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    pub block_size: u64,
    pub replication_factor: u32,
    pub erasure_data_shards: usize,
    pub erasure_parity_shards: usize,
    pub heartbeat_interval_secs: u64,
    pub health_check_interval_secs: u64,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            block_size: 256 * 1024 * 1024, // 256MB
            replication_factor: 2,
            erasure_data_shards: 6,
            erasure_parity_shards: 3,
            heartbeat_interval_secs: 30,
            health_check_interval_secs: 60,
        }
    }
}