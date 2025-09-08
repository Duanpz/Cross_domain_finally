use crate::types::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// GMM请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GmmRequest {
    CreateFile {
        path: String,
        size: u64,
        tags: Vec<String>,
    },
    GetFile {
        path: String,
    },
    DeleteFile {
        path: String,
    },
    ListDirectory {
        path: String,
    },
    GetDataCenterStatus,
    UpdatePolicy {
        policy: Policy,
    },
}

/// GMM响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GmmResponse {
    FileCreated {
        file_id: Uuid,
        primary_dc: String,
        replica_dcs: Vec<String>,
        dn_addresses: Vec<String>,
    },
    FileInfo {
        metadata: FileMetadata,
    },
    FileDeleted,
    DirectoryListing {
        entries: Vec<FileMetadata>,
    },
    DataCenterStatus {
        centers: Vec<DataCenter>,
    },
    PolicyUpdated,
    Error {
        message: String,
    },
}

/// RMN请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RmnRequest {
    UpdateMetadata {
        file_id: Uuid,
        metadata: FileMetadata,
    },
    GetBlockMap {
        file_id: Uuid,
    },
    DeleteFile {
        file_id: Uuid,
    },
    ReportLoad,
}

/// RMN响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RmnResponse {
    MetadataUpdated,
    BlockMap {
        blocks: Vec<BlockInfo>,
    },
    FileDeleted,
    LoadReport {
        cpu_usage: f32,
        memory_usage: f32,
        storage_usage: f32,
        active_connections: u32,
    },
    Error {
        message: String,
    },
}

/// DN请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DnRequest {
    WriteBlock {
        block_id: Uuid,
        data: Vec<u8>,
    },
    ReadBlock {
        block_id: Uuid,
    },
    DeleteBlock {
        block_id: Uuid,
    },
    ReplicateBlock {
        block_id: Uuid,
        target_dn: String,
    },
}

/// DN响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DnResponse {
    BlockWritten {
        block_id: Uuid,
        checksum: Vec<u8>,
    },
    BlockData {
        data: Vec<u8>,
    },
    BlockDeleted,
    BlockReplicated,
    Error {
        message: String,
    },
}