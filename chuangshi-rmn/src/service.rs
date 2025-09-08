use anyhow::Result;
use chuangshi_common::{
    proto::{
        rmn_service_server::RmnService,
        gmm_service_client::GmmServiceClient,
        *,
    },
    types::*,
    utils::*,
    ChuangshiError,
};

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{transport::Channel, Request, Response, Status};
use tracing::{info, warn, error, debug};
use uuid::Uuid;
use chrono::Utc;
use chrono::DateTime;
use crate::cache::MetadataCache;
use crate::block_manager::BlockManager;
use std::collections::HashMap;
#[derive(Clone)]
pub struct RmnServiceImpl {
    datacenter_id: String,
    metadata_cache: Arc<MetadataCache>,
    block_manager: Arc<BlockManager>,
    namespace: Arc<RwLock<HashMap<String, Uuid>>>,
    dn_status: Arc<DashMap<String, DnStatus>>,
    gmm_client: GmmServiceClient<Channel>,
}

#[derive(Clone)]
struct DnStatus {
    address: String,
    total_capacity: u64,
    used_capacity: u64,
    block_count: u32,
    io_load: f32,
    last_heartbeat: DateTime<Utc>,
    is_healthy: bool,
}

impl RmnServiceImpl {
    pub async fn new(
        datacenter_id: String,
        data_dir: String,
        gmm_channel: Channel,
    ) -> Result<Self> {
        let metadata_cache = Arc::new(MetadataCache::new(&data_dir).await?);
        let block_manager = Arc::new(BlockManager::new(&data_dir).await?);
        
        Ok(Self {
            datacenter_id,
            metadata_cache,
            block_manager,
            namespace: Arc::new(RwLock::new(HashMap::new())),
            dn_status: Arc::new(DashMap::new()),
            gmm_client: GmmServiceClient::new(gmm_channel),
        })
    }
    
    pub async fn run_heartbeat(&self, datacenter_id: String, rmn_address: String) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        let mut client = self.gmm_client.clone();
        
        loop {
            interval.tick().await;
            
            // 收集系统负载
            let cpu_usage = self.get_cpu_usage();
            let memory_usage = self.get_memory_usage();
            let storage_usage = self.get_storage_usage().await;
            let active_connections = self.get_active_connections();
            
            let report = RmnStatusReport {
                datacenter_id: datacenter_id.clone(),
                rmn_address: rmn_address.clone(),
                cpu_usage,
                memory_usage,
                storage_usage,
                active_connections,
            };
            
            match client.report_rmn_status(Request::new(report)).await {
                Ok(_) => debug!("Heartbeat sent to GMM"),
                Err(e) => warn!("Failed to send heartbeat to GMM: {}", e),
            }
        }
    }
    
    pub async fn run_dn_health_check(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        
        loop {
            interval.tick().await;
            
            let now = Utc::now();
            for mut entry in self.dn_status.iter_mut() {
                let elapsed = (now - entry.last_heartbeat).num_seconds();
                if elapsed > 120 {
                    if entry.is_healthy {
                        warn!("DN {} is unhealthy", entry.address);
                        entry.is_healthy = false;
                    }
                }
            }
        }
    }
    
    fn get_cpu_usage(&self) -> f32 {
        // TODO: 实现真实的CPU使用率获取
        0.3
    }
    
    fn get_memory_usage(&self) -> f32 {
        // TODO: 实现真实的内存使用率获取
        0.4
    }
    
    async fn get_storage_usage(&self) -> f32 {
        let total_capacity: u64 = self.dn_status.iter()
            .map(|entry| entry.total_capacity)
            .sum();
        
        let used_capacity: u64 = self.dn_status.iter()
            .map(|entry| entry.used_capacity)
            .sum();
        
        if total_capacity > 0 {
            used_capacity as f32 / total_capacity as f32
        } else {
            0.0
        }
    }
    
    fn get_active_connections(&self) -> u32 {
        // TODO: 实现连接数统计
        10
    }
    
    async fn notify_dn_delete(&self, dn_address: &str, block_id: Uuid) -> Result<()> {
        // TODO: 发送gRPC请求到DN
        info!("Notifying DN {} to delete block {}", dn_address, block_id);
        Ok(())
    }
}

#[tonic::async_trait]
impl RmnService for RmnServiceImpl {
    async fn update_metadata(
        &self,
        request: Request<UpdateMetadataRequest>,
    ) -> std::result::Result<Response<UpdateMetadataResponse>, Status> {
        let req = request.into_inner();
        let proto_metadata = req.metadata.ok_or_else(|| {
            Status::invalid_argument("metadata is required")
        })?;
        
        // 转换为内部格式
        let file_id = Uuid::parse_str(&proto_metadata.file_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid file_id: {}", e)))?;
        
        let metadata = chuangshi_common::types::FileMetadata {
            file_id,
            path: proto_metadata.path.clone(),
            size: proto_metadata.size,
            created_at: DateTime::from_timestamp(proto_metadata.created_at, 0)
                .unwrap_or_else(|| Utc::now()),
            modified_at: DateTime::from_timestamp(proto_metadata.modified_at, 0)
                .unwrap_or_else(|| Utc::now()),
            owner: proto_metadata.owner,
            permissions: proto_metadata.permissions,
            replicas: proto_metadata.replicas.into_iter().map(|r| chuangshi_common::types::ReplicaInfo {
                datacenter_id: r.datacenter_id,
                rmn_address: r.rmn_address,
                dn_addresses: r.dn_addresses,
                is_primary: r.is_primary,
                status: match r.status.as_str() {
                    "Creating" => ReplicaStatus::Creating,
                    "Ready" => ReplicaStatus::Ready,
                    "Syncing" => ReplicaStatus::Syncing,
                    "Deleting" => ReplicaStatus::Deleting,
                    _ => ReplicaStatus::Error,
                },
            }).collect(),
            is_complete: proto_metadata.is_complete,
            block_size: 256 * 1024 * 1024, // 256MB
            block_count: (proto_metadata.size + 256 * 1024 * 1024 - 1) / (256 * 1024 * 1024),
        };
        
        info!("Updating metadata for file: {}", file_id);
        
        // 缓存元数据
        self.metadata_cache.update(file_id, metadata.clone());
        
        // 更新本地命名空间
        let mut ns = self.namespace.write().await;
        ns.insert(proto_metadata.path, file_id);
        
        // 生成块映射
        self.block_manager.create_block_map(&metadata).await
            .map_err(|e| Status::internal(format!("Failed to create block map: {}", e)))?;
        
        Ok(Response::new(UpdateMetadataResponse {
            success: true,
        }))
    }
    
    async fn get_block_map(
        &self,
        request: Request<GetBlockMapRequest>,
    ) -> std::result::Result<Response<GetBlockMapResponse>, Status> {
        let req = request.into_inner();
        let file_id = Uuid::parse_str(&req.file_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid file_id: {}", e)))?;
        
        info!("Getting block map for file: {}", file_id);
        
        let blocks = self.block_manager.get_blocks(file_id).await
            .map_err(|e| Status::internal(format!("Failed to get block map: {}", e)))?;
        
        // 转换为protobuf格式
        let proto_blocks = blocks.into_iter().map(|b| chuangshi_common::proto::BlockInfo {
            block_id: b.block_id.to_string(),
            file_id: b.file_id.to_string(),
            index: b.index,
            size: b.size,
            checksum: b.checksum,
            dn_address: b.dn_address,

        }).collect();
        
        Ok(Response::new(GetBlockMapResponse {
            blocks: proto_blocks,
        }))
    }
    
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> std::result::Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        
        // 从路径解析文件ID
        let ns = self.namespace.read().await;
        let file_id = ns.get(&req.path)
            .ok_or_else(|| Status::not_found("File not found"))?;
        
        info!("Deleting file: {} ({})", req.path, file_id);
        
        // 获取文件元数据
        let metadata = self.metadata_cache.get(*file_id);
        
        if let Some(metadata) = metadata {
            // 获取块信息
            let blocks = self.block_manager.get_blocks(*file_id).await
                .map_err(|e| Status::internal(format!("Failed to get blocks: {}", e)))?;
            
            // 通知DN删除块
            for block in blocks {
                if let Err(e) = self.notify_dn_delete(&block.dn_address, block.block_id).await {
                    warn!("Failed to notify DN to delete block: {}", e);
                }
            }
        }
        
        // 删除本地缓存
        self.metadata_cache.remove(*file_id);
        self.block_manager.delete_blocks(*file_id).await
            .map_err(|e| Status::internal(format!("Failed to delete blocks: {}", e)))?;
        
        // 从命名空间删除
        let mut ns = self.namespace.write().await;
        ns.remove(&req.path);
        
        Ok(Response::new(DeleteFileResponse {
            success: true,
        }))
    }
    
    async fn report_dn_status(
        &self,
        request: Request<DnStatusReport>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let report = request.into_inner();
        debug!("Received DN status report from {}", report.dn_address);
        
        // 更新DN状态
        let status = DnStatus {
            address: report.dn_address.clone(),
            total_capacity: report.total_capacity,
            used_capacity: report.used_capacity,
            block_count: report.block_count,
            io_load: report.io_load,
            last_heartbeat: Utc::now(),
            is_healthy: true,
        };
        
        self.dn_status.insert(report.dn_address, status);
        
        Ok(Response::new(Empty {}))
    }
}