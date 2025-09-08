use anyhow::Result;
use chuangshi_common::{
    proto::{
        dn_service_server::DnService,
        rmn_service_client::RmnServiceClient,
        *,
    },
    types::*,
    utils::*,
    ChuangshiError,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{transport::Channel, Request, Response, Status};
use tracing::{info, warn, error, debug};
use uuid::Uuid;
use bytes::Bytes;

use crate::storage::StorageEngine;
use crate::erasure_coding::ErasureCoder;
use crate::replication::ReplicationManager;
#[derive(Clone)]
pub struct DnServiceImpl {
    datacenter_id: String,
    storage_engine: Arc<StorageEngine>,
    erasure_coder: Arc<ErasureCoder>,
    replication_manager: Arc<ReplicationManager>,
    rmn_client: RmnServiceClient<Channel>,
}

impl DnServiceImpl {
    pub async fn new(
        datacenter_id: String,
        data_dir: String,
        capacity: u64,
        rmn_channel: Channel,
    ) -> Result<Self> {
        let storage_engine = Arc::new(StorageEngine::new(&data_dir, capacity).await?);
        let erasure_coder = Arc::new(ErasureCoder::new(6, 3)); // 6+3纠删码
        let replication_manager = Arc::new(ReplicationManager::new());
        
        Ok(Self {
            datacenter_id,
            storage_engine,
            erasure_coder,
            replication_manager,
            rmn_client: RmnServiceClient::new(rmn_channel),
        })
    }
    
    pub async fn run_heartbeat(&self, dn_address: String) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        let mut client = self.rmn_client.clone();
        
        loop {
            interval.tick().await;
            
            let (total, used) = self.storage_engine.get_capacity().await;
            let block_count = self.storage_engine.get_block_count().await;
            let io_load = self.storage_engine.get_io_load().await;
            
            let report = DnStatusReport {
                dn_address: dn_address.clone(),
                total_capacity: total,
                used_capacity: used,
                block_count,
                io_load,
            };
            
            match client.report_dn_status(Request::new(report)).await {
                Ok(_) => debug!("Heartbeat sent to RMN"),
                Err(e) => warn!("Failed to send heartbeat to RMN: {}", e),
            }
        }
    }
    
    pub async fn run_storage_cleanup(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600)); // 每小时
        
        loop {
            interval.tick().await;
            
            info!("Running storage cleanup");
            
            // 清理过期的临时文件
            if let Err(e) = self.storage_engine.cleanup_temp_files().await {
                error!("Failed to cleanup temp files: {}", e);
            }
            
            // 验证块完整性
            if let Err(e) = self.storage_engine.verify_blocks().await {
                error!("Failed to verify blocks: {}", e);
            }
        }
    }
}

#[tonic::async_trait]
impl DnService for DnServiceImpl {
    async fn write_block(
        &self,
        request: Request<WriteBlockRequest>,
    ) -> std::result::Result<Response<WriteBlockResponse>, Status> {
        let req = request.into_inner();
        let block_id = Uuid::parse_str(&req.block_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block_id: {}", e)))?;
        
        info!("Writing block: {} with size: {}", block_id, req.data.len());
        
        // 计算校验和
        let checksum = calculate_checksum(&req.data);
        
        // 应用纠删码
        let shards = self.erasure_coder.encode(&req.data)
            .map_err(|e| Status::internal(format!("Failed to encode: {}", e)))?;
        
        // 存储主数据块
        self.storage_engine.write_block(block_id, &req.data).await
            .map_err(|e| Status::internal(format!("Failed to write block: {}", e)))?;
        
        // 存储纠删码分片
        for (i, shard) in shards.iter().enumerate() {
            let shard_id = format!("{}-{}", block_id, i);
            self.storage_engine.write_shard(&shard_id, shard).await
                .map_err(|e| Status::internal(format!("Failed to write shard: {}", e)))?;
        }
        
        // 如果是副本，通知主副本同步
        if req.is_replica {
            // TODO: 实现副本同步逻辑
        }
        
        Ok(Response::new(WriteBlockResponse {
            block_id: block_id.to_string(),
            checksum,
            success: true,
        }))
    }
    
    async fn read_block(
        &self,
        request: Request<ReadBlockRequest>,
    ) -> std::result::Result<Response<ReadBlockResponse>, Status> {
        let req = request.into_inner();
        let block_id = Uuid::parse_str(&req.block_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block_id: {}", e)))?;
        
        info!("Reading block: {}", block_id);
        
        // 尝试读取主数据块
        match self.storage_engine.read_block(block_id).await {
            Ok(data) => {
                Ok(Response::new(ReadBlockResponse { data }))
            }
            Err(_) => {
                // 主块读取失败，尝试从纠删码恢复
                warn!("Primary block read failed, attempting recovery from erasure code");
                
                let recovered = self.recover_from_erasure_code(block_id).await
                    .map_err(|e| Status::internal(format!("Failed to recover block: {}", e)))?;
                
                // 修复主块
                self.storage_engine.write_block(block_id, &recovered).await
                    .map_err(|e| Status::internal(format!("Failed to repair block: {}", e)))?;
                
                Ok(Response::new(ReadBlockResponse { data: recovered }))
            }
        }
    }
    
    async fn delete_block(
        &self,
        request: Request<DeleteBlockRequest>,
    ) -> std::result::Result<Response<DeleteBlockResponse>, Status> {
        let req = request.into_inner();
        let block_id = Uuid::parse_str(&req.block_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block_id: {}", e)))?;
        
        info!("Deleting block: {}", block_id);
        
        // 删除主块
        self.storage_engine.delete_block(block_id).await
            .map_err(|e| Status::internal(format!("Failed to delete block: {}", e)))?;
        
        // 删除纠删码分片
        for i in 0..9 { // 6+3=9个分片
            let shard_id = format!("{}-{}", block_id, i);
            self.storage_engine.delete_shard(&shard_id).await.ok();
        }
        
        Ok(Response::new(DeleteBlockResponse {
            success: true,
        }))
    }
    
    async fn replicate_block(
        &self,
        request: Request<ReplicateBlockRequest>,
    ) -> std::result::Result<Response<ReplicateBlockResponse>, Status> {
        let req = request.into_inner();
        let block_id = Uuid::parse_str(&req.block_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid block_id: {}", e)))?;
        
        info!("Replicating block {} to {}", block_id, req.target_dn);
        
        // 读取块数据
        let data = self.storage_engine.read_block(block_id).await
            .map_err(|e| Status::internal(format!("Failed to read block: {}", e)))?;
        
        // 发送到目标DN
        self.replication_manager.replicate_to(&req.target_dn, block_id, &data).await
            .map_err(|e| Status::internal(format!("Failed to replicate: {}", e)))?;
        
        Ok(Response::new(ReplicateBlockResponse {
            success: true,
        }))
    }
}

impl DnServiceImpl {
    async fn recover_from_erasure_code(&self, block_id: Uuid) -> Result<Vec<u8>> {
        let mut shards = vec![];
        
        // 收集可用的分片
        for i in 0..9 {
            let shard_id = format!("{}-{}", block_id, i);
            match self.storage_engine.read_shard(&shard_id).await {
                Ok(shard) => shards.push(Some(shard)),
                Err(_) => shards.push(None),
            }
        }
        
        // 使用纠删码恢复
        self.erasure_coder.decode(shards)
    }
}