use anyhow::Result;
use chuangshi_common::{
    proto::{
        gmm_service_server::GmmService,
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

use crate::metadata::MetadataManager;
use crate::policy::PolicyEngine;
use crate::namespace::NamespaceManager;
use crate::health::HealthMonitor;
#[derive(Clone)] 
pub struct GmmServiceImpl {
    metadata_manager: Arc<MetadataManager>,
    policy_engine: Arc<RwLock<PolicyEngine>>,
    namespace_manager: Arc<NamespaceManager>,
    health_monitor: Arc<HealthMonitor>,
    datacenters: Arc<DashMap<String, DataCenter>>,
    config: SystemConfig,
}

impl GmmServiceImpl {
    pub async fn new(data_dir: String) -> Result<Self> {
        let metadata_manager = Arc::new(MetadataManager::new(&data_dir).await?);
        let policy_engine = Arc::new(RwLock::new(PolicyEngine::new()));
        let namespace_manager = Arc::new(NamespaceManager::new(&data_dir).await?);
        let health_monitor = Arc::new(HealthMonitor::new());
        
        // 初始化数据中心
        let datacenters = Arc::new(DashMap::new());
        
        // 添加默认数据中心配置
        let default_dcs = vec![
            ("beijing", "Beijing DC", "north", "beijing:8002"),
            ("shanghai", "Shanghai DC", "east", "shanghai:8012"),
           
        ];
        
        for (id, name, region, rmn_addr) in default_dcs {
            datacenters.insert(
                id.to_string(),
                DataCenter {
                    id: id.to_string(),
                    name: name.to_string(),
                    region: region.to_string(),
                    rmn_address: rmn_addr.to_string(),
                    dn_addresses: vec![
                        format!("{}:8003", id),
                        format!("{}:8004", id),
                        format!("{}:8005", id),
                    ],
                    capacity: StorageCapacity {
                        total: 10 * 1024 * 1024 * 1024 * 1024, // 10TB
                        used: 0,
                        available: 10 * 1024 * 1024 * 1024 * 1024,
                    },
                    status: DataCenterStatus::Online,
                    last_heartbeat: Utc::now(),
                },
            );
        }
        
        Ok(Self {
            metadata_manager,
            policy_engine,
            namespace_manager,
            health_monitor,
            datacenters,
            config: SystemConfig::default(),
        })
    }
    
    pub async fn run_health_checker(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(
                self.config.health_check_interval_secs
            )).await;
            
            // 检查数据中心健康状态
            let now = Utc::now();
            for mut dc in self.datacenters.iter_mut() {
                let elapsed = (now - dc.last_heartbeat).num_seconds();
                if elapsed > 120 {
                    if dc.status != DataCenterStatus::Offline {
                        warn!("Datacenter {} is offline", dc.id);
                        dc.status = DataCenterStatus::Offline;
                    }
                } else if elapsed > 60 {
                    if dc.status == DataCenterStatus::Online {
                        warn!("Datacenter {} is degraded", dc.id);
                        dc.status = DataCenterStatus::Degraded;
                    }
                }
            }
        }
    }
    
    pub async fn run_policy_executor(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            
            // 执行策略检查和调度
            debug!("Running policy executor");
            
            // TODO: 实现策略执行逻辑
        }
    }
    
    async fn select_datacenters(&self, tags: &[String], size: u64) -> Vec<String> {
        let policy_engine = self.policy_engine.read().await;
        let selected = policy_engine.select_datacenters(tags, size, &self.datacenters);
        
        if selected.is_empty() {
            // 如果策略没有选择任何数据中心，使用默认策略
            let online_dcs: Vec<String> = self.datacenters
                .iter()
                .filter(|dc| dc.status == DataCenterStatus::Online)
                .map(|dc| dc.id.clone())
                .collect();
            
            if !online_dcs.is_empty() {
                vec![online_dcs[0].clone()]
            } else {
                vec![]
            }
        } else {
            selected
        }
    }

    async fn notify_rmn_create(&self, rmn_address: &str, metadata: &chuangshi_common::types::FileMetadata) -> Result<()> {
        info!("Notifying RMN {} to create file {}", rmn_address, metadata.file_id);
        
        // 建立到RMN的连接
        let channel = Channel::from_shared(format!("http://{}", rmn_address))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to RMN {}: {}", rmn_address, e))?;
        
        let mut rmn_client = chuangshi_common::proto::rmn_service_client::RmnServiceClient::new(channel);
        
        // 转换元数据为protobuf格式
        let proto_metadata = chuangshi_common::proto::FileMetadata {
            file_id: metadata.file_id.to_string(),
            path: metadata.path.clone(),
            size: metadata.size,
            created_at: metadata.created_at.timestamp(),
            modified_at: metadata.modified_at.timestamp(),
            owner: metadata.owner.clone(),
            permissions: metadata.permissions,
            replicas: metadata.replicas.iter().map(|r| chuangshi_common::proto::ReplicaInfo {
                datacenter_id: r.datacenter_id.clone(),
                rmn_address: r.rmn_address.clone(),
                dn_addresses: r.dn_addresses.clone(),
                is_primary: r.is_primary,
                status: format!("{:?}", r.status),
            }).collect(),
            is_complete: metadata.is_complete,
        };
        
        // 发送更新元数据请求
        let request = Request::new(chuangshi_common::proto::UpdateMetadataRequest {
            metadata: Some(proto_metadata),
        });
        
        match rmn_client.update_metadata(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    info!("Successfully notified RMN {} to create file {}", rmn_address, metadata.file_id);
                } else {
                    warn!("RMN {} failed to create file {}", rmn_address, metadata.file_id);
                    return Err(anyhow::anyhow!("RMN failed to create file"));
                }
            }
            Err(e) => {
                error!("Failed to notify RMN {} to create file {}: {}", rmn_address, metadata.file_id, e);
                return Err(anyhow::anyhow!("Failed to notify RMN: {}", e));
            }
        }
        
        Ok(())
    }

    async fn notify_rmn_delete(&self, rmn_address: &str, file_id: Uuid) -> Result<()> {
        info!("Notifying RMN {} to delete file {}", rmn_address, file_id);
        
        // 建立到RMN的连接
        let channel = Channel::from_shared(format!("http://{}", rmn_address))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to RMN {}: {}", rmn_address, e))?;
        
        let mut rmn_client = chuangshi_common::proto::rmn_service_client::RmnServiceClient::new(channel);
        
        // 首先需要获取文件路径（从元数据缓存中）
        let file_path = if let Ok(metadata) = self.metadata_manager.get_metadata(file_id).await {
            metadata.path
        } else {
            // 如果元数据已被删除，使用文件ID作为路径
            format!("/chuangshi/deleted/{}", file_id)
        };
        
        // 发送删除文件请求
        let request = Request::new(chuangshi_common::proto::DeleteFileRequest {
            path: file_path,
        });
        
        match rmn_client.delete_file(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    info!("Successfully notified RMN {} to delete file {}", rmn_address, file_id);
                } else {
                    warn!("RMN {} failed to delete file {}", rmn_address, file_id);
                    // 对于删除操作，即使RMN报告失败也继续，因为文件可能已经不存在
                }
            }
            Err(e) => {
                // 对于删除操作的错误，记录但不中断流程
                warn!("Failed to notify RMN {} to delete file {}: {}", rmn_address, file_id, e);
                // 如果是NOT_FOUND错误，认为删除成功
                if e.code() == tonic::Code::NotFound {
                    info!("File {} already deleted on RMN {}", file_id, rmn_address);
                } else {
                    // 其他错误可能需要重试，但这里简化处理
                    error!("RMN {} delete notification failed, will continue: {}", rmn_address, e);
                }
            }
        }
        
        Ok(())
    }
}

#[tonic::async_trait]
impl GmmService for GmmServiceImpl {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> std::result::Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        info!("Creating file: {} with size: {}", req.path, req.size);
        
        // 验证路径
        if !is_valid_path(&req.path) {
            return Err(Status::invalid_argument("Invalid path"));
        }
        
        // 选择数据中心
        let selected_dcs = self.select_datacenters(&req.tags, req.size).await;
        if selected_dcs.is_empty() {
            return Err(Status::resource_exhausted("No available datacenters"));
        }
        
        let primary_dc = &selected_dcs[0];
        let replica_dcs = selected_dcs[1..].to_vec();
        
        // 生成文件ID
        let file_id = Uuid::new_v4();
        
        // 计算块信息
        let block_size = self.config.block_size;
        let block_count = (req.size + block_size - 1) / block_size;
        
        // 获取DN地址
        let primary_dc_info = self.datacenters.get(primary_dc)
            .ok_or_else(|| Status::internal("Datacenter not found"))?;
        let dn_addresses = primary_dc_info.dn_addresses.clone();
        
        // 创建文件元数据
        let mut metadata = chuangshi_common::types::FileMetadata {
            file_id,
            path: req.path.clone(),
            size: req.size,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            owner: "user".to_string(),
            permissions: 0o644,
            replicas: vec![],
            is_complete: false,
            block_size,
            block_count,
        };
        
        // 添加主副本
        metadata.replicas.push(chuangshi_common::types::ReplicaInfo {
            datacenter_id: primary_dc.clone(),
            rmn_address: primary_dc_info.rmn_address.clone(),
            dn_addresses: dn_addresses.clone(),
            is_primary: true,
            status: chuangshi_common::types::ReplicaStatus::Creating,
        });
        
        // 添加备份副本
        for dc_id in &replica_dcs {
            if let Some(dc_info) = self.datacenters.get(dc_id) {
                metadata.replicas.push(chuangshi_common::types::ReplicaInfo {
                    datacenter_id: dc_id.clone(),
                    rmn_address: dc_info.rmn_address.clone(),
                    dn_addresses: dc_info.dn_addresses.clone(),
                    is_primary: false,
                    status: chuangshi_common::types::ReplicaStatus::Creating,
                });
            }
        }
        
        // 保存元数据
        self.metadata_manager.save_metadata(&metadata).await
            .map_err(|e| Status::internal(format!("Failed to save metadata: {}", e)))?;
        
        // 更新命名空间
        self.namespace_manager.add_file(&req.path, file_id).await
            .map_err(|e| Status::internal(format!("Failed to update namespace: {}", e)))?;
        
        // 通知RMN
        for replica in &metadata.replicas {
            if let Err(e) = self.notify_rmn_create(&replica.rmn_address, &metadata).await {
                warn!("Failed to notify RMN {}: {}", replica.rmn_address, e);
            }
        }
        
        Ok(Response::new(CreateFileResponse {
            file_id: file_id.to_string(),
            primary_dc: primary_dc.clone(),
            replica_dcs,
            dn_addresses,
        }))
    }
    
    async fn get_file(
        &self,
        request: Request<GetFileRequest>,
    ) -> std::result::Result<Response<GetFileResponse>, Status> {
        let req = request.into_inner();
        info!("Getting file: {}", req.path);
        
        // 从命名空间获取文件ID
        let file_id = self.namespace_manager.resolve_path(&req.path).await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;
        
        // 获取元数据
        let metadata = self.metadata_manager.get_metadata(file_id).await
            .map_err(|e| Status::internal(format!("Failed to get metadata: {}", e)))?;
        
        // 转换为protobuf格式
        let proto_metadata = chuangshi_common::proto::FileMetadata {
            file_id: metadata.file_id.to_string(),
            path: metadata.path,
            size: metadata.size,
            created_at: metadata.created_at.timestamp(),
            modified_at: metadata.modified_at.timestamp(),
            owner: metadata.owner,
            permissions: metadata.permissions,
            replicas: metadata.replicas.iter().map(|r| chuangshi_common::proto::ReplicaInfo {
                datacenter_id: r.datacenter_id.clone(),
                rmn_address: r.rmn_address.clone(),
                dn_addresses: r.dn_addresses.clone(),
                is_primary: r.is_primary,
                status: format!("{:?}", r.status),
            }).collect(),
            is_complete: metadata.is_complete,
        };
        
        Ok(Response::new(GetFileResponse {
            metadata: Some(proto_metadata),
        }))
    }
    
    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> std::result::Result<Response<DeleteFileResponse>, Status> {
        let req = request.into_inner();
        info!("Deleting file: {}", req.path);
        
        // 从命名空间获取文件ID
        let file_id = self.namespace_manager.resolve_path(&req.path).await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;
        
        // 获取元数据
        let metadata = self.metadata_manager.get_metadata(file_id).await
            .map_err(|e| Status::internal(format!("Failed to get metadata: {}", e)))?;
        
        // 标记为删除中
        self.metadata_manager.mark_deleting(file_id).await
            .map_err(|e| Status::internal(format!("Failed to mark deleting: {}", e)))?;
        
        // 通知所有RMN删除
        for replica in &metadata.replicas {
            if let Err(e) = self.notify_rmn_delete(&replica.rmn_address, file_id).await {
                warn!("Failed to notify RMN {}: {}", replica.rmn_address, e);
            }
        }
        
        // 从命名空间删除
        self.namespace_manager.remove_file(&req.path).await
            .map_err(|e| Status::internal(format!("Failed to update namespace: {}", e)))?;
        
        // 删除元数据
        self.metadata_manager.delete_metadata(file_id).await
            .map_err(|e| Status::internal(format!("Failed to delete metadata: {}", e)))?;
        
        Ok(Response::new(DeleteFileResponse {
            success: true,
        }))
    }
    
    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> std::result::Result<Response<ListDirectoryResponse>, Status> {
        let req = request.into_inner();
        info!("Listing directory: {}", req.path);
        
        // 列出目录内容
        let entries = self.namespace_manager.list_directory(&req.path).await
            .map_err(|e| Status::internal(format!("Failed to list directory: {}", e)))?;
        
        // 获取每个文件的元数据
        let mut proto_entries:Vec<chuangshi_common::proto::FileMetadata> = Vec::new();
        for (path, file_id) in entries {
            if let Ok(metadata) = self.metadata_manager.get_metadata(file_id).await {
                proto_entries.push(chuangshi_common::proto::FileMetadata {
                    file_id: metadata.file_id.to_string(),
                    path,
                    size: metadata.size,
                    created_at: metadata.created_at.timestamp(),
                    modified_at: metadata.modified_at.timestamp(),
                    owner: metadata.owner,
                    permissions: metadata.permissions,
                    replicas: metadata.replicas.iter().map(|r| chuangshi_common::proto::ReplicaInfo {
                        datacenter_id: r.datacenter_id.clone(),
                        rmn_address: r.rmn_address.clone(),
                        dn_addresses: r.dn_addresses.clone(),
                        is_primary: r.is_primary,
                        status: format!("{:?}", r.status),
                    }).collect(),
                    is_complete: metadata.is_complete,
                });
            }
        }
        
        Ok(Response::new(ListDirectoryResponse {
            entries: proto_entries,
        }))
    }
    
    async fn update_namespace(
        &self,
        request: Request<UpdateNamespaceRequest>,
    ) -> std::result::Result<Response<UpdateNamespaceResponse>, Status> {
        let req = request.into_inner();
        
        if req.is_delete {
            self.namespace_manager.remove_file(&req.path).await
                .map_err(|e| Status::internal(format!("Failed to remove from namespace: {}", e)))?;
        } else {
            let file_id = Uuid::parse_str(&req.file_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid file_id: {}", e)))?;
            self.namespace_manager.add_file(&req.path, file_id).await
                .map_err(|e| Status::internal(format!("Failed to add to namespace: {}", e)))?;
        }
        
        Ok(Response::new(UpdateNamespaceResponse {
            success: true,
        }))
    }
    
    async fn report_rmn_status(
        &self,
        request: Request<RmnStatusReport>,
    ) -> std::result::Result<Response<Empty>, Status> {
        let report = request.into_inner();
        debug!("Received RMN status report from {}", report.datacenter_id);
        
        // 更新数据中心状态
        if let Some(mut dc) = self.datacenters.get_mut(&report.datacenter_id) {
            dc.last_heartbeat = Utc::now();
            if dc.status == DataCenterStatus::Offline || dc.status == DataCenterStatus::Degraded {
                info!("Datacenter {} is back online", report.datacenter_id);
                dc.status = DataCenterStatus::Online;
            }
            
            // 更新负载信息
            let used = (dc.capacity.total as f32 * report.storage_usage) as u64;
            dc.capacity.used = used;
            dc.capacity.available = dc.capacity.total - used;
        } else {
            warn!("Unknown datacenter in status report: {}", report.datacenter_id);
        }
        
        // 更新健康监控
        self.health_monitor.update_rmn_status(
            report.datacenter_id,
            report.cpu_usage,
            report.memory_usage,
            report.storage_usage,
        );
        
        Ok(Response::new(Empty {}))
    }
}