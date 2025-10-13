use anyhow::{Result, bail};
use chuangshi_common::{
    proto::{
        gmm_service_client::GmmServiceClient,
        dn_service_client::DnServiceClient,
        rmn_service_client::RmnServiceClient,
        *,
    },
    types::*,
    utils::*,
};


// use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{transport::Channel, Request};
use uuid::Uuid;
use indicatif::{ProgressBar, ProgressStyle};
// use tracing::{info, debug, warn};

use crate::cache::MetadataCache;

const CHUNK_SIZE: usize = 256 * 1024 * 1024; // 256MB

pub struct ChuangshiClient {
    gmm_client: GmmServiceClient<Channel>,
    metadata_cache: MetadataCache,
}

impl ChuangshiClient {
    pub async fn new(gmm_addr: &str) -> Result<Self> {
        let channel = Channel::from_shared(format!("http://{}", gmm_addr))?
            .connect()
            .await?;
        Ok(Self {
            gmm_client: GmmServiceClient::new(channel),
            metadata_cache: MetadataCache::new(),
        })
    }
    
    pub async fn upload_file(
        &self,
        local_path: &str,
        remote_path: &str,
        tags: Vec<String>,
    ) -> Result<chuangshi_common::types::FileMetadata> {
        // 获取文件信息
        let file_meta = tokio::fs::metadata(local_path).await?;
        let file_size = file_meta.len();
        // 创建进度条
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        
        // 请求GMM创建文件
        let mut client = self.gmm_client.clone();
        let response = client.create_file(Request::new(CreateFileRequest {
            path: remote_path.to_string(),
            size: file_size,
            tags,
        })).await?;
        
        let resp = response.into_inner();
        let file_id = Uuid::parse_str(&resp.file_id)?;
        let dn_addresses = resp.dn_addresses;
        // 打开本地文件
        let mut file = File::open(local_path).await?;
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut block_index = 0u64;
        println!("DN ready");
        // 分块上传到DN
        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            
            let block_data = buffer[..n].to_vec();
            let block_id = generate_block_id(&file_id, block_index);
            
            // 选择DN（简单轮询）
            let dn_addr = &dn_addresses[block_index as usize % dn_addresses.len()];
                // ① 打日志：要发多少字节到哪个 DN
            println!("[Client] sending block {} ({} bytes) to {}", block_id, n, dn_addr);
            // 上传块到DN
            self.upload_block_to_dn(dn_addr, block_id, block_data).await?;
             // ② 打日志：DN 返回成功
            println!("[Client] block {} uploaded ok", block_id);
            pb.inc(n as u64);
            block_index += 1;
        }
        
        pb.finish_with_message("Upload complete");
        
        // 获取最终元数据
        self.get_file_info(remote_path).await
    }
    
    pub async fn download_file(&self, remote_path: &str, local_path: &str) -> Result<()> {
        // 获取文件元数据
        let metadata = self.get_file_info(remote_path).await?;
        
        // 创建进度条
        let pb = ProgressBar:: new(metadata.size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        
        // 获取块映射
        let blocks = self.get_block_map(&metadata).await?;
        
        // 创建本地文件
        let mut file = File::create(local_path).await?;
        
        // 下载并写入每个块                                         
        for block in blocks {                                                           
            let data = self.download_block_from_dn(&block.dn_address, block.block_id).await?;
            file.write_all(&data).await?;
            pb.inc(data.len() as u64);
        }
        
        pb.finish_with_message("Download complete");
        
        Ok(())
    }
    
    pub async fn delete_file(&self, remote_path: &str) -> Result<()> {
        let mut client = self.gmm_client.clone();
        let response = client.delete_file(Request::new(DeleteFileRequest {
            path: remote_path.to_string(),
        })).await?;
        
        let resp = response.into_inner();
        if !resp.success {
            bail!("Failed to delete file");
        }
        
        Ok(())
    }
    
    pub async fn list_directory(&self, path: &str) -> Result<Vec<chuangshi_common::types::FileMetadata>> {
        let mut client = self.gmm_client.clone();
        let response = client.list_directory(Request::new(ListDirectoryRequest {
            path: path.to_string(),
        })).await?;
        
        let resp = response.into_inner();
        
        // 转换为内部格式
        let mut entries = Vec::new();
        for proto_meta in resp.entries {
            let metadata = self.proto_to_metadata(proto_meta)?;
            entries.push(metadata);
        }
        
        Ok(entries)
    }
    
    pub async fn get_file_info(&self, remote_path: &str) -> Result<chuangshi_common::types::FileMetadata> {
        // 检查缓存
        if let Some(metadata) = self.metadata_cache.get_by_path(remote_path) {
            return Ok(metadata);
        }
        
        let mut client = self.gmm_client.clone();
        let response = client.get_file(Request::new(GetFileRequest {
            path: remote_path.to_string(),
        })).await?;
        
        let resp = response.into_inner();
        let proto_meta = resp.metadata.ok_or_else(|| anyhow::anyhow!("No metadata returned"))?;
        let metadata = self.proto_to_metadata(proto_meta)?;
        
        // 更新缓存
        let uuid = metadata.file_id;
        self.metadata_cache.update(uuid, metadata.clone());
        
        Ok(metadata)
    }
    
    // async fn upload_block_to_dn(
    //     &self,
    //     dn_addr: &str,
    //     block_id: Uuid,
    //     data: Vec<u8>,
    // ) -> Result<()> {
    //     let channel = Channel::from_shared(format!("http://{}", dn_addr))?
    //         .connect()
    //         .await?;
        
    //     let mut client = DnServiceClient::new(channel);
        
    //     let response = client.write_block(Request::new(WriteBlockRequest {
    //         block_id: block_id.to_string(),
    //         data,
    //         is_replica: false,
    //     })).await?;
        
    //     let resp = response.into_inner();
    //     if !resp.success {
    //         bail!("Failed to write block to DN");
    //     }
        
    //     Ok(())
    // }
    async fn upload_block_to_dn(
        &self,
        dn_addr: &str,
        block_id: Uuid,
        data: Vec<u8>,
    ) -> Result<()> {
        // ① 连接前
        println!("[Client] connecting to DN at http://{}", dn_addr);

        let channel = Channel::from_shared(format!("http://{}", dn_addr))?
            .connect()
            .await?;
        // ② 连接成功
        println!("[Client] connected to DN, now writing block {}", block_id);

        let mut client = DnServiceClient::new(channel);
        let response = client
            .write_block(Request::new(WriteBlockRequest {
                block_id: block_id.to_string(),
                data,
                is_replica: false,
            }))
            .await?;

        // ③ 收到响应
        println!("[Client] write_block returned: success={}", response.into_inner().success);
        Ok(())
    }

    
    async fn download_block_from_dn(
        &self,
        dn_addr: &str,
        block_id: Uuid,
    ) -> Result<Vec<u8>> {
        let channel = Channel::from_shared(format!("http://{}", dn_addr))?
            .connect()
            .await?;
        
        let mut client = DnServiceClient::new(channel);
        
        let response = client.read_block(Request::new(ReadBlockRequest {
            block_id: block_id.to_string()
        })).await?;
        
        Ok(response.into_inner().data)
    }
    
    async fn get_block_map(&self, metadata: &chuangshi_common::types::FileMetadata) -> Result<Vec<chuangshi_common::types::BlockInfo>> {
        // 选择主副本的RMN
        let primary_replica = metadata.replicas.iter()
            .find(|r| r.is_primary)
            .ok_or_else(|| anyhow::anyhow!("No primary replica found"))?;
        
        let channel = Channel::from_shared(format!("http://{}", primary_replica.rmn_address))?
            .connect()
            .await?;
        
        let mut client = RmnServiceClient::new(channel);
        
        let response = client.get_block_map(Request::new(GetBlockMapRequest {
            file_id: metadata.file_id.to_string(),
        })).await?;
        
        let resp = response.into_inner();
        
        // 转换为内部格式
        let mut blocks:Vec<chuangshi_common::types::BlockInfo> = Vec::new();
        for proto_block in resp.blocks {
            blocks.push(chuangshi_common::types::BlockInfo {
                block_id: Uuid::parse_str(&proto_block.block_id)?,
                file_id: Uuid::parse_str(&proto_block.file_id)?,
                index: proto_block.index,
                size: proto_block.size,
                checksum: proto_block.checksum,
                dn_address: proto_block.dn_address,
                erasure_shards: vec![], // 简化处理
            });
        }
        // let mut blocks:Vec<chuangshi_common::proto::BlockInfo> = Vec::new();
        // for proto_block: in resp.blocks {
        //     blocks.push(BlockInfo {
        //         block_id: Uuid::parse_str(&proto_block.block_id)?,
        //         file_id: Uuid::parse_str(&proto_block.file_id)?,
        //         index: proto_block.index,
        //         size: proto_block.size,
        //         checksum: proto_block.checksum,
        //         dn_address: proto_block.dn_address,
        //         erasure_shards: vec![], // 简化处理
        //     });
        // }
        
        Ok(blocks)
    }
    
    fn proto_to_metadata(&self, proto: chuangshi_common::proto::FileMetadata) -> Result<chuangshi_common::types::FileMetadata> {
        Ok(chuangshi_common::types::FileMetadata {
            file_id: Uuid::parse_str(&proto.file_id)?,
            path: proto.path,
            size: proto.size,
            created_at: chrono::DateTime::from_timestamp(proto.created_at, 0)
                .unwrap_or_else(|| chrono::Utc::now()),
            modified_at: chrono::DateTime::from_timestamp(proto.modified_at, 0)
                .unwrap_or_else(|| chrono::Utc::now()),
            owner: proto.owner,
            permissions: proto.permissions,
            replicas: proto.replicas.into_iter().map(|r: chuangshi_common::proto::ReplicaInfo| chuangshi_common::types::ReplicaInfo {
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
            is_complete: proto.is_complete,
            block_size: 256 * 1024 * 1024,
            block_count: (proto.size + 256 * 1024 * 1024 - 1) / (256 * 1024 * 1024),
        })
    }
}