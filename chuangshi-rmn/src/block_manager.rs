use anyhow::Result;
use chuangshi_common::{types::*, utils::*};
use dashmap::DashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use uuid::Uuid;

pub struct BlockManager {
    blocks: DashMap<Uuid, Vec<BlockInfo>>,
    data_dir: String,
    storage_used: AtomicU64,
}

impl BlockManager {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let blocks_dir = Path::new(data_dir).join("blocks");
        fs::create_dir_all(&blocks_dir).await?;
        
        Ok(Self {
            blocks: DashMap::new(),
            data_dir: blocks_dir.to_string_lossy().to_string(),
            storage_used: AtomicU64::new(0),
        })
    }
    
    pub async fn create_block_map(&self, metadata: &FileMetadata) -> Result<()> {
        let mut blocks = Vec::new();
        let block_size = metadata.block_size;
        let mut remaining_size = metadata.size;
        let mut index = 0;
        
        // 查找主副本的DN地址
        let primary_replica = metadata.replicas.iter()
            .find(|r| r.is_primary)
            .ok_or_else(|| anyhow::anyhow!("No primary replica found"))?;
        
        let dn_addresses = &primary_replica.dn_addresses;
        
        while remaining_size > 0 {
            let current_block_size = remaining_size.min(block_size);
            let block_id = generate_block_id(&metadata.file_id, index);
            
            // 轮询选择DN
            let dn_address = &dn_addresses[index as usize % dn_addresses.len()];
            
            let block = BlockInfo {
                block_id,
                file_id: metadata.file_id,
                index,
                size: current_block_size,
                checksum: vec![], // 将在实际写入时计算
                dn_address: dn_address.clone(),
                erasure_shards: vec![], // 将在实际写入时生成
            };
            
            blocks.push(block);
            remaining_size = remaining_size.saturating_sub(current_block_size);
            index += 1;
        }
        
        // 保存块映射
        self.blocks.insert(metadata.file_id, blocks);
        
        // 持久化
        self.save_block_map(metadata.file_id).await?;
        
        Ok(())
    }
    
    pub async fn get_blocks(&self, file_id: Uuid) -> Result<Vec<BlockInfo>> {
        if let Some(blocks) = self.blocks.get(&file_id) {
            return Ok(blocks.clone());
        }
        
        // 尝试从磁盘加载
        self.load_block_map(file_id).await
    }
    
    pub async fn delete_blocks(&self, file_id: Uuid) -> Result<()> {
        self.blocks.remove(&file_id);
        
        // 删除持久化文件
        let path = Path::new(&self.data_dir).join(format!("{}.blocks", file_id));
        fs::remove_file(path).await.ok();
        
        Ok(())
    }
    
    pub async fn get_storage_usage(&self) -> f32 {
        let used = self.storage_used.load(Ordering::Relaxed);
        let total = 10 * 1024 * 1024 * 1024 * 1024; // 10TB
        used as f32 / total as f32
    }
    
    async fn save_block_map(&self, file_id: Uuid) -> Result<()> {
        if let Some(blocks) = self.blocks.get(&file_id) {
            let path = Path::new(&self.data_dir).join(format!("{}.blocks", file_id));
            let data = bincode::serialize(&blocks.clone())?;
            fs::write(path, data).await?;
        }
        Ok(())
    }
    
    async fn load_block_map(&self, file_id: Uuid) -> Result<Vec<BlockInfo>> {
        let path = Path::new(&self.data_dir).join(format!("{}.blocks", file_id));
        let data = fs::read(path).await?;
        let blocks: Vec<BlockInfo> = bincode::deserialize(&data)?;
        
        self.blocks.insert(file_id, blocks.clone());
        Ok(blocks)
    }
    pub async fn remove_block_record(&self, block_id: Uuid) -> Result<()> {
        // 遍历所有文件的块映射，找到并删除该块
        for mut entry in self.blocks.iter_mut() {
            let file_id = *entry.key();
            let blocks = entry.value_mut();
            
            // 查找要删除的块
            let original_len = blocks.len();
            blocks.retain(|b| b.block_id != block_id);
            
            if blocks.len() < original_len {
                // 找到并删除了块，需要更新持久化
                let updated_blocks = blocks.clone();
                drop(entry); // 释放锁
                
                // 更新内存中的映射
                self.blocks.insert(file_id, updated_blocks);
                
                // 持久化更新
                self.save_block_map(file_id).await?;
                
                // 更新存储使用量（假设每个块256MB）
                let removed_size = 256 * 1024 * 1024;
                self.storage_used.fetch_sub(removed_size, Ordering::Relaxed);
                
                tracing::info!("Removed block {} from file {}", block_id, file_id);
                return Ok(());
            }
        }
        
        // 如果没找到块，不算错误（可能已经删除）
        tracing::debug!("Block {} not found in block manager", block_id);
        Ok(())
    }
}