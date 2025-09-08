use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;
use chuangshi_common::utils::*;

pub struct StorageEngine {
    data_dir: PathBuf,
    capacity_total: u64,
    capacity_used: AtomicU64,
    block_count: AtomicU32,
    io_operations: AtomicU64,
}

impl StorageEngine {
    pub async fn new(data_dir: &str, capacity: u64) -> Result<Self> {
        let data_path = Path::new(data_dir);
        
        // 创建存储目录
        fs::create_dir_all(data_path.join("blocks")).await?;
        fs::create_dir_all(data_path.join("shards")).await?;
        fs::create_dir_all(data_path.join("temp")).await?;
        
        // 计算已使用空间
        let used = Self::calculate_used_space(data_path).await?;
        
        Ok(Self {
            data_dir: data_path.to_path_buf(),
            capacity_total: capacity,
            capacity_used: AtomicU64::new(used),
            block_count: AtomicU32::new(0),
            io_operations: AtomicU64::new(0),
        })
    }
    
    pub async fn write_block(&self, block_id: Uuid, data: &[u8]) -> Result<()> {
        let block_path = self.data_dir.join("blocks").join(format!("{}.block", block_id));
        
        // 写入临时文件
        let temp_path = self.data_dir.join("temp").join(format!("{}.tmp", block_id));
        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        
        // 原子性重命名
        fs::rename(temp_path, block_path).await?;
        
        // 更新统计
        self.capacity_used.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.block_count.fetch_add(1, Ordering::Relaxed);
        self.io_operations.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }
    
    pub async fn read_block(&self, block_id: Uuid) -> Result<Vec<u8>> {
        let block_path = self.data_dir.join("blocks").join(format!("{}.block", block_id));
        
        let mut file = fs::File::open(block_path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        
        self.io_operations.fetch_add(1, Ordering::Relaxed);
        
        Ok(data)
    }
    
    pub async fn delete_block(&self, block_id: Uuid) -> Result<()> {
        let block_path = self.data_dir.join("blocks").join(format!("{}.block", block_id));
        
        if block_path.exists() {
            let metadata = fs::metadata(&block_path).await?;
            let size = metadata.len();
            
            fs::remove_file(block_path).await?;
            
            self.capacity_used.fetch_sub(size, Ordering::Relaxed);
            self.block_count.fetch_sub(1, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    pub async fn write_shard(&self, shard_id: &str, data: &[u8]) -> Result<()> {
        let shard_path = self.data_dir.join("shards").join(format!("{}.shard", shard_id));
        
        let mut file = fs::File::create(shard_path).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        
        self.capacity_used.fetch_add(data.len() as u64, Ordering::Relaxed);
        
        Ok(())
    }
    
    pub async fn read_shard(&self, shard_id: &str) -> Result<Vec<u8>> {
        let shard_path = self.data_dir.join("shards").join(format!("{}.shard", shard_id));
        
        let mut file = fs::File::open(shard_path).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        
        Ok(data)
    }
    
    pub async fn delete_shard(&self, shard_id: &str) -> Result<()> {
        let shard_path = self.data_dir.join("shards").join(format!("{}.shard", shard_id));
        
        if shard_path.exists() {
            let metadata = fs::metadata(&shard_path).await?;
            let size = metadata.len();
            
            fs::remove_file(shard_path).await?;
            
            self.capacity_used.fetch_sub(size, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    pub async fn get_capacity(&self) -> (u64, u64) {
        (self.capacity_total, self.capacity_used.load(Ordering::Relaxed))
    }
    
    pub async fn get_block_count(&self) -> u32 {
        self.block_count.load(Ordering::Relaxed)
    }
    
    pub async fn get_io_load(&self) -> f32 {
        let ops = self.io_operations.swap(0, Ordering::Relaxed);
        // 简化计算：操作数/1000作为负载值
        (ops as f32 / 1000.0).min(1.0)
    }
    
    pub async fn cleanup_temp_files(&self) -> Result<()> {
        let temp_dir = self.data_dir.join("temp");
        let mut entries = fs::read_dir(temp_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Ok(metadata) = fs::metadata(&path).await {
                if let Ok(modified) = metadata.modified() {
                    if let Ok(elapsed) = std::time::SystemTime::now().duration_since(modified) {
                        // 删除超过1小时的临时文件
                        if elapsed.as_secs() > 3600 {
                            fs::remove_file(path).await.ok();
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn verify_blocks(&self) -> Result<()> {
        let blocks_dir = self.data_dir.join("blocks");
        let mut entries = fs::read_dir(blocks_dir).await?;
        let mut verified_count = 0;
        
        while let Some(entry) = entries.next_entry().await? {
            if verified_count >= 100 { // 每次最多验证100个块
                break;
            }
            
            let path = entry.path();
            if let Some(name) = path.file_stem() {
                if let Some(block_id_str) = name.to_str() {
                    if let Ok(block_id) = Uuid::parse_str(block_id_str) {
                        // 读取并验证校验和
                        if let Ok(data) = self.read_block(block_id).await {
                            let _checksum = calculate_checksum(&data);
                            // TODO: 与存储的校验和比较
                            verified_count += 1;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn calculate_used_space(data_dir: &Path) -> Result<u64> {
        let mut total_size = 0u64;
        
        for subdir in &["blocks", "shards"] {
            let dir_path = data_dir.join(subdir);
            if dir_path.exists() {
                let mut entries = fs::read_dir(dir_path).await?;
                while let Some(entry) = entries.next_entry().await? {
                    if let Ok(metadata) = entry.metadata().await {
                        total_size += metadata.len();
                    }
                }
            }
        }
        
        Ok(total_size)
    }
}