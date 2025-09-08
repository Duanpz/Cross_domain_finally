use anyhow::Result;
use chuangshi_common::types::*;
use dashmap::DashMap;
use std::path::Path;
use uuid::Uuid;
use tokio::fs;

pub struct MetadataManager {
    cache: DashMap<Uuid, FileMetadata>,
    data_dir: String,
}

impl MetadataManager {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let metadata_dir = Path::new(data_dir).join("metadata");
        fs::create_dir_all(&metadata_dir).await?;
        
        let manager = Self {
            cache: DashMap::new(),
            data_dir: metadata_dir.to_string_lossy().to_string(),
        };
        
        // 加载已有元数据
        manager.load_existing_metadata().await?;
        
        Ok(manager)
    }
    
    async fn load_existing_metadata(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.data_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.path().extension() == Some(std::ffi::OsStr::new("meta")) {
                if let Ok(data) = fs::read(entry.path()).await {
                    if let Ok(metadata) = bincode::deserialize::<FileMetadata>(&data) {
                        self.cache.insert(metadata.file_id, metadata);
                    }
                }
            }
        }
        
        tracing::info!("Loaded {} metadata entries", self.cache.len());
        Ok(())
    }
    
    pub async fn save_metadata(&self, metadata: &FileMetadata) -> Result<()> {
        // 保存到缓存
        self.cache.insert(metadata.file_id, metadata.clone());
        
        // 持久化到磁盘
        let path = Path::new(&self.data_dir).join(format!("{}.meta", metadata.file_id));
        let data = bincode::serialize(metadata)?;
        fs::write(path, data).await?;
        
        Ok(())
    }
    
    pub async fn get_metadata(&self, file_id: Uuid) -> Result<FileMetadata> {
        if let Some(metadata) = self.cache.get(&file_id) {
            return Ok(metadata.clone());
        }
        
        // 尝试从磁盘加载
        let path = Path::new(&self.data_dir).join(format!("{}.meta", file_id));
        let data = fs::read(path).await?;
        let metadata: FileMetadata = bincode::deserialize(&data)?;
        
        self.cache.insert(file_id, metadata.clone());
        Ok(metadata)
    }
    
    pub async fn delete_metadata(&self, file_id: Uuid) -> Result<()> {
        self.cache.remove(&file_id);
        
        let path = Path::new(&self.data_dir).join(format!("{}.meta", file_id));
        fs::remove_file(path).await?;
        
        Ok(())
    }
    
    pub async fn mark_deleting(&self, file_id: Uuid) -> Result<()> {
        if let Some(mut metadata) = self.cache.get_mut(&file_id) {
            for replica in &mut metadata.replicas {
                replica.status = ReplicaStatus::Deleting;
            }
            
            // 保存更新
            let updated = metadata.clone();
            drop(metadata);
            self.save_metadata(&updated).await?;
        }
        
        Ok(())
    }
    
    pub async fn mark_complete(&self, file_id: Uuid) -> Result<()> {
        if let Some(mut metadata) = self.cache.get_mut(&file_id) {
            metadata.is_complete = true;
            metadata.modified_at = chrono::Utc::now();
            
            for replica in &mut metadata.replicas {
                if replica.status == ReplicaStatus::Creating {
                    replica.status = ReplicaStatus::Ready;
                }
            }
            
            let updated = metadata.clone();
            drop(metadata);
            self.save_metadata(&updated).await?;
        }
        
        Ok(())
    }
    
    pub async fn list_all(&self) -> Vec<FileMetadata> {
        self.cache.iter().map(|entry| entry.value().clone()).collect()
    }
}