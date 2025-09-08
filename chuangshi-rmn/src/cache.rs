use anyhow::Result;
use chuangshi_common::types::*;
use dashmap::DashMap;
use std::path::Path;
use tokio::fs;
use uuid::Uuid;

pub struct MetadataCache {
    cache: DashMap<Uuid, FileMetadata>,
    data_dir: String,
}

impl MetadataCache {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let cache_dir = Path::new(data_dir).join("cache");
        fs::create_dir_all(&cache_dir).await?;
        
        Ok(Self {
            cache: DashMap::new(),
            data_dir: cache_dir.to_string_lossy().to_string(),
        })
    }
    
    pub fn update(&self, file_id: Uuid, metadata: FileMetadata) {
        self.cache.insert(file_id, metadata);
    }
    
    pub fn get(&self, file_id: Uuid) -> Option<FileMetadata> {
        self.cache.get(&file_id).map(|entry| entry.clone())
    }
    
    pub fn remove(&self, file_id: Uuid) {
        self.cache.remove(&file_id);
    }
    
    pub fn get_by_path(&self, path: &str) -> Option<FileMetadata> {
        for entry in self.cache.iter() {
            if entry.path == path {
                return Some(entry.clone());
            }
        }
        None
    }
    
    pub fn list_all(&self) -> Vec<FileMetadata> {
        self.cache.iter().map(|entry| entry.clone()).collect()
    }
}