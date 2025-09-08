use chuangshi_common::types::FileMetadata;
use dashmap::DashMap;
use uuid::Uuid;

pub struct MetadataCache {
    cache: DashMap<Uuid, FileMetadata>,
    path_cache: DashMap<String, Uuid>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            path_cache: DashMap::new(),
        }
    }
    
    pub fn update(&self, file_id: Uuid, metadata: FileMetadata) {
        self.path_cache.insert(metadata.path.clone(), file_id);
        self.cache.insert(file_id, metadata);
    }
    
    pub fn get(&self, file_id: Uuid) -> Option<FileMetadata> {
        self.cache.get(&file_id).map(|entry| entry.clone())
    }
    
    pub fn get_by_path(&self, path: &str) -> Option<FileMetadata> {
        self.path_cache.get(path)
            .and_then(|file_id| self.cache.get(&file_id))
            .map(|entry| entry.clone())
    }
    
    pub fn remove(&self, file_id: Uuid) {
        if let Some(metadata) = self.cache.remove(&file_id) {
            self.path_cache.remove(&metadata.1.path);
        }
    }
    
    pub fn clear(&self) {
        self.cache.clear();
        self.path_cache.clear();
    }
}