use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChuangshiError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Metadata error: {0}")]
    Metadata(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("File not found: {0}")]
    FileNotFound(String),
    
    #[error("Path not found: {0}")]
    PathNotFound(String),
    
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    
    #[error("Erasure coding error: {0}")]
    ErasureCoding(String),
    
    #[error("Replication error: {0}")]
    Replication(String),
    
    #[error("Policy error: {0}")]
    Policy(String),
    
    #[error("DataCenter offline: {0}")]
    DataCenterOffline(String),
    
    #[error("RMN error: {0}")]
    Rmn(String),
    
    #[error("DN error: {0}")]
    Dn(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Other error: {0}")]
    Other(String),
}