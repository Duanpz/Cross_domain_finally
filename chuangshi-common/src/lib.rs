pub mod types;
pub mod utils;
pub mod error;

// Re-export generated protobuf code
pub mod proto {
    tonic::include_proto!("chuangshi");
}

pub use error::ChuangshiError;
pub type Result<T> = std::result::Result<T, ChuangshiError>;