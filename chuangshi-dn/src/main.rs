use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::{Server, Channel};
use tracing::{info, error};

mod service;
mod storage;
mod erasure_coding;
mod replication;

use service::DnServiceImpl;

#[derive(Parser, Debug)]
#[clap(name = "chuangshi-dn", about = "Chuangshi NDFS Data Node")]
struct Args {
    /// Bind address for the DN service
    #[clap(short, long, default_value = "0.0.0.0:8003")]
    bind_addr: String,
    
    /// Datacenter ID
    #[clap(short = 'd', long)]
    datacenter_id: String,
    
    /// RMN address
    #[clap(short = 'r', long)]
    rmn_addr: String,
    
    /// Data directory
    #[clap(long, default_value = "./data/dn")]
    data_dir: String,
    
    /// Storage capacity in bytes
    #[clap(long, default_value = "10737418240")] // 10GB
    storage_capacity: u64,
    
    /// Enable debug logging
    #[clap(short = 'v', long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // 初始化日志
    let log_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("chuangshi_dn={},chuangshi_common={}", log_level, log_level))
        .init();
    
    info!("Starting Chuangshi DN for datacenter: {}", args.datacenter_id);
    info!("Bind address: {}", args.bind_addr);
    info!("RMN address: {}", args.rmn_addr);
    info!("Data directory: {}", args.data_dir);
    info!("Storage capacity: {} bytes", args.storage_capacity);
    
    // 创建数据目录
    std::fs::create_dir_all(&args.data_dir)?;
    
    // 连接到RMN
    let rmn_channel = Channel::from_shared(format!("http://{}", args.rmn_addr))?
        .connect()
        .await?;
    
    // 创建服务实例
    let service = Arc::new(
        DnServiceImpl::new(
            args.datacenter_id.clone(),
            args.data_dir.clone(),
            args.storage_capacity,
            rmn_channel.clone(),
        ).await?
    );
    
    // 启动心跳任务
    let heartbeat_service = service.clone();
    let dn_address = args.bind_addr.clone();
    tokio::spawn(async move {
        heartbeat_service.run_heartbeat(dn_address).await;
    });
    
    // 启动存储清理任务
    let cleanup_service = service.clone();
    tokio::spawn(async move {
        cleanup_service.run_storage_cleanup().await;
    });
    
    // 解析地址
    let addr: SocketAddr = args.bind_addr.parse()?;
    
    // 启动gRPC服务
    info!("DN service listening on {}", addr);
    
    Server::builder()
        .add_service(chuangshi_common::proto::dn_service_server::DnServiceServer::new((*service).clone())) // 裸 impl
        .serve(addr)
        .await?;

        
    Ok(())
}