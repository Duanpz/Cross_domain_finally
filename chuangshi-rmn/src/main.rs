use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::{Server, Channel};
use tracing::{info, error, warn};
use chuangshi_common::proto::rmn_service_server::RmnServiceServer;
mod service;
mod cache;
mod block_manager;

use service::RmnServiceImpl;

#[derive(Parser, Debug)]
#[clap(name = "chuangshi-rmn", about = "Chuangshi NDFS Regional Metadata Node")]
struct Args {
    /// Bind address for the RMN service
    #[clap(short, long, default_value = "0.0.0.0:8002")]
    bind_addr: String,
    
    /// Datacenter ID
    #[clap(short = 'd', long)]
    datacenter_id: String,
    
    /// GMM address
    #[clap(short = 'g', long)]
    gmm_addr: String,
    
    /// Data directory
    #[clap(long, default_value = "./data/rmn")]
    data_dir: String,
    
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
        .with_env_filter(format!("chuangshi_rmn={},chuangshi_common={}", log_level, log_level))
        .init();
    
    info!("Starting Chuangshi RMN for datacenter: {}", args.datacenter_id);
    info!("Bind address: {}", args.bind_addr);
    info!("GMM address: {}", args.gmm_addr);
    info!("Data directory: {}", args.data_dir);
    
    // 创建数据目录
    std::fs::create_dir_all(&args.data_dir)?;
    
    // 连接到GMM
    let gmm_channel = Channel::from_shared(format!("http://{}", args.gmm_addr))?
        .connect()
        .await?;
    
    // 创建服务实例
    let service = Arc::new(
        RmnServiceImpl::new(
            args.datacenter_id.clone(),
            args.data_dir.clone(),
            gmm_channel.clone(),
        ).await?
    );
    
    // 启动心跳任务
    let heartbeat_service = service.clone();
    let datacenter_id = args.datacenter_id.clone();
    let bind_addr = args.bind_addr.clone();
    tokio::spawn(async move {
        heartbeat_service.run_heartbeat(datacenter_id, bind_addr).await;
    });
    
    // 启动DN健康检查
    let health_service = service.clone();
    tokio::spawn(async move {
        health_service.run_dn_health_check().await;
    });
    
    // 解析地址
    let addr: SocketAddr = args.bind_addr.parse()?;
    
    // 启动 gRPC 服务
    info!("RMN service listening on {}", addr);

    Server::builder()
        .add_service(RmnServiceServer::new((*service).clone())) // 关键：解引用再克隆
        .serve(addr)
        .await?;

    Ok(())
}