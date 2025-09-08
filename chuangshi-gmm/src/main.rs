use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{info, error};

mod service;
mod metadata;
mod policy;
mod namespace;
mod health;

use service::GmmServiceImpl;

#[derive(Parser, Debug)]
#[clap(name = "chuangshi-gmm", about = "Chuangshi NDFS Global Metadata Master")]
struct Args {
    /// Bind address for the GMM service
    #[clap(short, long, default_value = "0.0.0.0:8001")]
    bind_addr: String,
    
    /// Data directory for persistent storage
    #[clap(short, long, default_value = "./data/gmm")]
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
        .with_env_filter(format!("chuangshi_gmm={},chuangshi_common={}", log_level, log_level))
        .init();
    
    info!("Starting Chuangshi GMM");
    info!("Bind address: {}", args.bind_addr);
    info!("Data directory: {}", args.data_dir);
    
    // 创建数据目录
    std::fs::create_dir_all(&args.data_dir)?;
    
    // 创建服务实例
    let service = Arc::new(GmmServiceImpl::new(args.data_dir.clone()).await?);
    
    // 启动健康检查任务
    let health_service = service.clone();
    tokio::spawn(async move {
        health_service.run_health_checker().await;
    });
    
    // 启动策略执行器
    let policy_service = service.clone();
    tokio::spawn(async move {
        policy_service.run_policy_executor().await;
    });
    
    // 解析地址
    let addr: SocketAddr = args.bind_addr.parse()?;
    
    // 启动gRPC服务
    info!("GMM service listening on {}", addr);
    
    // Server::builder()
    //     .add_service(
    //         chuangshi_common::proto::gmm_service_server::GmmServiceServer::new(
    //             service.clone()
    //         )
    //     )
    //     .serve(addr)
    //     .await?;

    Server::builder()
    .add_service(chuangshi_common::proto::gmm_service_server::GmmServiceServer::new((*service).clone())) // 裸 impl
    .serve(addr)
    .await?;

    
    Ok(())
}