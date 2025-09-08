use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::Path;
use tracing::{info, error};

mod sdk;
mod cache;

use sdk::ChuangshiClient;

#[derive(Parser)]
#[clap(name = "chuangshi", version = "0.1.0")]
#[clap(about = "Chuangshi NDFS Client", long_about = None)]
struct Cli {
    /// GMM server address
    #[clap(short, long, default_value = "localhost:8001")]
    gmm_addr: String,
    
    /// Enable debug output
    #[clap(short = 'v', long)]
    verbose: bool,
    
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload a file to the cluster
    Put {
        /// Local file path
        #[clap(value_name = "LOCAL_FILE")]
        local_path: String,
        
        /// Remote path in NDFS
        #[clap(value_name = "REMOTE_PATH")]
        remote_path: String,
        
        /// Tags for the file
        #[clap(short, long)]
        tags: Vec<String>,
    },
    
    /// Download a file from the cluster
    Get {
        /// Remote path in NDFS
        #[clap(value_name = "REMOTE_PATH")]
        remote_path: String,
        
        /// Local file path
        #[clap(value_name = "LOCAL_FILE")]
        local_path: String,
    },
    
    /// Delete a file from the cluster
    Delete {
        /// Remote path in NDFS
        #[clap(value_name = "REMOTE_PATH")]
        remote_path: String,
        
        /// Force delete without confirmation
        #[clap(short, long)]
        force: bool,
    },
    
    /// List directory contents
    Ls {
        /// Directory path
        #[clap(value_name = "PATH", default_value = "/chuangshi")]
        path: String,
        
        /// Long format listing
        #[clap(short, long)]
        long: bool,
    },
    
    /// Show file information
    Info {
        /// Remote path in NDFS
        #[clap(value_name = "REMOTE_PATH")]
        remote_path: String,
    },
    
    /// Create a directory
    Mkdir {
        /// Directory path
        #[clap(value_name = "PATH")]
        path: String,
    },
    
    /// Show cluster status
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    print!("Client beginning\n");
    // 初始化日志
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("chuangshi_client={}", log_level))
        .init();
    
    // 创建客户端
    let client = ChuangshiClient::new(&cli.gmm_addr).await?;
    
    // 执行命令
    match cli.command {
        Commands::Put { local_path, remote_path, tags } => {
            // 验证本地文件
            if !Path::new(&local_path).exists() {
                error!("Local file not found: {}", local_path);
                std::process::exit(1);
            }
            
            // 验证远程路径
            if !remote_path.starts_with("/chuangshi") {
                error!("Remote path must start with /chuangshi");
                std::process::exit(1);
            }
            
            println!("Uploading {} to {}", local_path, remote_path);
            
            match client.upload_file(&local_path, &remote_path, tags).await {
                Ok(metadata) => {
                    println!("✓ File uploaded successfully!");
                    println!("  File ID: {}", metadata.file_id);
                    println!("  Size: {}", format_bytes(metadata.size));
                    println!("  Primary DC: {}", metadata.replicas[0].datacenter_id);
                }
                Err(e) => {
                    error!("Failed to upload file: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        Commands::Get { remote_path, local_path } => {
            println!("Downloading {} to {}", remote_path, local_path);
            
            match client.download_file(&remote_path, &local_path).await {
                Ok(_) => {
                    println!("✓ File downloaded successfully!");
                }
                Err(e) => {
                    error!("Failed to download file: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        Commands::Delete { remote_path, force } => {
            if !force {
                print!("Are you sure you want to delete {}? [y/N] ", remote_path);
                use std::io::{self, Write};
                io::stdout().flush()?;
                
                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Deletion cancelled.");
                    return Ok(());
                }
            }
            
            println!("Deleting {}", remote_path);
            
            match client.delete_file(&remote_path).await {
                Ok(_) => {
                    println!("✓ File deleted successfully!");
                }
                Err(e) => {
                    error!("Failed to delete file: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        Commands::Ls { path, long } => {
            match client.list_directory(&path).await {
                Ok(entries) => {
                    if entries.is_empty() {
                        println!("Directory is empty");
                    } else if long {
                        println!("{:<10} {:<10} {:<20} {}", "MODE", "SIZE", "MODIFIED", "NAME");
                        for entry in entries {
                            let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                            println!("{:<10} {:<10} {:<20} {}",
                                format!("{:o}", entry.permissions),
                                format_bytes(entry.size),
                                entry.modified_at.format("%Y-%m-%d %H:%M:%S"),
                                name
                            );
                        }
                    } else {
                        for entry in entries {
                            let name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
                            println!("{}", name);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to list directory: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        Commands::Info { remote_path } => {
            match client.get_file_info(&remote_path).await {
                Ok(metadata) => {
                    println!("File Information:");
                    println!("  Path: {}", metadata.path);
                    println!("  File ID: {}", metadata.file_id);
                    println!("  Size: {} ({} bytes)", format_bytes(metadata.size), metadata.size);
                    println!("  Created: {}", metadata.created_at);
                    println!("  Modified: {}", metadata.modified_at);
                    println!("  Owner: {}", metadata.owner);
                    println!("  Permissions: {:o}", metadata.permissions);
                    println!("  Status: {}", if metadata.is_complete { "Complete" } else { "Incomplete" });
                    println!("  Block Size: {}", format_bytes(metadata.block_size));
                    println!("  Block Count: {}", metadata.block_count);
                    println!("  Replicas:");
                    
                    for (i, replica) in metadata.replicas.iter().enumerate() {
                        println!("    {}. Datacenter: {}", i + 1, replica.datacenter_id);
                        println!("       Type: {}", if replica.is_primary { "Primary" } else { "Secondary" });
                        println!("       Status: {:?}", replica.status);
                        println!("       RMN: {}", replica.rmn_address);
                        println!("       DNs: {:?}", replica.dn_addresses);
                    }
                }
                Err(e) => {
                    error!("Failed to get file info: {}", e);
                    std::process::exit(1);
                }
            }
        }
        
        Commands::Mkdir { path } => {
            println!("Creating directory: {}", path);
            // TODO: 实现创建目录
            println!("Directory creation not yet implemented");
        }
        
        Commands::Status => {
            println!("Cluster Status:");
            println!("  GMM Address: {}", cli.gmm_addr);
            // TODO: 获取集群状态
            println!("Status check not yet implemented");
        }
    }
    
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}