#![allow(dead_code)]
use sha2::{Sha256, Digest};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// 计算数据的SHA256校验和
pub fn calculate_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// 验证校验和
pub fn verify_checksum(data: &[u8], expected: &[u8]) -> bool {
    let actual = calculate_checksum(data);
    actual == expected
}

/// 生成块ID
pub fn generate_block_id(file_id: &Uuid, block_index: u64) -> Uuid {
    let input = format!("{}-{}", file_id, block_index);
    let hash = calculate_checksum(input.as_bytes());
    
    // 使用hash的前16字节创建UUID
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    Uuid::from_bytes(bytes)
}

/// 解析文件路径
pub fn parse_path(path: &str) -> Vec<String> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// 格式化字节大小
pub fn format_bytes(bytes: u64) -> String {
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

/// 检查路径是否有效
pub fn is_valid_path(path: &str) -> bool {
    if !path.starts_with("/chuangshi") {
        return false;
    }
    
    // 检查非法字符
    let illegal_chars = ['\\', ':', '*', '?', '"', '<', '>', '|'];
    for c in illegal_chars {
        if path.contains(c) {
            return false;
        }
    }
    
    // 检查路径组件
    let components = parse_path(path);
    for component in components {
        if component.is_empty() || component == "." || component == ".." {
            return false;
        }
    }
    
    true
}