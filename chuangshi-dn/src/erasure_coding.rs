#![allow(dead_code)]
use anyhow::{Result, bail};
use reed_solomon_erasure::galois_8::ReedSolomon;

pub struct ErasureCoder {
    data_shards: usize,
    parity_shards: usize,
    encoder: ReedSolomon,
}

impl ErasureCoder {
    pub fn new(data_shards: usize, parity_shards: usize) -> Self {
        let encoder = ReedSolomon::new(data_shards, parity_shards)
            .expect("Invalid erasure coding parameters");
        
        Self {
            data_shards,
            parity_shards,
            encoder,
        }
    }
    
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            bail!("Cannot encode empty data");
        }
        
        // 计算每个分片的大小
        let shard_size = (data.len() + self.data_shards - 1) / self.data_shards;
        
        // 创建分片数组
        let mut shards = vec![vec![0u8; shard_size]; self.data_shards + self.parity_shards];
        
        // 将数据分割到数据分片
        for (i, chunk) in data.chunks(shard_size).enumerate() {
            if i >= self.data_shards {
                break;
            }
            shards[i][..chunk.len()].copy_from_slice(chunk);
            // 填充剩余部分为0
            for j in chunk.len()..shard_size {
                shards[i][j] = 0;
            }
        }
        
        // 计算校验分片
        self.encoder.encode(&mut shards)?;
        
        Ok(shards)
    }
    
    pub fn decode(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>> {
        // 检查是否有足够的分片
        let available_count = shards.iter().filter(|s| s.is_some()).count();
        if available_count < self.data_shards {
            bail!(
                "Not enough shards for recovery: {} available, {} required",
                available_count,
                self.data_shards
            );
        }
        
        // 重建缺失的分片
        self.encoder.reconstruct(&mut shards)?;
        
        // 合并数据分片
        let mut data = Vec::new();
        for i in 0..self.data_shards {
            if let Some(shard) = &shards[i] {
                data.extend_from_slice(shard);
            } else {
                bail!("Failed to reconstruct data shard {}", i);
            }
        }
        
        // 去除填充的0
        while data.last() == Some(&0) {
            data.pop();
        }
        
        Ok(data)
    }
    
    pub fn verify(&self, shards: &[Vec<u8>]) -> bool {
        if shards.len() != self.data_shards + self.parity_shards {
            return false;
        }
        
        // 克隆分片进行验证
        let mut verify_shards = shards.to_vec();
        
        // 重新计算校验分片
        if let Ok(()) = self.encoder.encode(&mut verify_shards) {
            // 比较校验分片
            for i in self.data_shards..shards.len() {
                if verify_shards[i] != shards[i] {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_decode() {
        let coder = ErasureCoder::new(6, 3);
        let original_data = b"Hello, Chuangshi NDFS! This is a test message.";
        
        // 编码
        let shards = coder.encode(original_data).unwrap();
        assert_eq!(shards.len(), 9);
        
        // 创建有缺失的分片
        let mut incomplete_shards: Vec<Option<Vec<u8>>> = shards
            .iter()
            .map(|s| Some(s.clone()))
            .collect();
        
        // 删除3个分片
        incomplete_shards[0] = None;
        incomplete_shards[3] = None;
        incomplete_shards[7] = None;
        
        // 解码
        let recovered_data = coder.decode(incomplete_shards).unwrap();
        
        // 验证恢复的数据
        assert_eq!(&recovered_data[..original_data.len()], original_data);
    }
}