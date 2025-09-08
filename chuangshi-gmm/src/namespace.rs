use anyhow::{Result, bail};
use chuangshi_common::{utils::parse_path, ChuangshiError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;
use std::sync::Arc;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceNode {
    pub name: String,
    pub children: HashMap<String, NamespaceNode>,
    pub file_id: Option<Uuid>,
}

impl NamespaceNode {
    fn new(name: String) -> Self {
        Self {
            name,
            children: HashMap::new(),
            file_id: None,
        }
    }
}

pub struct NamespaceManager {
    root: Arc<RwLock<NamespaceNode>>,
    data_dir: String,
}

impl NamespaceManager {
    pub async fn new(data_dir: &str) -> Result<Self> {
        let namespace_file = Path::new(data_dir).join("namespace.json");
        
        let root = if namespace_file.exists() {
            // 加载已有命名空间
            let data = fs::read_to_string(&namespace_file).await?;
            serde_json::from_str(&data)?
        } else {
            // 创建新的命名空间
            NamespaceNode::new("chuangshi".to_string())
        };
        
        Ok(Self {
            root: Arc::new(RwLock::new(root)),
            data_dir: data_dir.to_string(),
        })
    }
    
    pub async fn add_file(&self, path: &str, file_id: Uuid) -> Result<()> {
        let components = parse_path(path);
        if components.is_empty() || components[0] != "chuangshi" {
            bail!("Invalid path: {}", path);
        }
        
        let mut root = self.root.write().await;
        let mut current = &mut *root;
        
        // 跳过根节点 "chuangshi"
        for (i, component) in components.iter().skip(1).enumerate() {
            let is_last = i == components.len() - 2;
            
            if is_last {
                // 最后一个组件是文件
                current.children.insert(
                    component.clone(),
                    NamespaceNode {
                        name: component.clone(),
                        children: HashMap::new(),
                        file_id: Some(file_id),
                    },
                );
            } else {
                // 中间组件是目录
                current = current.children
                    .entry(component.clone())
                    .or_insert_with(|| NamespaceNode::new(component.clone()));
            }
        }
        
        // 持久化
        self.save_namespace(&*root).await?;
        
        Ok(())
    }
    
    pub async fn remove_file(&self, path: &str) -> Result<()> {
        let components = parse_path(path);
        if components.is_empty() || components[0] != "chuangshi" {
            bail!("Invalid path: {}", path);
        }
        
        let mut root = self.root.write().await;
        
        // 递归查找并删除
        Self::remove_file_recursive(&mut *root, &components[1..]);
        
        // 持久化
        self.save_namespace(&*root).await?;
        
        Ok(())
    }
    
    fn remove_file_recursive(node: &mut NamespaceNode, components: &[String]) -> bool {
        if components.is_empty() {
            return false;
        }
        
        if components.len() == 1 {
            // 删除文件
            node.children.remove(&components[0]).is_some()
        } else if let Some(child) = node.children.get_mut(&components[0]) {
            Self::remove_file_recursive(child, &components[1..])
        } else {
            false
        }
    }
    
    pub async fn resolve_path(&self, path: &str) -> Result<Uuid> {
        let components = parse_path(path);
        if components.is_empty() || components[0] != "chuangshi" {
            bail!("Invalid path: {}", path);
        }
        
        let root = self.root.read().await;
        let mut current = &*root;
        
        for component in components.iter().skip(1) {
            if let Some(node) = current.children.get(component) {
                current = node;
            } else {
                bail!("Path not found: {}", path);
            }
        }
        
        current.file_id.ok_or_else(|| anyhow::anyhow!("Not a file: {}", path))
    }
    
    pub async fn list_directory(&self, path: &str) -> Result<Vec<(String, Uuid)>> {
        let components = parse_path(path);
        if components.is_empty() || components[0] != "chuangshi" {
            bail!("Invalid path: {}", path);
        }
        
        let root = self.root.read().await;
        let mut current = &*root;
        
        // 导航到目录
        for component in components.iter().skip(1) {
            if let Some(node) = current.children.get(component) {
                current = node;
            } else {
                bail!("Path not found: {}", path);
            }
        }
        
        // 收集文件
        let mut entries = Vec::new();
        for (name, node) in &current.children {
            if let Some(file_id) = node.file_id {
                let full_path = format!("{}/{}", path.trim_end_matches('/'), name);
                entries.push((full_path, file_id));
            }
        }
        
        Ok(entries)
    }
    
    async fn save_namespace(&self, root: &NamespaceNode) -> Result<()> {
        let namespace_file = Path::new(&self.data_dir).join("namespace.json");
        let data = serde_json::to_string_pretty(root)?;
        fs::write(namespace_file, data).await?;
        Ok(())
    }
}