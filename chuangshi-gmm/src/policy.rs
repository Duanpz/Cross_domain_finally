use chuangshi_common::types::*;
use dashmap::DashMap;
use std::sync::Arc;
use uuid::Uuid;
use std::collections::HashMap;
pub struct PolicyEngine {
    policies: Vec<Policy>,
    datacenter_loads: HashMap<String, f64>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        // 初始化默认策略
        let default_policies = vec![
            Policy {
                id: Uuid::new_v4(),
                name: "AI训练数据策略".to_string(),
                description: "AI训练数据优先存储在GPU资源丰富的数据中心".to_string(),
                rules: vec![
                    PolicyRule {
                        condition: PolicyCondition::FileTag("AI训练".to_string()),
                        action: PolicyAction::PlaceIn(vec![
                            "shanghai".to_string(),
                            "beijing".to_string(),
                        ]),
                    },
                ],
                priority: 100,
                enabled: true,
            },
            Policy {
                id: Uuid::new_v4(),
                name: "大文件策略".to_string(), 
                description: "大文件自动创建多副本".to_string(),
                rules: vec![
                    PolicyRule {
                        condition: PolicyCondition::FileSize {
                            min: Some(1024 * 1024 * 1024), // 1GB
                            max: None,
                        },
                        action: PolicyAction::SetReplicationFactor(3),
                    },
                ],
                priority: 50,
                enabled: true,
            },
            Policy {
                id: Uuid::new_v4(),
                name: "默认策略".to_string(),
                description: "默认存储策略".to_string(),
                rules: vec![],
                priority: 0,
                enabled: true,
            },
        ];
        
        Self {
            policies: default_policies,
            datacenter_loads: HashMap::new(),
        }
    }
    
    pub fn select_datacenters(
        &self,
        tags: &[String],
        size: u64,
        datacenters: &DashMap<String, DataCenter>,
    ) -> Vec<String> {
        let mut selected = Vec::new();
        let mut replication_factor = 2; // 默认副本数
        
        // 应用策略
        for policy in &self.policies {
            if !policy.enabled {
                continue;
            }
            
            for rule in &policy.rules {
                if self.match_condition(&rule.condition, tags, size) {
                    match &rule.action {
                        PolicyAction::PlaceIn(dcs) => {
                            for dc in dcs {
                                if datacenters.get(dc).map_or(false, |d| {
                                    d.status == DataCenterStatus::Online
                                }) && !selected.contains(dc) {
                                    selected.push(dc.clone());
                                }
                            }
                        }
                        PolicyAction::SetReplicationFactor(factor) => {
                            replication_factor = *factor as usize;
                        }
                        _ => {}
                    }
                }
            }
        }
        
        // 如果策略没有选择足够的数据中心，使用负载均衡选择
        if selected.len() < replication_factor {
            let mut candidates: Vec<(String, f64)> = datacenters
                .iter()
                .filter(|dc| {
                    dc.status == DataCenterStatus::Online && !selected.contains(&dc.id)
                })
                .map(|dc| {
                    let load = dc.capacity.used as f64 / dc.capacity.total as f64;
                    (dc.id.clone(), load)
                })
                .collect();
            
            // 按负载排序，选择负载最低的
            candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            
            for (dc_id, _) in candidates {
                if selected.len() >= replication_factor {
                    break;
                }
                selected.push(dc_id);
            }
        }
        
        selected
    }
    
    fn match_condition(&self, condition: &PolicyCondition, tags: &[String], size: u64) -> bool {
        match condition {
            PolicyCondition::FileTag(tag) => tags.contains(tag),
            PolicyCondition::FileSize { min, max } => {
                let above_min = min.map_or(true, |m| size >= m);
                let below_max = max.map_or(true, |m| size <= m);
                above_min && below_max
            }
            PolicyCondition::FileExtension(ext) => {
                // TODO: 从路径提取扩展名进行匹配
                false
            }
            _ => false,
        }
    }
    
    pub fn update_policy(&mut self, policy: Policy) {
        if let Some(existing) = self.policies.iter_mut().find(|p| p.id == policy.id) {
            *existing = policy;
        } else {
            self.policies.push(policy);
        }
        
        // 按优先级排序
        self.policies.sort_by_key(|p| std::cmp::Reverse(p.priority));
    }
    
    pub fn update_datacenter_load(&mut self, datacenter_id: String, load: f64) {
        self.datacenter_loads.insert(datacenter_id, load);
    }
}