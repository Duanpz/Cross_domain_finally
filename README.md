# Chuangshi NDFS - 创世国家数据文件系统

一个基于Rust实现的分布式文件系统，为国家超算中心提供统一的存储解决方案。

## 特性

**高性能**: 基于Rust实现，零成本抽象，极致性能
**高可用**: 支持多副本、纠删码，自动故障恢复
**全局命名空间**: 统一的文件系统视图
**智能调度**: 基于策略的数据放置和迁移
**安全可靠**: 内存安全，类型安全，数据完整性校验

## 架构组件

- **GMM (Global Metadata Master)**: 全局元数据主节点
- **RMN (Regional Metadata Node)**: 区域元数据节点
- **DN (Data Node)**: 数据节点
- **Client**: 客户端SDK和CLI工具

## 快速开始


cargo build --release