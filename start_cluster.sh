#!/bin/bash

# Chuangshi NDFS Cluster Startup Script

set -e

echo "======================================"
echo "   Chuangshi NDFS Cluster Startup    "
echo "======================================"



# 检查是否已编译
if [ ! -f "target/release/chuangshi-gmm" ]; then
    echo -e "Building project..."
    cargo build --release
fi

# 创建数据目录
echo "Creating data directories..."
mkdir -p data/{gmm,beijing-{rmn,dn{1,2,3}},shanghai-{rmn,dn{1,2,3}},hefei-{rmn,dn{1,2,3}},guangzhou-{rmn,dn{1,2,3}}}
mkdir -p logs

# 清理旧进程
echo "Cleaning up old processes..."
# pkill -f chuangshi-gmm || true
# pkill -f chuangshi-rmn || true
# pkill -f chuangshi-dn || true
sleep 2

# 启动GMM
echo -e "Starting GMM..."
nohup ./target/release/chuangshi-gmm \
    --bind-addr 0.0.0.0:8001 \
    --data-dir data/gmm \
    > logs/gmm.log 2>&1 &

sleep 3

# 启动北京数据中心
echo -e "Starting Beijing datacenter..."
nohup ./target/release/chuangshi-rmn \
    --bind-addr 0.0.0.0:8002 \
    --datacenter-id beijing \
    --gmm-addr localhost:8001 \
    --data-dir data/beijing-rmn \
    > logs/beijing-rmn.log 2>&1 &

for i in {1..3}; do
    port=$((8002 + i))
    nohup ./target/release/chuangshi-dn \
        --bind-addr 0.0.0.0:$port \
        --datacenter-id beijing \
        --rmn-addr localhost:8002 \
        --data-dir data/beijing-dn$i \
        > logs/beijing-dn$i.log 2>&1 &
done

# 启动上海数据中心
echo -e "Starting Shanghai datacenter..."
nohup ./target/release/chuangshi-rmn \
    --bind-addr 0.0.0.0:8012 \
    --datacenter-id shanghai \
    --gmm-addr localhost:8001 \
    --data-dir data/shanghai-rmn \
    > logs/shanghai-rmn.log 2>&1 &

for i in {1..3}; do
    port=$((8012 + i))
    nohup ./target/release/chuangshi-dn \
        --bind-addr 0.0.0.0:$port \
        --datacenter-id shanghai \
        --rmn-addr localhost:8012 \
        --data-dir data/shanghai-dn$i \
        > logs/shanghai-dn$i.log 2>&1 &
done

sleep 2

echo ""
echo -e "✓ Cluster started successfully!"
echo ""
echo "Service Endpoints:"
echo "  GMM:         http://localhost:8001"
echo "  Beijing RMN: http://localhost:8002"
echo "  Shanghai RMN: http://localhost:8012"
echo ""
echo "Logs are available in the logs/ directory"
echo ""
echo "Example commands:"
echo "  ./target/release/chuangshi put README.md /chuangshi/test/readme.md"
echo "  ./target/release/chuangshi ls /chuangshi"
echo "  ./target/release/chuangshi get /chuangshi/test/readme.md local.md"
echo ""