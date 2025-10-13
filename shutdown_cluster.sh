#!/bin/bash
# Chuangshi NDFS Cluster Shutdown Script (no pkill/pgrep)
set -e

echo "======================================"
echo "   Chuangshi NDFS Cluster Shutdown   "
echo "======================================"

# 根据进程名提取 PID 并杀死
kill_procs() {
    local name=$1
    local pids
    pids=$(ps -ef | awk -v proc="$name" '$0 ~ proc && $0 !~ /awk/ {print $2}')
    if [ -n "$pids" ]; then
        echo "Stopping $name processes: $pids"
        kill $pids 2>/dev/null || true
        sleep 2
        # 仍未退出的强制杀
        for pid in $pids; do
            if ps -p "$pid" >/dev/null 2>&1; then
                echo "Force-killing $pid"
                kill -9 "$pid" 2>/dev/null || true
            fi
        done
    fi
}

kill_procs chuangshi-gmm
kill_procs chuangshi-rmn
kill_procs chuangshi-dn

echo ""
echo "✓ Cluster stopped successfully!"
