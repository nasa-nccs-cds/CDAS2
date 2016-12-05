#!/usr/bin/env bash
source ./setup_runtime.sh
echo "Running Python worker"
python ../python/pycdas/worker.py $*
