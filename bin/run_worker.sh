#!/usr/bin/env bash
source ./setup_runtime.sh
CDAS_WORKER=${CDAS_HOME_DIR}/python/pycdas/worker.py
echo "Running Python worker: "
echo ${CDAS_WORKER}
python ${CDAS_WORKER} $*
