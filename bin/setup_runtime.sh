#!/usr/bin/env bash

export CDAS_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export CDAS_SCALA_DIR=${CDAS_BIN_DIR}/../src/main/scala
export CDAS_CACHE_DIR=${CDAS_BIN_DIR}/../cache
export PATH=${CDAS_BIN_DIR}:${PATH}
export CLASSPATH=${CDAS_SCALA_DIR}:${CDAS_CACHE_DIR}:${CLASSPATH}