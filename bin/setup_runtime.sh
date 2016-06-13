#!/usr/bin/env bash

export CDAS_BIN_DIR="$( dirname "${BASH_SOURCE[0]}" )"
export CDAS_MAIN_DIR=${CDAS_BIN_DIR}/../src/main
export CDAS_SCALA_DIR=${CDAS_MAIN_DIR}/scala
export CDAS_RESOURCES_DIR=${CDAS_MAIN_DIR}/resources
export PATH=${CDAS_BIN_DIR}:${PATH}
export CLASSPATH=${CDAS_SCALA_DIR}:${CDAS_RESOURCES_DIR}:${CLASSPATH}