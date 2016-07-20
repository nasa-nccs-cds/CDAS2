#!/usr/bin/env bash

: ${CDAS_CACHE_DIR:?"Need to set the CDAS_CACHE_DIR environment variable"}

export CDAS_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export CDAS_HOME_DIR=${CDAS_BIN_DIR}/..
export CDAS_SCALA_DIR=${CDAS_BIN_DIR}/../src/main/scala
export CDAS_RESOURCE_DIR=${CDAS_BIN_DIR}/../src/main/scala
export PATH=${CDAS_BIN_DIR}:${PATH}
export CLASSPATH=${CDAS_SCALA_DIR}:${CDAS_CACHE_DIR}:${CDAS_RESOURCE_DIR}:${CLASSPATH}

alias cdas='cd $CDAS_HOME_DIR'
alias cdist='cd $CDAS_HOME_DIR/CDWPS; sbt dist; cd target/universal/; rm -rf cdwps-*-SNAPSHOT; unzip *.zip'
alias cdwps='$CDAS_HOME_DIR/../CDWPS/target/universal/cdwps-1.1-SNAPSHOT/bin/cdwps -J-Xmx6000M -J-Xms512M -J-
Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M'
alias pcdas='cd $CDAS_HOME_DIR; git fetch; git pull; sbt publish-local'
alias cdshell='cd /opt/alf/cdas/CDASClientConsole; sbt run'
