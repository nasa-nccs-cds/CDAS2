#!/usr/bin/env bash

: ${CDAS_CACHE_DIR:?"Need to set the CDAS_CACHE_DIR environment variable"}

export CDAS_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export CDAS_HOME_DIR=${CDAS_BIN_DIR}/..
export CDWPS_HOME_DIR=${CDWPS_HOME_DIR:-${CDAS_HOME_DIR}/../CDWPS}
export CDSHELL_HOME_DIR=${CDSHELL_HOME_DIR:-${CDAS_HOME_DIR}/../CDASClientConsole}
export CDAS_SCALA_DIR=${CDAS_BIN_DIR}/../src/main/scala
export PATH=${CDAS_BIN_DIR}:${PATH}
export CLASSPATH=${CDAS_SCALA_DIR}:${CDAS_CACHE_DIR}:${CLASSPATH}

alias cdas='cd $CDAS_HOME_DIR'
alias cdist='cd $CDWPS_HOME_DIR; sbt dist; cd target/universal/; rm -rf cdwps-*-SNAPSHOT; unzip *.zip'
alias cdwps='nohup $CDWPS_HOME_DIR/target/universal/cdwps-1.1-SNAPSHOT/bin/cdwps -J-Xmx6000M -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC -J-XX:MaxPermSize=800M &'
alias pcdas='cd $CDAS_HOME_DIR; git fetch; git pull; sbt publish-local'
alias cdshell='cd $CDSHELL_HOME_DIR; sbt run'
