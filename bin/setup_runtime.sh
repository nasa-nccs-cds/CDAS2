#!/usr/bin/env bash

export CDAS_CACHE_DIR=${CDAS_CACHE_DIR:-${HOME}/.cdas/cache}
export CDAS_MAX_MEM=${CDAS_MAX_MEM:-32000M}
export CDAS_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export CDAS_HOME_DIR=${CDAS_BIN_DIR}/..
export CDWPS_HOME_DIR=${CDWPS_HOME_DIR:-${CDAS_HOME_DIR}/../CDWPS}
export CDSHELL_HOME_DIR=${CDSHELL_HOME_DIR:-${CDAS_HOME_DIR}/../CDASClientConsole}
export CDAS_SCALA_DIR=${CDAS_BIN_DIR}/../src/main/scala
export CDAS_STAGE_DIR=${CDAS_HOME_DIR}/target/universal/stage
export CLASSPATH=${CDAS_SCALA_DIR}:${CDAS_CACHE_DIR}:${CDAS_STAGE_DIR}/conf:${CDAS_STAGE_DIR}/lib:${CONDA_PREFIX}/lib:${CLASSPATH}
export UVCDAT_ANONYMOUS_LOG=no
export CDAS_JAVA_ARGS="-J-Xmx$CDAS_MAX_MEM -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC"
export WPS_CMD="$CDWPS_HOME_DIR/target/universal/cdwps-1.1-SNAPSHOT/bin/cdwps $CDAS_JAVA_ARGS"
export CDSHELL_CMD="$CDSHELL_HOME_DIR/target/universal/stage/bin/cdasclientconsole $CDAS_JAVA_ARGS"
export PATH=${CDAS_STAGE_DIR}/bin:${CDAS_BIN_DIR}:${PATH}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib

alias cdas='cd $CDAS_HOME_DIR'
alias cdist='cd $CDWPS_HOME_DIR; sbt dist; cd target/universal/; rm -rf cdwps-*-SNAPSHOT; unzip *.zip; cd ../..; chmod -R a+rwX target; chmod -R a+rX ../CDWPS'
alias pdist='cd $CDAS_HOME_DIR; sbt universal:packageBin; cd target/universal/; rm -rf cdas2-*-SNAPSHOT; unzip *.zip; cd ../..; chmod -R a+rwX target; chmod -R a+rX ../CDAS2'
alias cdwps=$WPS_CMD
alias cdwpsb='nohup $WPS_CMD &'
alias pcdas='cd $CDAS_HOME_DIR; git fetch; git pull; sbt publish'
alias cdshd='unset CDAS_SERVER_ADDRESS; unset CDAS_SERVER_PORT; $CDSHELL_CMD'
alias cdshw='export CDAS_SERVER_ADDRESS=localhost; unset CDAS_SERVER_PORT; $CDSHELL_CMD'
alias cdshr='export CDAS_SERVER_ADDRESS=localhost; export CDAS_SERVER_PORT=9001; $CDSHELL_CMD'
alias cdup='cd $CDAS_HOME_DIR; ./bin/update.sh; rm ~/.cdas/*.log; python ./python/src/pycdas/shutdown.py'
alias cdupy='cd $CDAS_HOME_DIR; python setup.py install; rm ~/.cdas/logs/*.log; rm ~/.cdas/*.log; ~/.cdas/bin/shutdown_python_worker.sh'
alias rncml='rm $CDAS_CACHE_DIR/collections/NCML/*'
alias rfrag='rm $CDAS_CACHE_DIR/fragment/*'

umask 002

