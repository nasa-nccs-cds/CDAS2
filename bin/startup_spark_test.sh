#!/usr/bin/env bash

CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.11/cdas2_2.11-1.2.2-SNAPSHOT.jar
spark-submit --class nasa.nccs.cdas.portal.TestApplication --master yarn --deploy-mode cluster --driver-memory 8g ${CDAS_JAR}