#!/usr/bin/env bash

CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.10/cdas2_2.10-1.2.2-SNAPSHOT.jar
spark-submit --class nasa.nccs.cdas.portal.CDASApplication --master yarn --deploy-mode cluster --driver-memory 8g ${CDAS_JAR} bind 4356 4357