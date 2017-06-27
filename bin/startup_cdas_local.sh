#!/usr/bin/env bash

CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.10/cdas2_2.10-1.2.2-SNAPSHOT.jar
CONDA_LIB=${CONDA_PREFIX}/lib
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -e "s/ /:/g" ):${CONDA_LIB}
spark-submit --class nasa.nccs.cdas.portal.CDASApplication --master local[*] --driver-class-path ${APP_DEP_CP} --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --driver-memory 8g ${CDAS_JAR} bind 5670 5671 ~/.cdas/cache/cdas.properties