#!/usr/bin/env bash

CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.10/cdas2_2.10-1.2.2-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -E -e 's/[ \n]+/:/g'):$CDAS_JAR
spark-submit --class nasa.nccs.cdas.portal.CDASApplication --master local[*] --driver-class-path ${APP_DEP_CP} --driver-memory 8g ${CDAS_JAR} bind 4356 4357 ~/.cdas/cache/cdas.properties