#!/usr/bin/env bash

CDAS_JAR=${CDAS_HOME_DIR}/target/scala-2.10/cdas2_2.10-1.2.2-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -r 's/[ \n\r]+/:/g')
$CDH_HOME/bin/spark-submit --class nasa.nccs.cdas.portal.CDASApplication --master spark://localhost:7077 --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --conf "spark.driver.extraClassPath=${APP_DEP_CP}" --driver-m
emory 8g ${CDAS_JAR} bind 4356 4357 ~/.cdas/cache/cdas.properties
