#!/bin/bash

PROJ_PATH="/home/afcamar/gitlab/datamonk/spark/NfCsvJoin"
TARGET_DIR="/target/scala-2.11"
JAR_FILE="nf-csv-join-project_2.11-1.0.jar"
#PKGS="com.databricks:spark-csv_2.11:1.5.0"
PKGS="com.databricks:spark-csv_2.10:1.4.0"

sbt package
sleep 5

# cleanup out dir
#rm -rf /home/afcamar/gitlab/datamonk/spark/scalaCsvJoin/data/out

# run first class
#spark-submit \
#--packages ${PKGS} \
#--class "SparkCsvJoin1" \
#--master local[2] \
#${PROJ_PATH}/${TARGET_DIR}/${JAR_FILE}

# cleanup out dir
rm -rf /home/afcamar/gitlab/datamonk/spark/NfCsvJoin/data/out2

# run second class
#spark-submit \
#--packages ${PKGS} \
#--class "SparkCsvJoin2" \
#--master local[2] \
#${PROJ_PATH}/${TARGET_DIR}/${JAR_FILE}

# run Join2 w/ args
spark-submit \
--packages ${PKGS} \
--class "NfCsvJoin" \
--master local[2] \
${PROJ_PATH}/${TARGET_DIR}/${JAR_FILE} \
"${PROJ_PATH}/data/nf/*" \
"${PROJ_PATH}/data/sccm/*" \
"${PROJ_PATH}/data/out2"
