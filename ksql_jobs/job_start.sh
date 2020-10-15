#!/bin/bash

ARG="$*"
if [ "x$ARG" = "x" ] ; then
	echo "no file specified"
	exit 1
fi

QUERIES_FILE=$ARG
NAME=`basename $QUERIES_FILE | sed 's/\..*//g'`

LOG_DIR="/var/log/ksql/$NAME"
BIN_DIR="/opt/ksql/bin/"
CFG_DIR="/opt/ksql/config/"
JOB_DIR="/opt/ksql/config/jobs/"

mkdir -p $LOG_DIR

export KSQL_HEAP_OPTS="-Xms5G -Xmx40G"
export KSQL_LOG4J_OPTS="-Dksql.log.dir=$LOG_DIR -Dlog4j.configuration=file:$CFG_DIR/log4j-rolling.properties"

ps aux | grep "java.*$NAME" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null

nohup $BIN_DIR/ksql-server-start $JOB_DIR/job.properties --queries-file $QUERIES_FILE &

