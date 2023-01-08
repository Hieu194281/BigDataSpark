#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then
  hdfs namenode -format

  # start the master node processes
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # copy the data to the data HDFS directory
  hdfs dfs -mkdir -p /spark-logs
  hdfs dfs -mkdir -p /opt/spark/data

  # copy the data to the data HDFS directory
  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  # start the worker node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
elif [ "$SPARK_WORKLOAD" == "history" ];
then

  hdfs dfs -test -d /spark-logs
  while [ ! $? != 0 ];
  do
    echo "spark-logs doesn't exist yet... retrying"
    sleep 1;
    hdfs dfs -test -d /spark-logs
  done

  hdfs dfs -mkdir -p /spark-logs

  # start the MapReduce JobHistory server
  mapred --daemon start historyserver

  # start the spark history server
  start-history-server.sh
fi

tail -f /dev/null
