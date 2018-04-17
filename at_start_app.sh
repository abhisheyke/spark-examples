#!/usr/bin/env bash

DAEMON_NAME=spark-on-k8s
SPARK_HOME=~/platforms/spark-2.3.0-bin-hadoop2.7
JAR=target/abhi-shade*.jar 


start() {
        $SPARK_HOME/bin/spark-submit \
        --name $DAEMON_NAME \
        --master local[2] \
        --class com.aktripathi.k8s.TryK8S \
        $JAR
}

start
