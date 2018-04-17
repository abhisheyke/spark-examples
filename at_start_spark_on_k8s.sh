#!/usr/bin/env bash

DAEMON_NAME=spark-on-k8s
SPARK_HOME=~/platforms/spark-2.3.0-bin-hadoop2.7

# This startup script assumed that you have k8s cluster up and rrunning.
# Kubeadm or minikube can be used to bring up k8s cluster easily.
# k8s master node: k8s-master
# Same node was also hosting docker private repo. might need to replace hostname
# with IP address in container image.
#   - jar path on conatiner /opt/spark/examples/jars/abhi-shade-spark-example-x.y.z.jar
# For more details: https://spark.apache.org/docs/latest/running-on-kubernetes.html


SPARK_HOME/bin/spark-submit --master k8s://https://k8s-master:6443 \
                            --deploy-mode cluster \
                            --name spark-abhi \
                            --class com.aktripathi.k8s.TryK8S \
                            --conf spark.executor.instances=2 \
                            --conf spark.kubernetes.container.image=k8s-master:5000/abhit/spark:2.3.0 \
                            local:///opt/spark/examples/jars/abhi-shade-spark-example-x.y.z.jar
