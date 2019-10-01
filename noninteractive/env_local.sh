# This file must be compatible with bash(1) on the system.
# It should be alright to source this file multiple times.

# Because you can change SPARK_CONF_DIR in this file, the main
# scripts only create the directory after sourcing this file.  Since
# we need the directory to create the spark-defaults.conf file, we
# create the directory here.
[[ -d $SPARK_CONF_DIR ]] || mkdir -p "$SPARK_CONF_DIR"

# The created spark-defaults.conf file will only affect spark
# submitted under the current directory where this file resides.
# The parameters here may require tuning depending on the machine and workload.
[[ -s $SPARK_CONF_DIR/spark-defaults.conf ]] ||
	cat > "$SPARK_CONF_DIR/spark-defaults.conf" <<'EOF'
spark.task.cpus                       4
spark.worker.timeout              24000
spark.executor.heartbeatInterval   4000s
spark.files.fetchTimeout          12000s
spark.network.timeout             24000s
spark.locality.wait                6000s
spark.driver.memory                  32g
spark.executor.memory                32g
spark.rpc.netty.dispatcher.numThreads                8
spark.scheduler.maxRegisteredResourcesWaitingTime 4000s
spark.scheduler.minRegisteredResourcesRatio          1
spark.scheduler.listenerbus.eventqueue.capacity 100000
spark.driver.extraJavaOptions   -XX:+UseParallelGC -XX:ParallelGCThreads=8
spark.executor.extraJavaOptions -XX:+UseParallelGC -XX:ParallelGCThreads=8
EOF

# You can use SPARKJOB_HOST to detect the running system.
if [[ $SPARKJOB_HOST == theta ]];then
	echo "$(hostname): running on theta"
fi

# On cooley, interactive spark jobs setup ipython notebook by
# defaults.  You can change it here, along with setting up your
# other python environment.
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS

echo "$(hostname): sourced env_local.sh"
