import os

workers=['15', '12', '9', '6', '3']


for w in workers:

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --conf spark.executor.memory=6g \
    --conf spark.executor.cores=1 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
    --conf spark.executor.instances="+w+"\
    --conf spark.driver.memory=6g \
    --conf spark.driver.cores=6 \
    --conf spark.executor.memoryOverhead=1g \
    --conf spark.master=yarn \
    --conf spark.submit.deployMode=client \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.shuffle.partitions=7357 \
    --conf spark.default.parallelism=7357 \
    --conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz --class benchmark.rangeEvaluation target/TrajSPARQL-jar-with-dependencies.jar > rangeEvaluation_"+w)

for w in workers:
    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --conf spark.executor.memory=6g \
    --conf spark.executor.cores=1 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
    --conf spark.executor.instances="+w+"\
    --conf spark.driver.memory=6g \
    --conf spark.driver.cores=6 \
    --conf spark.executor.memoryOverhead=1g \
    --conf spark.master=yarn \
    --conf spark.submit.deployMode=client \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.shuffle.partitions=7357 \
    --conf spark.default.parallelism=7357 \
    --conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz --class benchmark.knnEvaluation target/TrajSPARQL-jar-with-dependencies.jar 40000.1 604800 > knnEvaluation_"+w)