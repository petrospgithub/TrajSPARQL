import ConfigParser
import os
import sys
import time

dataset_sample = ['0.2',
                  '0.4',
                  '0.6',
                  '0.8']

os.system("kill -9 $(lsof -t -i:9083)")

time.sleep(10)

os.system("kill -9 $(lsof -t -i:10000)")

time.sleep(10)

os.system("nohup /root/apache-hive-2.3.3-bin/bin/hive --service metastore > metastore.out &")

time.sleep(10)

os.system("nohup /root/apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=7168 --hiveconf mapreduce.map.java.opts=-Xmx5734m --hiveconf mapreduce.reduce.memory.mb=7168 --hiveconf mapreduce.reduce.java.opts=-Xmx5734m --hiveconf hive.aux.jars.path=file:///root/implementation/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar > hiveserver.out &")

time.sleep(10)


for d in dataset_sample:

    config = ConfigParser.RawConfigParser()
    config.read('./config/traj_octree.properties')

    cfgfile = open("./config/traj_octree.properties", 'w')
    config.set('Spark', 'spark.fraction_dataset', d)

    config.write(cfgfile)
    cfgfile.close()

    os.system("hdfs dfs -rm -r '*counter*'")
    os.system("hdfs dfs -rm -r 'octree*'")
    os.system("hdfs dfs -rm -r 'parti*'")

    time.sleep(10)

    os.system("mvn test -Dtest=DropTable")
    time.sleep(10)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --properties-file \"/root/implementation/TrajSPARQL/config/traj_octree.properties\" --class apps.OcTreeApp /root/implementation/TrajSPARQL/target/TrajSPARQL-jar-with-dependencies.jar")
    time.sleep(10)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --properties-file \"/root/implementation/TrajSPARQL/config/traj_octree.properties\" --class binary.OcTreeAppBinary /root/implementation/TrajSPARQL/target/TrajSPARQL-jar-with-dependencies.jar")
    time.sleep(10)

    os.system("mvn test -Dtest=CreateTablesArr -q -DargLine=\"-Dbuckets=30\" ")
    time.sleep(30)

    os.system("mvn test -Dtest=CreateTablesBinary -q -DargLine=\"-Dbuckets=30\" ")
    time.sleep(30)

    os.system("mvn test -Dtest=CreateTableStoreTraj -q -DargLine=\"-Dbuckets=30\" ")
    time.sleep(30)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --jars /root/implementation/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar \
     --conf spark.executor.memory=6g \
    --conf spark.executor.cores=1 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
    --conf spark.executor.instances=15 \
    --conf spark.driver.memory=6g \
    --conf spark.driver.cores=6 \
    --conf spark.executor.memoryOverhead=1g \
    --conf spark.master=yarn \
    --conf spark.submit.deployMode=client \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz --class benchmark.rangeEvaluation target/TrajSPARQL-jar-with-dependencies.jar > rangeSpeedUp_spark_"+d)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --jars /root/implementation/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar \
    --conf spark.executor.memory=6g \
    --conf spark.executor.cores=1 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
    --conf spark.executor.instances=15 \
    --conf spark.driver.memory=6g \
    --conf spark.driver.cores=6 \
    --conf spark.executor.memoryOverhead=1g \
    --conf spark.master=yarn \
    --conf spark.submit.deployMode=client \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz --class benchmark.knnEvaluation target/TrajSPARQL-jar-with-dependencies.jar 40000.1 604800 > knnSpeedUp_spark_"+d)