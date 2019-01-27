import ConfigParser
import os
import sys
import time

dataset_sample = ['0.2',
                  '0.4',
                  '0.6',
                  '0.8',
                  '1']

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

    os.system("mvn test -Dtest=DropTable")
    time.sleep(10)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --properties-file \"/root/implementation/TrajSPARQL/config/traj_octree.properties\" --class apps.OcTreeApp /root/implementation/TrajSPARQL/target/TrajSPARQL-jar-with-dependencies.jar")
    time.sleep(10)

    os.system("/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --properties-file \"/root/implementation/TrajSPARQL/config/traj_octree.properties\" --class binary.OcTreeAppBinary /root/implementation/TrajSPARQL/target/TrajSPARQL-jar-with-dependencies.jar")
    time.sleep(10)

    os.system("mvn test -Dtest=CreateTables -q -DargLine=\"-Dbuckets=30\" ")
    time.sleep(30)

    os.system("mvn test -Dtest=RangeQueriesBF_arr -q >> speed_up_range_"+d)
    time.sleep(60)

    os.system("mvn test -Dtest=RangeQueriesIndex_arr -q >> speed_up_range_"+d)
    time.sleep(60)

    os.system("mvn test -Dtest=RangeQueriesTraj_binary -q >> speed_up_range_"+d)
    time.sleep(60)

    os.system("mvn test -Dtest=knnQueries_arrPID -q >> speed_up_knn_"+d)

    time.sleep(60)

    os.system("mvn test -Dtest=knnQueries_arrIndex -q >> speed_up_knn_"+d)

    time.sleep(60)

    os.system("mvn test -Dtest=knnQueries_traj -q >> speed_up_knn_"+d)

    time.sleep(60)

    '''
    
    os.system("mvn test -Dtest=pidknn.knnSelf_arrPID -q >> "+str(15-int(sys.argv[1]))+"_worker_knnSelf")
    time.sleep(60)
    
    os.system("mvn test -Dtest=pidknn.knnSelf_arrIndex -q >> "+str(15-int(sys.argv[1]))+"_worker_knnSelf")
    time.sleep(60)
    
    os.system("mvn test -Dtest=pidknn.knnSelf_traj -q >> "+str(15-int(sys.argv[1]))+"_worker_knnSelf")
    time.sleep(60)
    
    '''





