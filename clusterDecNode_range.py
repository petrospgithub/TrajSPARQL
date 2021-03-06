import os
import sys
import time

myfile = '/root/hadoop-3.1.1/etc/hadoop/yarn.exclude'

f=open(myfile, 'w')

patterns = ['192.168.0.20',
            '192.168.0.19',
            '192.168.0.18',
            '192.168.0.17',
            '192.168.0.16',
            '192.168.0.15',
            '192.168.0.14',
            '192.168.0.13',
            '192.168.0.12',
            '192.168.0.11',
            '192.168.0.10',
            '192.168.0.9',
            '192.168.0.8',
            '192.168.0.7',
            '192.168.0.5']


for i in range (0, int(sys.argv[1])):
    f.write(patterns[i]+"\n")

f.close()

os.system("kill -9 $(lsof -t -i:9083)")

time.sleep(10)

os.system("kill -9 $(lsof -t -i:10000)")

time.sleep(10)

os.system("yarn rmadmin -refreshNodes")

time.sleep(10)

time.sleep(10)

os.system("nohup /root/apache-hive-2.3.3-bin/bin/hive --service metastore > metastore.out &")

time.sleep(10)

os.system("nohup /root/apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=7168 --hiveconf mapreduce.map.java.opts=-Xmx5734m --hiveconf mapreduce.reduce.memory.mb=7168 --hiveconf mapreduce.reduce.java.opts=-Xmx5734m --hiveconf hive.aux.jars.path=file:///root/implementation/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar > hiveserver.out &")

time.sleep(10)
'''
os.system("mvn test -Dtest=RangeQueriesBF_arr -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)

os.system("mvn test -Dtest=RangeQueriesIndex_arr -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)

os.system("mvn test -Dtest=RangeQueriesBF_binary -q >>"+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)

os.system("mvn test -Dtest=RangeQueriesIndex_binary -q >>"+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)

os.system("mvn test -Dtest=RangeQueriesTraj_binary -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)
'''
time.sleep(60)
os.system("mvn test -Dtest=RangeQueriesBF_arr -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)
os.system("mvn test -Dtest=RangeQueriesPID_arr -q >>"+str(15-int(sys.argv[1]))+"_worker_range_pid_arr")

time.sleep(60)
os.system("mvn test -Dtest=RangeQueriesIndex_arr -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)
os.system("mvn test -Dtest=RangeQueriesTraj_binary -q >> "+str(15-int(sys.argv[1]))+"_worker_range")

time.sleep(60)



#os.system("mvn test -Dtest=RangeQueriesPID_binary -q >> "+str(15-int(sys.argv[1]))+"_worker_range_pid_arr")

#time.sleep(60)

'''

#!/bin/sh
python clusterDecNode_range.py 0 > 15_worker
python clusterDecNode_range.py 3 > 12_worker
python clusterDecNode_range.py 6 > 9_worker
python clusterDecNode_range.py 9 > 6_worker
python clusterDecNode_range.py 12 > 3_worker

'''