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

os.system("nohup /root/apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=6144 --hiveconf mapreduce.map.java.opts=-Xmx8192m --hiveconf mapreduce.reduce.memory.mb=6144 --hiveconf mapreduce.reduce.java.opts=-Xmx8192m > hiveserver.out &")

time.sleep(10)

os.system("mvn test -Dtest=knnQueries -q ")

'''

#!/bin/sh
python clusterDecNode.py 0 > 15_worker
python clusterDecNode.py 3 > 12_worker
python clusterDecNode.py 6 > 9_worker
python clusterDecNode.py 9 > 6_worker
python clusterDecNode.py 12 > 3_worker

'''