import ConfigParser
import os
import sys
import time

config = ConfigParser.RawConfigParser()
config.read('./config/traj_octree.properties')

dataset_sample = [('0.20', 12),
                  ('0.40', 9),
                  ('0.60', 6),
                  ('0.80', 3),
                  ('0.90', 0)]

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

myfile = '/root/hadoop-3.1.1/etc/hadoop/yarn.exclude'

f=open(myfile, 'w')

#level = ['90', '180', '360']

for d in dataset_sample:
    cfgfile = open("./config/traj_octree.properties", 'w')
    config.set('Spark', 'spark.fraction_dataset', d[0])

    for i in range(0, d[1]):
        f.write(patterns[i] + "\n")

    f.close()

    os.system("kill -9 $(lsof -t -i:9083)")

    time.sleep(10)

    os.system("kill -9 $(lsof -t -i:10000)")

    time.sleep(10)

    os.system("yarn rmadmin -refreshNodes")

    time.sleep(10)

    os.system("stop-all.sh")

    time.sleep(10)

    os.system("start-all.sh")

    time.sleep(10)

    os.system("yarn rmadmin -refreshNodes")

    time.sleep(10)

    os.system(" nohup /root/apache-hive-2.3.3-bin/bin/hive --service metastore > metastore.out & ")

    time.sleep(10)

    os.system(" nohup /root/apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=6144 --hiveconf mapreduce.map.java.opts=-Xmx8192m --hiveconf mapreduce.reduce.memory.mb=6144 --hiveconf mapreduce.reduce.java.opts=-Xmx8192m > hiveserver.out & ")

    time.sleep(10)

    os.system("mvn test -Dtest=RangeQueries -q > scale_"+d[1]+"_"+d[0])

    config.write(cfgfile)
    cfgfile.close()


'''
mvn test -Dtest=CreateTables -q

mvn test -Dtest=DropTables -q
'''