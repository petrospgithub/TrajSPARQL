STIndexing source code github -> https://github.com/petrospgithub/STIndexing

big data frameworks folder -> https://mega.nz/#!bg0WkIwS!bS4Wk3zQUFLTCvqs6z7lcjDOMLwbcjCR9oBCgZ520EU
                           -> https://www.dropbox.com/s/k6xd6rscvj2v4uo/bigspatial_frameworks.tar.gz?dl=0
    
mvn install:install-file -Dfile=<path>/STIndexing-jar-with-dependencies.jar -DgroupId=di.thesis -DartifactId=stindexing -Dversion=1.0 -Dpackaging=jar
                           
mvn install:install-file -Dfile=bigspatial_frameworks/geospark-0.8.2.jar -DgroupId=org.datasyslab -DartifactId=geospark -Dversion=0.8.2 -Dpackaging=jar

mvn install:install-file -Dfile=bigspatial_frameworks/stark.jar -DgroupId=dbis -DartifactId=stark -Dversion=1.0 -Dpackaging=jar

STIndexing source code github ->

mvn install:install-file \ 
-Dfile=external_jar/STIndexing-jar-with-dependencies.jar \
-DgroupId=di.thesis \ 
-DartifactId=stindexing \
-Dversion=1.0 -Dpackaging=jar

Execution

spark-submit --master local[*] \
--properties-file "./config/geospark_storeHDFS.properties" \
--class spatial.partition.GeoSparkPartitioner \
 target/TrajSPARQL-jar-with-dependencies.jar postgres
 
spark-submit --master local[*] \
--properties-file "./config/stark_storeHDFS.properties" \
--class spatial.partition.StarkPartitioner \
 target/TrajSPARQL-jar-with-dependencies.jar postgres


spark-submit --master local[*] \
--properties-file "./config/spatial_rtree_storeHDFS.properties" \
--class spatial.partition.SpatialRtree \
 target/TrajSPARQL-jar-with-dependencies.jar postgres
/~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~/

preprocessing

spark-submit --master local[*] \
--properties-file "./config/traj_grid.properties" \
--class preprocessing.TrajectoryAvgSampling \
 target/TrajSPARQL-jar-with-dependencies.jar csv
 
 spark-submit --master local[*] \
 --properties-file "./config/traj_grid.properties" \
 --class preprocessing.TrajStats \
  target/TrajSPARQL-jar-with-dependencies.jar csv

read zip

 spark-submit --master local[*] \
 --class ReadZip \
  target/TrajSPARQL-jar-with-dependencies.jar sample_data/split_imis_3yearsae.zip unzip_dataset

/~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~/

trajectories!

spark-submit --master local[*] \
 --properties-file "./config/traj_grid.properties" \
 --class apps.TrajectorySTPartition \
 --driver-memory 8g \
 --executor-memory 4g \
 target/TrajSPARQL-jar-with-dependencies.jar csv
 
 spark-submit --master local[*] \
 --properties-file "./config/traj_bsp.properties" \
 --class apps.BSPApp \
 --driver-memory 8g \
 --executor-memory 4g \
 target/TrajSPARQL-jar-with-dependencies.jar csv
  
spark-submit --master local[*] \
 --properties-file "./config/traj_octree.properties" \
 --class apps.OcTreeApp \
 --driver-memory 8g \
 --executor-memory 4g \
 target/TrajSPARQL-jar-with-dependencies.jar csv
    
spark-submit --master local[*] \
 --properties-file "./config/traj_rtree.properties" \
 --class apps.RtreeApp \
 --driver-memory 8g \
 --executor-memory 4g \
 target/TrajSPARQL-jar-with-dependencies.jar csv
 
 
 
 /~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~/
 
cluster!!!

spark-submit --master yarn --deploy-mode cluster --properties-file "./config/geospark_storeHDFS.properties" --class TestCluster target/TrajSPARQL-jar-with-dependencies.jar csv
spark-submit --master yarn --deploy-mode client --properties-file "./config/geospark_storeHDFS.properties" --class TestCluster target/TrajSPARQL-jar-with-dependencies.jar csv


spark-submit --master yarn --deploy-mode client \
--properties-file "./config/traj_grid.properties" \
--class preprocessing.TrajectoryAvgSampling \
target/TrajSPARQL-jar-with-dependencies.jar csv

spark-submit --master yarn --deploy-mode client \
--properties-file "./config/traj_grid.properties" \
--class preprocessing.TrajStats \
target/TrajSPARQL-jar-with-dependencies.jar csv

/~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~/

TIPS
select regexp_replace(name, '[^\w]+','')
from roads



spark-submit --class binary.OcTreeAppBinary \
 --properties-file "./config/traj_octree.properties" \
 target/TrajSPARQL-jar-with-dependencies.jar
 
 spark-submit --class binary.OcTreeAppBinaryStoreTraj \
  --properties-file "./config/traj_octree.properties" \
  target/TrajSPARQL-jar-with-dependencies.jar