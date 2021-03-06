package benchmark

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util
import java.util.List

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{PointST, Triplet}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.rand
import spatial.partition.{MBBindexST, MBBindexSTBlob}
import types.Partitioner
import utils.MbbSerialization

import scala.collection.JavaConverters._

object rangeEvaluation {
  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("rangeEvaluation") //.master("local[*]")
      .config("hive.metastore.uris", "thrift://83.212.100.24:9083")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D' ")
    spark.sql(" CREATE TEMPORARY FUNCTION MbbConstructor AS 'di.thesis.hive.mbb.MbbSTUDF' ")
    spark.sql(" CREATE TEMPORARY FUNCTION ST_IndexIntersects AS 'di.thesis.hive.stoperations.IndexIntersects3D' ")

    spark.sql(" CREATE TEMPORARY FUNCTION ST_IndexIntersectsBinary AS 'di.thesis.hive.stoperations.IndexIntersects3DBinary' ")
    spark.sql(" CREATE TEMPORARY FUNCTION ST_Intersects3DBinary AS 'di.thesis.hive.stoperations.ST_Intersects3DBinary' ")
    spark.sql(" CREATE TEMPORARY FUNCTION MbbConstructorBinary AS 'di.thesis.hive.mbb.MbbSTUDFBinary' ")
    spark.sql(" CREATE TEMPORARY FUNCTION IndexIntersectsTraj AS 'di.thesis.hive.stoperations.IndexIntersectsTraj' ")

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


val arr=spark.sql("select box from index_imis400_binary distribute by rand() sort by rand() limit 2").collect()//.head.getAs[Array[Byte]]("box")


    val obj=arr(0).getAs[Array[Byte]]("box")
    val obj2=arr(1).getAs[Array[Byte]]("box")


    val env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))
    val env2=Some(MbbSerialization.deserialize(obj2.asInstanceOf[Array[Byte]]))

    spark.time({

      spark.sql(" SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
        " FROM (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binary) as a JOIN index_imis400_binaryTraj as b ON (a.trajectory_id=b.id) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)).collect()

      spark.sql(" SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) ".format(env2.get.getMinX, env2.get.getMaxX, env2.get.getMinY, env2.get.getMaxY, env2.get.getMinT, env2.get.getMaxT) +
        " FROM (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binary) as a JOIN index_imis400_binaryTraj as b ON (a.trajectory_id=b.id) ".format(env2.get.getMinX, env2.get.getMaxX, env2.get.getMinY, env2.get.getMaxY, env2.get.getMinT, env2.get.getMaxT)).count()

    })

    spark.stop()
  }
}

/*
/root/spark-2.3.0-bin-hadoop2.7/bin/spark-shell --conf spark.executor.memory=6g \
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
--conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz


/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --conf spark.executor.memory=6g \
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
--conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz
 */
