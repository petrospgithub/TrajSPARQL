package benchmark

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.Triplet
import org.apache.spark.sql.{Encoders, SparkSession}
import types.Partitioner
import org.apache.spark.sql.functions.rand
import org.apache.spark.storage.StorageLevel
import spatial.partition.MBBindexSTBlob

import scala.collection.JavaConverters._

object knnEvaluation {

  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("knnEvaluation") //.master("local[*]")
      .config("hive.metastore.uris", "thrift://83.212.100.24:9083")
      .enableHiveSupport()
      .getOrCreate()



    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN_arr AS 'di.thesis.hive.similarity.IndexKNN'")
    spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN_binary AS 'di.thesis.hive.similarity.IndexKNNBinary'")


    spark.sql(" CREATE TEMPORARY FUNCTION DTW_arr AS 'di.thesis.hive.similarity.DTWUDF'")
    spark.sql(" CREATE TEMPORARY FUNCTION DTW_binary AS 'di.thesis.hive.similarity.DTWBinary'")


    spark.sql(" CREATE TEMPORARY FUNCTION IndexStoreTrajKNN_binary AS 'di.thesis.hive.similarity.IndeKNNBinaryStoreTraj'")

    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedList_arr AS 'di.thesis.hive.similarity.ToOrderedList'")
    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedListBinary AS 'di.thesis.hive.similarity.ToOrderedListBinary'")
    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedListBinary AS 'di.thesis.hive.similarity.ToOrderedListBinarySpark'")

    val id=spark.sql("select rowId from trajectories_imis400_binary distribute by rand() sort by rand() limit 1").collect().head.getLong(0)

    spark.time ({

      spark.sql("SELECT final.trajArowid, ToOrderedListBinary(final.distance, final.rowid, 1, final.traja, final.trajb) FROM ( " +
        " SELECT IndexStoreTrajKNN_binary(c.trajectory, d.tree, 40000.1, 604800, 604800, c.rowId, 'DTW', 'Euclidean', 1, 50, 0.1, 0 ) " +
        " FROM ( SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) FROM " +
        " (SELECT * FROM trajectories_imis400_binary where rowId="+id+" ) as a CROSS JOIN partition_index_imis400_binary as b ) as c INNER JOIN " +
        " index_imis400_binaryTraj as d ON (c.trajectory_id=d.id) ) as final GROUP BY final.trajArowid").show()
/*
      spark.sql("SELECT final.trajArowid, ToOrderedListBinary(final.distance, final.rowid, 1, final.traja, final.trajb) FROM ( " +
        " SELECT IndexStoreTrajKNN_binary(c.trajectory, d.tree, 40000.1, 604800, 604800, c.rowId, 'DTW', 'Euclidean', 1, 50, 0.1, 0 ) " +
        " FROM ( SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) FROM " +
        " (SELECT * FROM trajectories_imis400_binary where rowId="+id+" ) as a CROSS JOIN partition_index_imis400_binary as b ) as c INNER JOIN " +
        " index_imis400_binaryTraj as d ON (c.trajectory_id=d.id) ) as final GROUP BY final.trajArowid").collect()

      spark.sql("SELECT final.trajArowid, ToOrderedListBinary(final.distance, final.rowid, 1, final.traja, final.trajb) FROM ( " +
        " SELECT IndexStoreTrajKNN_binary(c.trajectory, d.tree, 40000.1, 604800, 604800, c.rowId, 'DTW', 'Euclidean', 1, 50, 0.1, 0 ) " +
        " FROM ( SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) FROM " +
        " (SELECT * FROM trajectories_imis400_binary where rowId="+id+" ) as a CROSS JOIN partition_index_imis400_binary as b ) as c INNER JOIN " +
        " index_imis400_binaryTraj as d ON (c.trajectory_id=d.id) ) as final GROUP BY final.trajArowid").count()
*/
    })

    spark.stop()
  }
}

/*
spark-shell -jars /root/implementation/HiveTrajSPARQL/HiveTrajSPARQL-jar-with-dependencies.jar
    spark.sql(" ADD JAR file:///root/implementation/HiveTrajSPARQL/HiveTrajSPARQL-jar-with-dependencies.jar ")
    spark.sql(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D' ")


 spark.sql(" ADD JAR /root/implementation/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar ")
    spark.sql(" CREATE TEMPORARY FUNCTION IndexIntersectsTraj AS 'di.thesis.hive.stoperations.IndexIntersectsTraj' ")

spark.sql(" CREATE TEMPORARY FUNCTION MbbConstructorBinary AS 'di.thesis.hive.mbb.MbbSTUDFBinary' ")
    spark.sql(" CREATE TEMPORARY FUNCTION ST_IndexIntersectsBinary AS 'di.thesis.hive.stoperations.IndexIntersects3DBinary' ")

spark.sql("select box from index_imis400 distribute by rand() sort by rand() limit 1").collect.foreach(p=>println(p))


spark.sql(" SELECT IndexIntersectsTraj(MbbConstructorBinary( 2562556.1248126877, 2628152.990087475, 4559502.846446961, 4573813.639087068, CAST(1221926524 as BIGINT), CAST(1221982681 as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( 2562556.1248126877, 2628152.990087475, 4559502.846446961, 4573813.639087068, CAST(1221926524 as BIGINT), CAST(1221982681 as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binary) as a JOIN index_imis400_binaryTraj as b ON (a.trajectory_id=b.id) ")


 */