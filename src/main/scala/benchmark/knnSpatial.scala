package benchmark

import org.apache.spark.sql.SparkSession

object knnSpatial {

  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("knnEvaluation") //.master("local[*]")
      .config("hive.metastore.uris", "thrift://83.212.100.24:9083")
      .enableHiveSupport()
      .getOrCreate()


    val dist=args(0).toDouble
    val time=args(1).toInt

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN_arr AS 'di.thesis.hive.similarity.IndexKNN'")
    spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN_binary AS 'di.thesis.hive.similarity.IndexKNNBinary'")


    spark.sql(" CREATE TEMPORARY FUNCTION DTW_arr AS 'di.thesis.hive.similarity.DTWUDF'")
    spark.sql(" CREATE TEMPORARY FUNCTION DTW_binary AS 'di.thesis.hive.similarity.DTWBinary'")


    spark.sql(" CREATE TEMPORARY FUNCTION IndexStoreTrajKNN_binary AS 'di.thesis.hive.similarity.IndeKNNBinaryStoreTraj'")

    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedList_arr AS 'di.thesis.hive.similarity.ToOrderedList'")
    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedListBinary AS 'di.thesis.hive.similarity.ToOrderedListBinary'")
    spark.sql(" CREATE TEMPORARY FUNCTION ToOrderedListBinarySpark AS 'di.thesis.hive.similarity.ToOrderedListBinarySpark'")

    val arr=spark.sql("select rowId from trajectories_imis400_pid distribute by rand() sort by rand() limit 2").collect()

    val id1=arr(0).getLong(0)
    val id2=arr(1).getLong(0)


    /* TODO add extreme time tolerance!!!
    spark.time ({

      spark.sql(" SELECT c.rowId, ToOrderedList_arr( DTW_arr(c.trajectory, trajectories_imis400_pid.trajectory, 50, 'Euclidean', 604800, 604800), trajectories_imis400_pid.rowId, '-k -1', c.trajectory, trajectories_imis400_pid.trajectory )" +
        " FROM " +
        " (SELECT IndexTrajKNN_arr(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) " +
        " FROM (SELECT * FROM trajectories_imis400_pid WHERE rowId=" + id1 + ") as a JOIN partition_index_imis400 as b ) as c " +
        " JOIN trajectories_imis400_pid ON (c.trajectory_id==trajectories_imis400_pid.pid) " +
        " GROUP BY c.rowId ").collect()

      spark.sql(" SELECT c.rowId, ToOrderedList_arr( DTW_arr(c.trajectory, trajectories_imis400_pid.trajectory, 50, 'Euclidean', 604800, 604800), trajectories_imis400_pid.rowId, '-k -1', c.trajectory, trajectories_imis400_pid.trajectory )" +
        " FROM " +
        " (SELECT  " +
        " FROM (SELECT * FROM trajectories_imis400_pid WHERE rowId=" + id2 + ") as a JOIN partition_index_imis400 as b ) as c " +
        " JOIN trajectories_imis400_pid ON (c.trajectory_id==trajectories_imis400_pid.pid) " +
        " GROUP BY c.rowId ").collect()

    })
*/
    spark.stop()
  }
}
