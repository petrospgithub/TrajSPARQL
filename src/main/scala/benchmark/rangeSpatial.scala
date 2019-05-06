package benchmark

import org.apache.spark.sql.SparkSession
import utils.MbbSerialization

object rangeSpatial {
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

    spark.sql(" CREATE TEMPORARY FUNCTION SP_TrajSPMBR AS 'di.thesis.hive.stoperations.SP_TrajSPMBR' ")

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    val arr=spark.sql("select box from index_imis400_temp_binary distribute by rand() sort by rand() limit 2").collect()//.head.getAs[Array[Byte]]("box")


    val obj=arr(0).getAs[Array[Byte]]("box")
    val obj2=arr(1).getAs[Array[Byte]]("box")


    val env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))
    val env2=Some(MbbSerialization.deserialize(obj2.asInstanceOf[Array[Byte]]))


    spark.time({

     spark.sql(" SELECT * FROM " +
        "(SELECT SP_TrajSPMBR(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400) as b INNER JOIN trajectories_imis400_binary_pid as a ON (b.trajectory_id=a.pid) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)).collect()

      spark.sql( " SELECT * FROM " +
        "(SELECT SP_TrajSPMBR(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400) as b INNER JOIN trajectories_imis400_binary_pid as a ON (b.trajectory_id=a.pid) ".format(env2.get.getMinX, env2.get.getMaxX, env2.get.getMinY, env2.get.getMaxY, env2.get.getMinT, env2.get.getMaxT) +
        " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ".format(env2.get.getMinX, env2.get.getMaxX, env2.get.getMinY, env2.get.getMaxY, env2.get.getMinT, env2.get.getMaxT)).collect()


    })

    spark.stop()
  }
}
