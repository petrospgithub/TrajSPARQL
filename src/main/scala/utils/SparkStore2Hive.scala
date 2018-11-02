package utils
import  org.apache.hadoop.fs.{FileSystem,Path}

object SparkStore2Hive {

  /*
  hive-site.xml must be at Spark config folder

  destination table must exists with 4 columns (obj_id, trajectory, rowId, partid or obj_id, trajectory, traj_id, rowId, partid for segments)

   */

  def main(args:Array[String]):Unit= {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("store @ hive").config("hive.metastore.uris","thrift://TrajSPARQLmaster:9083").enableHiveSupport().getOrCreate()

    //FileSystem.get( spark.sparkContext.hadoopConfiguration ).listStatus( new Path("hdfs:///user/root/trajectories_repart_imis400")).foreach( x => println(x.getPath ))
    //spark.sparkContext.hadoopConfiguration

    import spark.implicits._

    val df2store_traj=spark.read.parquet("octree_repartition_imis400_parquet")

    val part_counter=df2store_traj.select('pid).distinct().count()

    df2store_traj.repartition(part_counter.toInt, 'pid).write.partitionBy("pid").parquet("trajectories_repart_imis400")


    spark.sql("CREATE EXTERNAL TABLE imis400_table(obj_id BIGINT, trajectory ARRAY<STRUCT<longitude:DOUBLE, latitude:DOUBLE, `timestamp`:BIGINT >>, traj_id BIGINT, rowId BIGINT, pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/trajectories_repart_imis400'")

    spark.stop()



  }
}
