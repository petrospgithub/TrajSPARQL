package benchmark

import org.apache.spark.sql.SparkSession
import spatial.partition.MBBindexST
import types.Partitioner

object createResources {
  def main(args: Array[String]): Unit = {


   // val traj_part=args(2)


    val spark = SparkSession.builder
      .appName("TrajectoryOctree")//.master("local[*]")
      .getOrCreate()

    val traj_path=args(0)
    val traj_index=args(1) //index with trajectories

    import spark.implicits._

    val trajectoryDS=spark.read.parquet(traj_path).as[Partitioner]

    val pid=trajectoryDS.select('pid).distinct().count().toInt


    trajectoryDS.repartition(pid, $"pid").write.option("compression", "snappy").mode("overwrite").parquet("trajectories_benchmark")

    spark.read.parquet(traj_index).as[MBBindexST].repartition(pid, $"id").write.option("compression", "snappy").mode("overwrite").parquet("index_benchmark")


    spark.stop()
  }

}
