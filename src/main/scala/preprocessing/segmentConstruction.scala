package preprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id
import types.Trajectory

object segmentConstruction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("trajectoryConstruction") //.master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val prop = spark.sparkContext.getConf

    val output = prop.get("spark.output")
    val path = prop.get("spark.path")

    val traj_dataset=spark.read.parquet(path).as[Trajectory]


        val segments=traj_dataset.flatMap(f=>{
          val id=f.id
          val rowId=f.rowId

          val arr=f.trajectory.sliding(2).map(segment=>{
            Trajectory(id,segment,rowId)
          })
          arr
        }).withColumnRenamed("rowId", "traj_id")

    segments.withColumn("rowId", monotonically_increasing_id() ).write.mode("overwrite").parquet("segments_"+output)

  }
}
