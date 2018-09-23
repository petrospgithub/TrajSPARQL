package preprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import types.MovingObject
//todo add real trajectory length not mbb


object TrajStats {

  case class Stats(length: Double, duration: Long, sampling: Double, numOfPoint:Int, avg_speed:Double)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("TrajStats")//.master("local[*]")
      .getOrCreate()
    /*
          val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/traj_grid.properties")

          val prop: Properties = new Properties()
          prop.load(input)


          val output = prop.getProperty("spark.output")
          val path = prop.getProperty("spark.path")
    */

    val prop = spark.sparkContext.getConf

    val output = prop.get("spark.output")

    val path = prop.get("spark.path")

    import spark.implicits._

    val traj_dataset=spark.read.parquet(path).as[MovingObject]

    traj_dataset.map(mo => {
      Stats(mo.length, mo.duration,mo.sampling, mo.trajectory.length, mo.avg_speed)
    }).persist(StorageLevel.MEMORY_AND_DISK).describe().write.mode("overwrite").format("csv").save("traj_dataset_stats_describe_" + output)

    spark.close()
  }

}
