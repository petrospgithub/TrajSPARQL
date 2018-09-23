package preprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import types.SpatioTemporalPoints

object AvgSampling {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder
      .appName("AvgSampling")//.master("local[*]")
      .getOrCreate()

    val prop = spark.sparkContext.getConf

    val output = prop.get("spark.output")
    val path = prop.get("spark.path")

    import spark.implicits._

    val csvDF = spark.read.option("delimiter", ",").option("header", "true").csv(path)//.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val pointST=csvDF.map(input=>{
      //val arr=input.getAs[String](0).split(",")

      val ts=input.getAs[String](3)
      val id=input.getAs[String](0)
      val lon=input.getAs[String](1)
      val lat=input.getAs[String](2)

/*
      val ts=input.getAs[String](0)
      val id=input.getAs[String](1)
      val lon=input.getAs[String](2)
      val lat=input.getAs[String](3)
*/
      SpatioTemporalPoints(Some(id.toLong), Some(lon.toDouble), Some(lat.toDouble), Some(ts.toLong))
    })//.persist(StorageLevel.MEMORY_AND_DISK_SER)

    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy('id).orderBy('timestamp)

    val sampling=pointST.withColumn("diff", 'timestamp - (lag('timestamp, 1) over windowSpec) ).select('diff).persist(StorageLevel.MEMORY_AND_DISK_SER)

    sampling.foreach(row=>{
      None
    })

    pointST.unpersist()

    sampling.write.mode("overwrite").parquet("traj_dataset_sampling_diff_" + output)
    sampling.describe().write.mode("overwrite").parquet("traj_dataset_sampling_describe_" + output)

    spark.close()
  }
}
