package preprocessing

import org.apache.spark.sql.SparkSession
import types.SpatioTemporalPoints
import utils.TransformSRID

object transform2meters {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder
      .appName("transformData") //.master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.csv(args(0))

    ds.map(input=>{
      val id = input.getAs[String](0)
      val lon = input.getAs[String](1)
      val lat = input.getAs[String](2)
      val ts = input.getAs[String](3)

      val geom=TransformSRID.myMeters(lon.toDouble, lat.toDouble)

      SpatioTemporalPoints(Some(id.toLong), Some(geom._1), Some(geom._2), Some(ts.toLong))

    }).write.mode("overwrite").csv(args(1))

  }
}
