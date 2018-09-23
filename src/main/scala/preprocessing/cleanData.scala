package preprocessing

import distance.Distance
import org.apache.spark.sql.SparkSession
import types.SpatioTemporalPoints
import org.apache.spark.sql.functions._

object cleanData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("clean_data") //.master("local[*]")
      .getOrCreate()

    val prop = spark.sparkContext.getConf

    val minx=prop.get("spark.minx")
    val maxx=prop.get("spark.maxx")
    val miny=prop.get("spark.miny")
    val maxy=prop.get("spark.maxy")

    val broadcastMinLon=spark.sparkContext.broadcast(minx.toDouble)
    val broadcastMaxLon=spark.sparkContext.broadcast(maxx.toDouble)
    val broadcastMinLat=spark.sparkContext.broadcast(miny.toDouble)
    val broadcastMaxLat=spark.sparkContext.broadcast(maxy.toDouble)

    import spark.implicits._


    val delimiter=prop.get("spark.delimiter").isEmpty match {
      case true=> " "
      case false=>  prop.get("spark.delimiter")
    }

    val csvDF = spark.read.option("delimiter", delimiter).option("header", prop.get("spark.header")).csv(prop.get("spark.path"))


    val pointST = csvDF.map(input => {
      val min_lon=broadcastMinLon.value
      val max_lon=broadcastMaxLon.value
      val min_lat=broadcastMinLat.value
      val max_lat=broadcastMaxLat.value

      val ts = input.getAs[String](0)
      val id = input.getAs[String](1)
      val lon = input.getAs[String](2)
      val lat = input.getAs[String](3)

      if (min_lon <= lon.toDouble && lon.toDouble <= max_lon &&
        min_lat<= lat.toDouble && lat.toDouble <= max_lat
      ) {
        SpatioTemporalPoints(Some(id.toLong), Some(lon.toDouble), Some(lat.toDouble), Some(ts.toLong))
      } else {SpatioTemporalPoints(None, None, None, None)}
    }).na.drop()


    def speed_sampling_udf = udf((lon:Double, lat:Double, t: Long,
                                  prev_lon:Double, prev_lat:Double, prev_t:Long,
                                  next_lon:Double, next_lat:Double, next_t:Long
                                 ) => Option[(Double,Long, Double)] {
      (
        (Distance.getHaversineDistance(prev_lat,prev_lon, lat, lon) / (t - prev_t).toDouble)*1.943844492,
        t - prev_t,
        (Distance.getHaversineDistance(lat,lon, next_lat, next_lon) / (next_t - t).toDouble)*1.943844492
      )
    })

    import org.apache.spark.sql.expressions.Window
    @transient val windowSpec = Window.partitionBy('id).orderBy('timestamp)

    val speed_sampling=pointST.withColumn("speed_sampling",
      speed_sampling_udf(
        'longitude, 'latitude,
        'timestamp,
        lag('longitude, 1) over windowSpec,
        lag('latitude, 1) over windowSpec,
        lag('timestamp, 1) over windowSpec,
        lead('longitude, 1) over windowSpec,
        lead('latitude, 1) over windowSpec,
        lead('timestamp, 1) over windowSpec
      )
    )

    speed_sampling.filter(!($"speed_sampling._1" >=120 && $"speed_sampling._2"<=600))

    val result=speed_sampling.filter( $"speed_sampling._1"<=prop.get("spark.vessel_acceptable_speed").toDouble && $"speed_sampling._3"<=prop.get("spark.vessel_acceptable_speed").toDouble )

    result.select($"id", $"longitude", $"latitude", $"timestamp").write.mode("overwrite").csv(prop.get("spark.output"))

    spark.close()

  }

}

