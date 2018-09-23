package preprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.functions._
import types.SpatioTemporalPoints


object trajectoryConstruction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("trajectoryConstruction")//.master("local[*]")
      .getOrCreate()
/*
    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/preprocessing.properties")

    val prop: Properties = new Properties()
    prop.load(input)
    val output = prop.getProperty("spark.output")
    val path = prop.getProperty("spark.path")
    val traj_split = prop.getProperty("spark.traj_split").toDouble
*/

    val prop = spark.sparkContext.getConf

    val output = prop.get("spark.output")
    val path = prop.get("spark.path")
    val traj_split = prop.get("spark.traj_split").toInt

    val broadcastSplit=spark.sparkContext.broadcast(traj_split.toDouble)

    import spark.implicits._

    val csvDF = spark.read.option("delimiter", ",").option("header", "true").csv(path)//.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val pointST = csvDF.map(input => {
      val ts = input.getAs[String](3)
      val id = input.getAs[String](0)
      val lon = input.getAs[String](1)
      val lat = input.getAs[String](2)

      SpatioTemporalPoints(Some(id.toLong), Some(lon.toDouble), Some(lat.toDouble), Some(ts.toLong))
    })

    import org.apache.spark.sql.expressions.Window
    @transient val windowSpec = Window.partitionBy('id).orderBy('timestamp)

    val sampling=pointST.withColumn("sampling",
      'timestamp - (lag('timestamp, 1) over windowSpec)
    )

    val distinct=sampling.select('id).distinct().count().toInt

    val foo=sampling.repartition(distinct, 'id).mapPartitions(partition=>{

      var traj_id:Int=1
      partition.map(row=>{

        if (row.isNullAt(4)) {
          (row.getAs[Long](0), row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3), traj_id)
        } else {
          if (row.getAs[Long](4) <= broadcastSplit.value) {
            (row.getAs[Long](0), row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3), traj_id)
          } else {
            traj_id=traj_id+1
            (row.getAs[Long](0), row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3), traj_id)
          }
        }
      })

    })

    foo.show()
    //println(foo.rdd.getNumPartitions)

    val traj=foo.groupBy('_1,'_5).agg(collect_list(struct('_2.alias("longitude"), '_3.alias("latitude"), '_4.alias("timestamp"))) as "trajectory")
      //.persist(StorageLevel.MEMORY_AND_DISK_SER)

//    traj.printSchema()

    @transient val rowNum = Window.partitionBy('id).orderBy('trajectory.getField("timestamp"))

    traj.select('_1.alias("id"), 'trajectory).filter(size('trajectory)>1).withColumn("rowId", row_number() over(rowNum) ).write.mode("overwrite").parquet("trajectories_"+output)

    spark.close()
  }
}

/*
TODO

val broadcastMinLon=spark.sparkContext.broadcast(12.83203)
val broadcastMaxLon=spark.sparkContext.broadcast(36.29883)
val broadcastMinLat=spark.sparkContext.broadcast(30.05642)
val broadcastMaxLat=spark.sparkContext.broadcast(41.23376)


import spark.implicits._
case class ZipPoints(id:Option[Long], longitude:Option[Double], latitude:Option[Double], timestamp:Option[Long])


val csvDF = spark.read.option("delimiter", " ").option("header", "false").csv("imis3years_old/split_imis_3yearsaa.gz")

val csvDF = spark.read.option("delimiter", " ").option("header", "false").csv("imis3years_old")
csvDF.count

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
        ZipPoints(Some(id.toLong), Some(lon.toDouble), Some(lat.toDouble), Some(ts.toLong))
      } else {ZipPoints(None, None, None, None)}
    }).na.drop

pointST.count
pointST.show

object Distance {
  def getHaversineDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double):Double={
    val deltaLat = Math.toRadians(lat2 - lat1)
    val deltaLon = Math.toRadians(lon2 - lon1)
    val a = Math.pow(Math.sin(deltaLat / 2.0D), 2) + Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(lat1)) * Math.pow(Math.sin(deltaLon / 2.0D), 2)
    val greatCircleDistance = 2.0D * Math.atan2(Math.sqrt(a), Math.sqrt(1.0D - a))
    //3958.761D * greatCircleDistance           //Return value in miles
    //3440.0D * greatCircleDistance             //Return value in nautical miles
    6371000.0D * greatCircleDistance            //Return value in meters, assuming Earth radius is 6371 km
  }
}

def speed_sampling_udf = udf((lon:Double, lat:Double, t: Long, prev_lon:Double, prev_lat:Double, prev_t:Long) => Option[(Double,Long)] {
      ( (Distance.getHaversineDistance(prev_lat,prev_lon, lat, lon) / (t - prev_t).toDouble)*1.943844492, t - prev_t)
    })

import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy('id).orderBy('timestamp)

    val speed_sampling=pointST.withColumn("speed_sampling",
      speed_sampling_udf(
        'longitude, 'latitude,
        'timestamp,
        lag('longitude, 1) over windowSpec,
        lag('latitude, 1) over windowSpec,
        lag('timestamp, 1) over windowSpec
      )
    )

speed_sampling.count
speed_sampling.show

speed_sampling.select($"speed_sampling._1").describe().show

/* speed describe
+-------+------------------+
|summary|                _1|
+-------+------------------+
|  count|        1562852950|
|   mean|29.162405515743952|
| stddev|2983.2087267883026|
|    min|               0.0|
|    max| 3246675.595869456|
+-------+------------------+
*/



val result=speed_sampling.filter(($"speed_sampling._1"<=240 && $"speed_sampling._2"<=1800) || $"speed_sampling._1".isNull)

//to sampling den xreiazetai gt upologizetai alliws kai meta ginetai to pruning

//val result=speed_sampling.filter($"speed_sampling._1"<=240 || $"speed_sampling._1".isNull) //auto paizei twra

result.show

result.select($"id", $"longitude", $"latitude", $"timestamp").write.mode("overwrite").option("delimiter", ",").csv("imis3years_cleansed")


val test=pointST.na.drop

1934322863
1562890330 (after remove outliers from mbb)
1562890330
1550494703

split 1800 -> 2079075 trajectories
split 600 -> 7069442 trajectories
oasa split 600 -> 3687979 trajectories
 */



