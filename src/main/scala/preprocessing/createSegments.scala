package preprocessing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import types.{CPointST, SpatioTemporalPoints}

object createSegments {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("trajectoryConstruction").master("local[*]")
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

    val csvDF = spark.read.option("delimiter", ",").option("header", "false").csv(path)//.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val pointST = csvDF.map(input => {
      val ts = input.getAs[String](3)
      val id = input.getAs[String](0)
      val lon = input.getAs[String](1)
      val lat = input.getAs[String](2)

      SpatioTemporalPoints(Some(id.toLong), Some(lon.toDouble), Some(lat.toDouble), Some(ts.toLong))
    })

    import org.apache.spark.sql.expressions.Window
    @transient val windowSpec = Window.partitionBy('id).orderBy('timestamp)

    def prev_udf = udf ( (prev_lon:Double, prev_lat:Double, prev_t:Long) => Option[(Double, Double, Long)] {
      (prev_lon,prev_lat,prev_t)
    })

    val sampling=pointST.withColumn("sampling",
      'timestamp - (lag('timestamp, 1) over windowSpec)
    ).withColumn("prev", prev_udf(
      lag('longitude, 1) over windowSpec,
      lag('latitude, 1) over windowSpec,
      lag('timestamp, 1) over windowSpec
    )
    )

    val distinct=sampling.select('id).distinct().count().toInt

    val foo=sampling.repartition(distinct, 'id).mapPartitions(partition=>{

      var traj_id:Int=1
      partition.map(row=>{

        if (row.isNullAt(4)) {
          (row.getAs[Long](0), Array[CPointST](CPointST(row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3))), traj_id) //TODO return mvoing objects here!!!
        } else {
          if (row.getAs[Long](4) <= broadcastSplit.value) {

            val prev=row.getAs[GenericRowWithSchema](5)

            val prev_lon=prev.getAs[Double](0)
            val prev_lat=prev.getAs[Double](1)
            val prev_ts=prev.getAs[Long](2)

            (row.getAs[Long](0), Array[CPointST](CPointST(prev_lon, prev_lat,prev_ts), CPointST(row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3)) ), (""+row.getAs[Long](0)+traj_id).hashCode )

          } else {
            traj_id=traj_id+1
            (row.getAs[Long](0), Array[CPointST](CPointST(row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3))), traj_id) //TODO return mvoing objects here!!!
          }
        }
      })

    }).filter(size('_2)>1)

    foo.withColumn("rowId", monotonically_increasing_id() ).write.mode("overwrite").parquet("segments"+output)//.show()

      //


    /*
    val foo=sampling.repartition(distinct, 'id).mapPartitions(partition=>{

      var traj_id:Int=1
      partition.map(row=>{

        if (row.isNullAt(4)) {
          (row.getAs[Long](0), row.getAs[Double](1),row.getAs[Double](2), row.getAs[Long](3), traj_id) //TODO return mvoing objects here!!!
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

    // @transient val rowNum = Window.partitionBy('id).orderBy('trajectory.getField("timestamp"))

    traj.select('_1.alias("id"), 'trajectory).filter(size('trajectory)>1).withColumn("rowId", monotonically_increasing_id() ).write.mode("overwrite").parquet("trajectories_"+output)
*/
    spark.close()
  }
}
