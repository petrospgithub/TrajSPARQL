package apps

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import dbis.stark.STObject
import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner, SpatialPartitioner}
import index.SpatialIndex
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialRDD.SpatialDF
import types.{MovingObject, MovingSpatial, Trajectory}


object SpatialTrajectoryPartition {
  def main(args: Array[String]): Unit = {
//todo!!
    val spark = SparkSession.builder
        .appName("SpatialTrajectoryPartition")
        .getOrCreate()
/*
    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/traj_spatial.properties")

    val prop: Properties = new Properties()
    prop.load(input)

    val host = prop.getProperty("host")
    val user = prop.getProperty("user")
    val pass = prop.getProperty("pass")
    val dbtable = prop.getProperty("dbtable")
    val output = prop.getProperty("output")
    val file_output = prop.getProperty("filetype")

    val geospark_repartition = prop.getProperty("geospark_repartition").toInt
    val geospark_sample_number = prop.getProperty("geospark_sample_number").toLong

    val partitionsPerDimension = prop.getProperty("partitionsPerDimension")
    val dimensions = prop.getProperty("dimensions")
    val sideLength = prop.getProperty("sideLength")
    val maxCostPerPartition = prop.getProperty("maxCostPerPartition")
    val traj_split = prop.getProperty("traj_split")
    val accept_traj = prop.getProperty("accept_traj")
    val path = prop.getProperty("path")

    val rtree_nodeCapacity=prop.getProperty("localIndex_nodeCapacity").toInt
    val name=prop.getProperty("name")
*/

      val prop=spark.sparkContext.getConf
     val host = prop.get("spark.host")
    val user = prop.get("spark.user")
    val pass = prop.get("spark.pass")
    val dbtable = prop.get("spark.dbtable")
    val output = prop.get("spark.output")
    val file_output = prop.get("spark.filetype")

    val geospark_repartition = prop.get("spark.geospark_repartition").toInt
    val geospark_sample_number = prop.get("spark.geospark_sample_number").toLong

    val partitionsPerDimension = prop.get("spark.partitionsperdimension")
    val dimensions = prop.get("spark.dimensions")
    val sideLength = prop.get("spark.sideLength")
    val maxCostPerPartition = prop.get("spark.maxcostperpartition")
    val traj_split = prop.get("spark.traj_split")
    val accept_traj = prop.get("spark.accept_traj")
    val path = prop.get("spark.path")

    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
    val name=prop.get("spark.name")
   /*  */

    val broadcastrtree_nodeCapacity = spark.sparkContext.broadcast(rtree_nodeCapacity)

    import spark.implicits._

    val traj_dataset=spark.read.parquet(path).as[Trajectory]

    val moving_spatial = traj_dataset.map(row => {
      val input = row.trajectory
      val length = input.length

      val coord: Array[Coordinate] = new Array[Coordinate](length)
      var j = 0

      while (j < length) {
        coord(j) = new Coordinate(input(j).getLongitude, input(j).getLatitude)
        j = j + 1
      }

      val geometryFactory = new GeometryFactory()
      val lineString = geometryFactory.createLineString(coord)

      MovingSpatial(row.id, row.trajectory, lineString.toText)
    })


    name match {
      case "quadtree" =>
        val rdd = moving_spatial.rdd.map(row => {
          (new WKTReader().read(row.lineString), row.asInstanceOf[Object])
        })

        val spatialDF = new SpatialDF
        spatialDF.setSampleNumber(geospark_sample_number)
        spatialDF.setRawSpatialRDD(rdd.repartition(geospark_repartition).toJavaRDD())
        spatialDF.analyze()
        spatialDF.spatialPartitioning(GridType.QUADTREE)

        val broadcastBoundary = spark.sparkContext.broadcast(spatialDF.getBoundaryEnvelope)

        val mbbrdd = spatialDF.getSpatialPartitionedRDD.rdd.mapPartitionsWithIndex((index, it) => {
          SpatialIndex.rtree(index, it.asInstanceOf[Iterator[MovingSpatial]].toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
        }, preservesPartitioning = true)

        val repart_traj = spark.createDataset(spatialDF.getSpatialPartitionedRDD.asInstanceOf[JavaRDD[MovingSpatial]])

        val partitionMBBDF = spark.createDataFrame(mbbrdd)

        partitionMBBDF.show()

        partitionMBBDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")
        repart_traj.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")


      case "grid" =>

        val rdd = moving_spatial.rdd.map(row => {
          (STObject.apply(row.lineString), row)
        })

        val minMax = SpatialPartitioner.getMinMax(rdd)
        val env = new Envelope(minMax._1, minMax._2, minMax._3, minMax._4)
        val broadcastBoundary = spark.sparkContext.broadcast(env)

        val partitioner = new SpatialGridPartitioner(rdd, partitionsPerDimension = partitionsPerDimension.toInt, false, minMax, dimensions = dimensions.toInt)

        val repart_traj = spark.createDataset(rdd.partitionBy(partitioner).values)

        val mbbrdd = rdd.partitionBy(partitioner).values.mapPartitionsWithIndex((index: Int, it: Iterator[MovingSpatial]) => {
          SpatialIndex.rtree(index, it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
        }, preservesPartitioning = true)

        val partitionMBBDF = spark.createDataFrame(mbbrdd)

        //partitionMBBDF.show()

        partitionMBBDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")
        repart_traj.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")


      case "bsp" =>

        val rdd = moving_spatial.rdd.map(row => {
          (STObject.apply(row.lineString), row)
        })

        val partitioner = new BSPartitioner(rdd, sideLength = sideLength.toInt, maxCostPerPartition = maxCostPerPartition.toInt)
        val env = new Envelope(partitioner.minX, partitioner.maxX, partitioner.minY, partitioner.maxY)
        val broadcastBoundary = spark.sparkContext.broadcast(env)

        val repart_traj = spark.createDataset(rdd.partitionBy(partitioner).values)

        val mbbrdd = rdd.partitionBy(partitioner).values.mapPartitionsWithIndex((index: Int, it: Iterator[MovingSpatial]) => {
          SpatialIndex.rtree(index, it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
        }, preservesPartitioning = true)

        val partitionMBBDF = spark.createDataFrame(mbbrdd)

        //partitionMBBDF.show()

        partitionMBBDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")
        repart_traj.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_" + args(0) + "_" + output + "_parquet")

    }

    spark.close()
  }
}
