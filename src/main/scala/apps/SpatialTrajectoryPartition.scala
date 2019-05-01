package apps

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import dbis.stark.STObject
import dbis.stark.spatial.partitioner.{SpatialGridPartitioner, SpatialPartitioner}
import di.thesis.indexing.spatialextension.STRtreeObjID
import di.thesis.indexing.types.{EnvelopeST, GidEnvelope}
import index.{SpatialIndex, SpatioTemporalIndex}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialRDD.SpatialDF
import spatiotemporal.STGrid
import types.{MovingObject, MovingSpatial, Trajectory, Tree}
import utils.MbbSerialization


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

    /*
     val host = prop.get("spark.host")
    val user = prop.get("spark.user")
    val pass = prop.get("spark.pass")
    val dbtable = prop.get("spark.dbtable")
    val file_output = prop.get("spark.filetype")
*/

    val output = prop.get("spark.output")

    val geospark_repartition = prop.get("spark.geospark_repartition").toInt
    val geospark_sample_number = prop.get("spark.geospark_sample_number").toLong

    //val sideLength = prop.get("spark.sideLength")

    val partitionsPerDimension = prop.get("spark.partitionsperdimension")
    val dimensions = 2//prop.get("spark.dimensions")

    /*
    val partitionsPerDimension = prop.get("spark.partitionsperdimension")
    val dimensions = prop.get("spark.dimensions")
    val sideLength = prop.get("spark.sideLength")
    val maxCostPerPartition = prop.get("spark.maxcostperpartition")
    val traj_split = prop.get("spark.traj_split")
    val accept_traj = prop.get("spark.accept_traj")
*/
    val path = prop.get("spark.path")

    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
    val name = prop.get("spark.name")
   /*  */

    val broadcastrtree_nodeCapacity = spark.sparkContext.broadcast(rtree_nodeCapacity)

    import spark.implicits._

    val traj_dataset=spark.read.parquet(path).as[Trajectory]


    val mbbst:EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]])

    val broadcastBoundary=spark.sparkContext.broadcast(mbbst)


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

      //geom.setSRID(3857)

      //val lineString = TransformSRID.to4326(geom)

   //   if (lineString.isValid) {
        MovingSpatial(Some(row.id), Some(row.trajectory), Some(row.rowId), Some(lineString.toText))
    //  } else {
    //    MovingSpatial(None, None, None, None)
     // }

    }).filter(row=>{
      val geom=new WKTReader().read(row.lineString.get)
      geom.isValid
    })


    name match {
      case "quadtree" =>
        val rdd = moving_spatial.rdd.map(row => {

          val geom=new WKTReader().read(row.lineString.get)
          (geom.getBoundary, row.asInstanceOf[Object])
        })

        val spatialDF = new SpatialDF
        spatialDF.setSampleNumber(geospark_sample_number)
        spatialDF.setRawSpatialRDD(rdd.repartition(geospark_repartition).toJavaRDD())
        spatialDF.analyze()
        spatialDF.spatialPartitioning(GridType.QUADTREE)

        val quadRDD = spatialDF.getSpatialPartitionedRDD.rdd.mapPartitionsWithIndex((index, it) => {
          //SpatialIndex.rtree(index, it.asInstanceOf[Iterator[MovingSpatial]].toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
          SpatialIndex.rtree(index, it.asInstanceOf[Iterator[MovingSpatial]].toArray)
        }, preservesPartitioning = true)

       val repartition = spark.createDataset(quadRDD)

        val partitions_counter = repartition.groupBy('pid).count()

        partitions_counter.write.csv("quadtree_binary_traj_partitions_counter_" + output + "_" + geospark_repartition + "_" + geospark_sample_number)


        val partitionMBB = repartition.groupByKey(p => p.id).mapGroups({
          (id, it) => {
            SpatioTemporalIndex.rtreeblob_store_traj(it, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
          }
        })

        partitionMBB.write.option("compression", "snappy").mode("overwrite").parquet("quadtree_traj_partitionMBBDF_binary_" + output + "_parquet")
        repartition.write.option("compression", "snappy").mode("overwrite").parquet("quadtree_traj_repartition_binary_" + output + "_parquet")


        partitionMBB.repartition(1).mapPartitions(f => {
          val rtree3D: STRtreeObjID = new STRtreeObjID()

          rtree3D.setDatasetEnvelope(broadcastBoundary.value.jtsGeom().getEnvelopeInternal)

          while (f.hasNext) {
            val temp = f.next()
            val temp_mbbst = MbbSerialization.deserialize(temp.box.get)

            //val envelope = new EnvelopeST(temp_mbbst.getMinX, temp_mbbst.getMaxX, temp_mbbst.getMinY, temp_mbbst.getMaxY, temp_mbbst.getMinT, temp_mbbst.getMaxT)

            val envelope:GidEnvelope = new GidEnvelope(temp_mbbst.getMinX, temp_mbbst.getMaxX, temp_mbbst.getMinY, temp_mbbst.getMaxY)

            envelope.setGid(temp.id.get)

            rtree3D.insert(envelope, envelope)
          }

          rtree3D.build()

          val bos = new ByteArrayOutputStream()

          val out = new ObjectOutputStream(bos)
          out.writeObject(rtree3D)
          out.flush()
          val yourBytes = bos.toByteArray.clone()

          out.close()

          Iterator(Tree(Some(yourBytes)))
        }).write.option("compression", "snappy").mode("overwrite").parquet("partitions_quad_binary_" + output + "_parquet")

      case "grid" =>

        val rdd = moving_spatial.rdd.map(row => {
          (STObject.apply(row.lineString.get), row)
        })

        val minMax = SpatialPartitioner.getMinMax(rdd)
        val env = new Envelope(minMax._1, minMax._2, minMax._3, minMax._4)
        val partitioner = new SpatialGridPartitioner(rdd, partitionsPerDimension = partitionsPerDimension.toInt, false, minMax, dimensions = dimensions.toInt)

        val starkRDD = rdd.partitionBy(partitioner).values.mapPartitionsWithIndex((index, it) => {
          SpatialIndex.rtree(index, it.toArray)
        }, preservesPartitioning = true)

        val repartition = spark.createDataset(starkRDD)//.repartition(12500)

        val partitions_counter = repartition.groupBy('pid).count()

        partitions_counter.write.csv("stark_binary_traj_partitions_counter_" + output + "_" + geospark_repartition + "_" + geospark_sample_number)


        val partitionMBB = repartition.groupByKey(p => p.id).mapGroups({
          (id, it) => {
            SpatioTemporalIndex.rtreeblob_store_traj(it, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
          }
        })

        partitionMBB.write.option("compression", "snappy").mode("overwrite").parquet("stark_traj_partitionMBBDF_binary_" + output + "_parquet")
        repartition.write.option("compression", "snappy").mode("overwrite").parquet("stark_traj_repartition_binary_" + output + "_parquet")


        partitionMBB.repartition(1).mapPartitions(f => {
          val rtree3D: STRtreeObjID = new STRtreeObjID()

          rtree3D.setDatasetEnvelope(broadcastBoundary.value.jtsGeom().getEnvelopeInternal)

          while (f.hasNext) {
            val temp = f.next()
            val temp_mbbst = MbbSerialization.deserialize(temp.box.get)
            val envelope:GidEnvelope = new GidEnvelope(temp_mbbst.getMinX, temp_mbbst.getMaxX, temp_mbbst.getMinY, temp_mbbst.getMaxY)

            envelope.setGid(temp.id.get)

            rtree3D.insert(envelope, envelope)
          }

          rtree3D.build()

          val bos = new ByteArrayOutputStream()

          val out = new ObjectOutputStream(bos)
          out.writeObject(rtree3D)
          out.flush()
          val yourBytes = bos.toByteArray.clone()

          out.close()

          Iterator(Tree(Some(yourBytes)))
        }).write.option("compression", "snappy").mode("overwrite").parquet("partitions_stark_binary_" + output + "_parquet")



      /*
    case "bsp" =>

      val rdd = moving_spatial.rdd.map(row => {
        (STObject.apply(row.lineString.get), row)
      })

      val partitioner = new BSPartitioner(rdd, 1, 1, true, null, null)

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
    */
    }

    spark.close()
  }
}

/*
TODO

/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --properties-file "/root/implementation/TrajSPARQL/config/spatial_traj.properties" --class apps.SpatialTrajectoryPartition /root/implementation/TrajSPARQL/target/TrajSPARQL-jar-with-dependencies.jar

*/