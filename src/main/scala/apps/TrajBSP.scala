package apps

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import di.thesis.indexing.types.{EnvelopeST, PointST}
import index.SpatioTemporalIndex
import org.apache.spark.sql._
import spatiotemporal.{STGrid, TrajBSPartitioner, TrajectoryHistogram}
import _root_.types._
import di.thesis.indexing.spatiotemporaljts.STRtree3D
import utils.ArraySearch

object TrajBSP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TrajectoryBSP")//.master("local[*]")
      .getOrCreate()
    /*
        val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/traj_bsp.properties")

        val prop: Properties = new Properties()
        prop.load(input)

        val output = prop.getProperty("spark.output")
        val file_output = prop.getProperty("spark.filetype")

        val sideLength = prop.getProperty("spark.sidelength").toDouble

        val t_sideLength = prop.getProperty("spark.t_sidelength").toInt

        val path = prop.getProperty("spark.path")
        val rtree_nodeCapacity=prop.getProperty("spark.localindex_nodecapacity").toInt
      */


    val prop = spark.sparkContext.getConf
    val output = prop.get("spark.output")
    // val file_output = prop.get("spark.filetype")

    val sideLength =prop.get("spark.sidelength").toDouble

    val path = prop.get("spark.path")

    val t_sideLength = prop.get("spark.t_sidelength").toInt
    val rtree_nodeCapacity = prop.get("spark.localindex_nodecapacity").toInt


    import spark.implicits._

    //spark.conf.set("spark.sql.orc.impl", "native")
    val broadcastrtree_nodeCapacity = spark.sparkContext.broadcast(rtree_nodeCapacity)

    val traj_dataset = try {
      spark.read.parquet(path).as[Trajectory]
    } catch {
      case _: AnalysisException => spark.read.parquet(path).as[Segment]
    }

    val mbbst: EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]])

    val broadcastBoundary = spark.sparkContext.broadcast(mbbst)

    val partitioner = new TrajBSPartitioner(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]], maxCostPerPartition = 1, sideLength = sideLength, withExtent = false, t_sideLength = t_sideLength)

    val broadsideLength = spark.sparkContext.broadcast(sideLength)
    val broadmbbST = spark.sparkContext.broadcast(partitioner.mbbst)
    val broadGrid = spark.sparkContext.broadcast(partitioner.partitions)
    val broadnumXcells = spark.sparkContext.broadcast(partitioner.numXCells)

    val repartition = traj_dataset.map(mo => {

      val pointST: PointST = mo.getMean()

      val cellId = TrajectoryHistogram.getCellId(pointST, broadmbbST.value, broadsideLength.value, broadsideLength.value, broadnumXcells.value)
      val spatial = broadGrid.value.filter(_.id == cellId)

      val target = ArraySearch.binarySearchIterative(spatial, pointST.getTimestamp)
      val pid = "" + cellId + target //.replace("-", "")


      mo match {
        case _: Trajectory =>
          Partitioner(Some(mo.id), Some(mo.trajectory), None, Some(mo.rowId), Some(pid.hashCode))

        case _: Segment =>
          Partitioner(Some(mo.id), Some(mo.trajectory), Some(mo.asInstanceOf[Segment].traj_id), Some(mo.rowId), Some(pid.hashCode))
      }
    })

    val partitions_counter = repartition.groupBy('pid).count()

    //val distinct_partitions = repartition.select('pid).distinct().count()

    partitions_counter.write.csv("bsp_partitions_counter_" + output+"_"+sideLength+"_"+t_sideLength)

    val partitionMBB=repartition.groupByKey(p=>p.pid).mapGroups({
      (id, it) => {
        SpatioTemporalIndex.rtree(it, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
      }
    })

    partitionMBB.write.option("compression", "snappy").mode("overwrite").parquet("bsp_partitionMBBDF_" + output + "_parquet")
    repartition.write.option("compression", "snappy").mode("overwrite").parquet("bsp_repartition_" + output + "_parquet")

    partitionMBB.coalesce(1).mapPartitions(f=>{
      val rtree3D: STRtree3D = new STRtree3D()

      rtree3D.setDatasetMBB(broadcastBoundary.value)

      while (f.hasNext) {
        val temp=f.next()
        val envelope: EnvelopeST = temp.box.get
        envelope.setGid(temp.id.get)
        rtree3D.insert(envelope)
      }

      rtree3D.build()

      val bos = new ByteArrayOutputStream()

      val out = new ObjectOutputStream(bos)
      out.writeObject(rtree3D)
      out.flush()
      val yourBytes = bos.toByteArray.clone()

      out.close()

      Iterator(Tree(Some(yourBytes)))
    }).write.option("compression", "snappy").mode("overwrite").parquet("partitions_tree" + output + "_parquet")

    spark.stop()

  }

}

