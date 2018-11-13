package apps

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{EnvelopeST, PointST}
import index.SpatioTemporalIndex
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import spatiotemporal.TrajGridPartitioner
import spatiotemporal.TrajectoryHistogram.getCellId
import types._
import utils.ArraySearch

object TrajectorySTPartition {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
        .appName("TrajectoryGridPartition")
        .getOrCreate()
/*
    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/traj_grid.properties")

    val prop: Properties = new Properties()
    prop.load(input)

    val host = prop.getProperty("spark.host")
    val user = prop.getProperty("spark.user")
    val pass = prop.getProperty("spark.pass")
    val dbtable = prop.getProperty("spark.dbtable")
    val output = prop.getProperty("spark.output")
    val file_output = prop.getProperty("spark.filetype")

    val partitionsPerDimension = prop.getProperty("spark.partitionsSpatialPerDimension").toInt
    val temporalPartition = prop.getProperty("spark.temporalPartition").toInt

    val traj_split = prop.getProperty("spark.traj_split")
    val accept_traj = prop.getProperty("spark.accept_traj")

    val path = prop.getProperty("spark.path")
    val rtree_nodeCapacity=prop.getProperty("spark.localIndex_nodeCapacity").toInt
 */

    val prop=spark.sparkContext.getConf

    val output = prop.get("spark.output")

    val partitionsPerDimension = prop.get("spark.partitionsspatialperdimension").toInt

    val temporalPartition = prop.get("spark.temporalpartition").toInt

    val traj_split = prop.get("spark.traj_split")
    val accept_traj = prop.get("spark.accept_traj")

    val path = prop.get("spark.path")
    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
/* */

    import spark.implicits._

    val broadcastrtree_nodeCapacity=spark.sparkContext.broadcast(rtree_nodeCapacity)

    val traj_dataset= try {
      spark.read.parquet(path).as[Trajectory]
    } catch {
      case _:AnalysisException=> spark.read.parquet(path).as[Segment]
    }

    val partitioner = new TrajGridPartitioner(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]], partitionsPerSpatialDimension = partitionsPerDimension, temporalPartition = temporalPartition, false)

    val broadXlength = spark.sparkContext.broadcast(partitioner.xLength)
    val broadYlength = spark.sparkContext.broadcast(partitioner.yLength)
    val broadTlength = spark.sparkContext.broadcast(partitioner.tLength)
    val broadcastnumXCell = spark.sparkContext.broadcast(partitionsPerDimension)
    val broadmbbST = spark.sparkContext.broadcast(partitioner.mbbst)
    val broadGrid = spark.sparkContext.broadcast(partitioner.partitions)
    val broadcastBoundary=spark.sparkContext.broadcast(partitioner.mbbst)

    /*
        partitioner.partitions.foreach(f=>{
          println(f.range)
        })
    */

    val repartition = traj_dataset.map(mo => {
      val pointST: PointST = mo.getMean()

      val cellId = getCellId(pointST,
        broadmbbST.value, broadXlength.value,
        broadYlength.value, broadcastnumXCell.value)

      val spatial = broadGrid.value.filter(_.id == cellId)

      val target = ArraySearch.binarySearchIterative(spatial, pointST.getTimestamp)
//      types.Partitioner(("" + cellId + target).replace("-", "").toInt, mo.id, mo.trajectory)
      val pid = "" + cellId + target //.replace("-", "")


      mo match {
        case _: Trajectory =>
          Partitioner(Some(mo.id), Some(mo.trajectory), None, Some(mo.rowId), Some(pid.hashCode))

        case _: Segment =>
          Partitioner(Some(mo.id), Some(mo.trajectory), Some(mo.asInstanceOf[Segment].traj_id), Some(mo.rowId), Some(pid.hashCode))
      }
  })

    //repartition.foreach(f=>println(f+" "+f.trajectory))

    val partitions = repartition.select($"pid").distinct().count()


    val partitionMBB=repartition.groupByKey(p=>p.pid).mapGroups({
      (id, it) => {
        SpatioTemporalIndex.rtree(it, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
      }
    })

    val partitions_counter = repartition.groupBy('pid).count()

    //val distinct_partitions = repartition.select('pid).distinct().count()

    partitions_counter.write.csv("bsp_partitions_counter_" + output+"_"+partitionsPerDimension+"_"+temporalPartition)

    partitionMBB.write.option("compression", "snappy").mode("overwrite").parquet("st_partitionMBBDF_" + output + "_parquet")
    repartition.write.option("compression", "snappy").mode("overwrite").parquet("st_repartition_" + output + "_parquet")


    partitionMBB.repartition(1).mapPartitions(f=>{
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
    }).write.option("compression", "snappy").mode("overwrite").parquet("partitions_tree_" + output + "_parquet")


    //val traj_repart = repartition.repartition(partitions.toInt, $"pid")//.as[TrajectoryPartitioner]//.drop('pid).as[MovingObject]
/*
    val rdd = traj_repart.rdd.mapPartitions(it => {
      SpatioTemporalIndex.rtree(it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
    },preservesPartitioning = true)
    //val temp=partitionMBBDF.collect()

    spark.createDataset(rdd).write.option("compression", "snappy").mode("overwrite").parquet("bsp_partitionMBBDF_" + output + "_parquet")
    repartition.write.option("compression", "snappy").mode("overwrite").parquet("bsp_repartition_" + output + "_parquet")
*/
    spark.close()

    /*
	    temp.foreach(x=>{
      if (x!=null) {
        //val bis:ByteArrayInputStream = new ByteArrayInputStream(x.ogc_geom)

        //val in: ObjectInput = new ObjectInputStream(bis)
        //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        //val env:EnvelopeST=in.readObject.asInstanceOf[EnvelopeST]
        //println(env)
        println(x.box.wkt())
        //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

      }
    })
*/
    //println("Done!")

  }
}
