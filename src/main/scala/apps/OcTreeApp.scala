package apps

import di.thesis.indexing.octree.OctreePartitioning
import di.thesis.indexing.types.{EnvelopeST, PointST}
import index.SpatioTemporalIndex
import org.apache.spark.sql.{AnalysisException, Dataset, Encoders, SparkSession}
import spatiotemporal.STGrid
import types._

object OcTreeApp {
  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder
      .appName("TrajectoryOctree")//.master("local[*]")
      .getOrCreate()
/*
        val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/traj_octree.properties")

        val prop: Properties = new Properties()
        prop.load(input)

        val output = prop.get("spark.output")

        val path = prop.getProperty("spark.path")
        val fraction=prop.getProperty("spark.fraction").toDouble
        val maxItemByNode=prop.getProperty("spark.maxitembynode").toInt
        val maxLevel=prop.getProperty("spark.maxlevel").toInt

        val rtree_nodeCapacity=prop.getProperty("spark.localindex_nodecapacity").toInt
*/
    val prop=spark.sparkContext.getConf
    val output = prop.get("spark.output")

    val path = prop.get("spark.path")
    val fraction=prop.get("spark.fraction").toDouble
    val maxItemByNode=prop.get("spark.maxitembynode").toInt
    val maxLevel=prop.get("spark.maxlevel").toInt

    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
    import spark.implicits._

    val broadcastrtree_nodeCapacity=spark.sparkContext.broadcast(rtree_nodeCapacity)

    val traj_dataset= try {
      spark.read.parquet(path).as[Trajectory]
    } catch {
      case _:AnalysisException=> spark.read.parquet(path).as[Segment]
    }

    val mbbst:EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]])

    val broadcastBoundary=spark.sparkContext.broadcast(mbbst)

    val enveEncoder = Encoders.bean(classOf[EnvelopeST])

    val mbbSamplingList=traj_dataset.map(x=>{
      x.mbbST
    })(enveEncoder).sample(withReplacement = true, fraction).collect() //TODO check fraction

    import scala.collection.JavaConverters._

    val octree=new OctreePartitioning(mbbSamplingList.toList.asJava, mbbst, maxItemByNode, maxLevel)

    val list=octree.getLeadfNodeList.asScala

    var i=0
    /*
        list.foreach(x=>{
          x.setGid(i)
          i=i+1
          println(
            "rect_prism(np.array(["+x.getMinX +","+x.getMaxX+"]),"+
              "np.array(["+ x.getMinT +","+ x.getMaxT +"])," +
            "np.array(["+x.getMinY+","+x.getMaxX+"])"+
              ")"
          )
        })
        */
/*
    list.foreach(x=>{
      x.setGid(i)
      //println(x.wkt())
      i=i+1
    })
*/
    val broadcastLeafs=spark.sparkContext.broadcast(list)

    val repartition = traj_dataset.map(mo => {
      val pointST: PointST = mo.getMean()

      var done = false
      val leafs = broadcastLeafs.value
      val length = leafs.length
      var i=0
      var partition_id = -1L

      while (i < length && !done) {
        if (leafs(i).contains(pointST)) {
          done = true
          partition_id=leafs(i).getGid//.toString
        }
        i=i+1
      }

      mo match {
        case _: MovingObject =>
          TrajectoryPartitioner(mo.id, mo.trajectory, mo.rowId, partition_id)

        case _: Segment =>
          SegmentPartitioner(mo.id, mo.trajectory, mo.asInstanceOf[Segment].traj_id, mo.rowId, partition_id)
      }

    })

    val partitions_counter = repartition.groupBy('pid).count()
    val distinct_partitions=partitions_counter.select('pid).distinct().count()
    
    partitions_counter.write.csv("octree_partitions_counter_" + output+"_"+maxItemByNode+"_"+maxLevel+"_"+fraction)

    val traj_repart = repartition.repartition(distinct_partitions.toInt, $"pid").as[TrajectoryPartitioner]//.persist(StorageLevel.MEMORY_AND_DISK_SER)


    val partitionMBBDF = traj_repart.mapPartitions(it => {
      SpatioTemporalIndex.rtree(it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
    })

    repartition.write.option("compression", "snappy").mode("overwrite").parquet("octree_repartition_" + output + "_parquet")
    partitionMBBDF.write.option("compression", "snappy").mode("overwrite").parquet("octree_partitionMBBDF_" + output + "_parquet")
  

    spark.close()
    /*
        temp.foreach(x => {
          if (x != null) {
            val bis: ByteArrayInputStream = new ByteArrayInputStream(x.ogc_geom)

            val in: ObjectInput = new ObjectInputStream(bis)
            //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            val env: EnvelopeST = in.readObject.asInstanceOf[EnvelopeST]
            //println(env)
            println(env.wkt())
            //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

          }
        })
    */
  }
}


