import di.thesis.indexing.octree.OctreePartitioning
import di.thesis.indexing.types.{EnvelopeST, PointST}
import org.apache.spark.sql.{AnalysisException, Dataset, Encoders, SparkSession}
import spatiotemporal.STGrid
import types.{MovingObject, Partitioner, Segment, Trajectory}

object debugging {

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder
      .appName("TrajectoryOctree")//.master("local[*]")
      .getOrCreate()

    val prop=spark.sparkContext.getConf
    val output = prop.get("spark.output")

    val path = prop.get("spark.path")
    val fraction=prop.get("spark.fraction").toDouble
    val maxItemByNode=prop.get("spark.maxitembynode").toInt
    val maxLevel=prop.get("spark.maxlevel").toInt


    val fraction_dataset=prop.get("spark.fraction_dataset").toDouble

    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
    import spark.implicits._

    val broadcastrtree_nodeCapacity=spark.sparkContext.broadcast(rtree_nodeCapacity)

    val traj_dataset= try {
      spark.read.parquet(path).as[Trajectory].sample(true, fraction_dataset)
    } catch {
      case _:AnalysisException=> spark.read.parquet(path).as[Segment].sample(false, fraction_dataset)
    }

    traj_dataset.show()

    val mbbst:EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]])

    val broadcastBoundary=spark.sparkContext.broadcast(mbbst)

    val enveEncoder = Encoders.kryo(classOf[EnvelopeST])

    val mbbSamplingList=traj_dataset.sample(withReplacement = true, fraction).map(x=>{
      x.mbbST
    })(enveEncoder).collect()

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

    list.foreach(x=>{
      x.setGid(i)
      //println(x.wkt())
      i=i+1
    })

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
        case _: Trajectory =>
          Partitioner(Some(mo.id), Some(mo.trajectory), None, Some(mo.rowId), Some(partition_id))

        case _: Segment =>
          Partitioner(Some(mo.id), Some(mo.trajectory), Some(mo.asInstanceOf[Segment].traj_id), Some(mo.rowId), Some(partition_id))
      }


    })

    repartition.show()

    spark.close()
  }

}
