package binary

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import di.thesis.indexing.octree.OctreePartitioning
import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{EnvelopeST, PointST}
import index.SpatioTemporalIndex
import org.apache.spark.sql.{AnalysisException, Dataset, Encoders, SparkSession}
import spatiotemporal.STGrid
import types._
import utils.{TrajectorySerialization, TrajectoryToMBR}

object OcTreeAppBinary {
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

    val encoder = Encoders.tuple(Encoders.LONG, Encoders.kryo[Array[PointST]], Encoders.LONG)
    val traj_dataset=spark.read.parquet(path)as encoder

/*
    val traj_dataset= try {

      val encoder = Encoders.tuple(Encoders.LONG, Encoders.kryo[Array[PointST]], Encoders.LONG)
      spark.read.parquet(path)as encoder

    } catch {
      case _:AnalysisException=>
        val encoder = Encoders.tuple(Encoders.LONG, Encoders.kryo[Array[PointST]], Encoders.LONG, Encoders.LONG)
       // spark.read.parquet(path)as encoder
        spark.read.parquet(path)as encoder
    }
*/
    val mbbst:EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset.asInstanceOf[Dataset[MovingObject]])

    val broadcastBoundary=spark.sparkContext.broadcast(mbbst)

    val enveEncoder = Encoders.kryo(classOf[EnvelopeST])

    val mbbSamplingList=traj_dataset.map(x=>{
      TrajectoryToMBR.trajMBR(x._3, x._2)
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

    list.foreach(x=>{
      x.setGid(i)
      //println(x.wkt())
      i=i+1
    })
/*
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
          PartitionerBlob(Some(mo.id), Some(TrajectorySerialization.serialize(mo.trajectory.asInstanceOf[Array[PointST]])), None, Some(mo.rowId), Some(partition_id))

        case _: Segment =>
          PartitionerBlob(Some(mo.id),  Some(TrajectorySerialization.serialize(mo.trajectory.asInstanceOf[Array[PointST]])), Some(mo.asInstanceOf[Segment].traj_id), Some(mo.rowId), Some(partition_id))
      }

    })

    val partitions_counter = repartition.groupBy('pid).count()

    partitions_counter.write.csv("octree_partitions_counter_" + output+"_"+maxItemByNode+"_"+maxLevel+"_"+fraction)


    val partitionMBB=repartition.groupByKey(p=>p.pid).mapGroups({
      (id, it) => {
        SpatioTemporalIndex.rtreeblob(it, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
      }
    })

    partitionMBB.write.option("compression", "snappy").mode("overwrite").parquet("octree_partitionMBBDF_" + output + "_parquet")
    repartition.write.option("compression", "snappy").mode("overwrite").parquet("octree_repartition_" + output + "_parquet")


    partitionMBB.repartition(1).mapPartitions(f=>{
      val rtree3D: STRtree3D = new STRtree3D()

      rtree3D.setDatasetMBB(broadcastBoundary.value)

      while (f.hasNext) {
        val temp=f.next()
        val envelope: EnvelopeST = new EnvelopeST(temp.box.get.minx, temp.box.get.maxx, temp.box.get.miny, temp.box.get.maxy, temp.box.get.mint, temp.box.get.maxt)
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
*/


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



/*
/bin/spark-submit --properties-file "./config/traj_octree.properties" --class apps.OcTreeApp ./target/TrajSPARQL-jar-with-dependencies.jar

 */