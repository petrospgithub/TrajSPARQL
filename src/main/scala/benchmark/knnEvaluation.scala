package benchmark

import java.io.{ByteArrayInputStream, ObjectInput, ObjectInputStream}
import java.util

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{EnvelopeST, Triplet}
import org.apache.spark.sql.{Encoders, SparkSession}
import types.Partitioner
import org.apache.spark.sql.functions.rand
import spatial.partition.{MBBindexST, MBBindexSTBlob}

import scala.collection.JavaConverters._

object knnEvaluation {

  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("TrajectoryOctree") //.master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val trajectoryDS = spark.read.parquet("octree_repartition_imis400_parquet").as[Partitioner]

   // val indexDS=spark.read.parquet("octree_traj_partitionMBBDF_binary_imis400_parquet").as[MBBindexSTBlob]

    val temp=spark.read.parquet("octree_traj_partitionMBBDF_binary_imis400_parquet").as[MBBindexSTBlob]

    val pid=temp.select('id).distinct().count().toInt

    val indexDS=temp.repartition(pid, $"id")

    //val part=spark.read.parquet("partitions_tree_imis400_parquet").as[Array[Byte]]

    val trajectory=trajectoryDS.orderBy(rand()).limit(1).collect() //todo check!

    trajectoryDS.unpersist()

    val traj=trajectory.head.trajectory.get

    val broadcastTraj=spark.sparkContext.broadcast(traj)


    //add knn parameters

    //flatmap sto partition

    val exec=spark.time ({

      val tEncoder = Encoders.kryo(classOf[Triplet])

      val arr=indexDS.flatMap(join=>{
        val b=join.tree

        val bis = new ByteArrayInputStream(b.get)
        val in = new ObjectInputStream(bis)
        val traj_tree = in.readObject.asInstanceOf[STRtree3D]

        val matches2:util.List[Triplet]=traj_tree.knn(broadcastTraj.value, 40000.1, "DTW", 1, 604800, 604800, "Euclidean", 50, 0, 0)

        matches2.iterator().asScala
      })(tEncoder).collect()

      spark.stop()

      println(arr.sortWith(_.getDistance <= _.getDistance).head)

      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")

      println("End: "+(System.currentTimeMillis()-start))
      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
    })

    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")

    println("Sparm time command: "+exec)
    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
  }
}
