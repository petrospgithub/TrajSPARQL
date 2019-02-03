package benchmark

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.Triplet
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.rand
import spatial.partition.{MBBindexST, MBBindexSTBlob}
import types.Partitioner

object readResources {
  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()


    val spark = SparkSession.builder
      .appName("TrajectoryOctree") //.master("local[*]")
      .getOrCreate()


    import spark.implicits._


    val exec=spark.time ({

      val trajectoryDS = spark.read.parquet("trajectories_benchmark").as[Partitioner]

      val indexDS = spark.read.parquet("index_benchmark").as[MBBindexSTBlob]

      val part = spark.read.parquet("partitions_tree_imis400_parquet").as[Array[Byte]]

      val trajectory = trajectoryDS.orderBy(rand()).limit(1).collect()

      val traj = trajectory.head.trajectory.get

      val randomMBR=indexDS.orderBy(rand()).select('box).limit(1).collect() //todo check!

      val box=utils.MbbSerialization.deserialize(randomMBR.head.getAs("box"))

      //val broadcastMBR=spark.sparkContext.broadcast(box)

      /*
    val broadcastTraj=spark.sparkContext.broadcast(traj)

    val distThreshold=spark.sparkContext.broadcast(args(0))
    val timeThreshold=spark.sparkContext.broadcast(args(1))
*/
      trajectoryDS.show()
      indexDS.show()
      part.show()

      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
      println("End: " + (System.currentTimeMillis() - start))
      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
    })

    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")

    println("Sparm time command: "+exec)
    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
  }
}
