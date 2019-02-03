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

    val trajectoryDS = spark.read.parquet("trajectories_benchmark").as[Partitioner]

    val indexDS=spark.read.parquet("index_benchmark").as[MBBindexSTBlob]

    val part=spark.read.parquet("partitions_tree_imis400_parquet").as[Array[Byte]]

    val trajectory=trajectoryDS.orderBy(rand()).limit(1).collect() //todo check!

    trajectoryDS.unpersist()

    val traj=trajectory.head.trajectory.get

    val broadcastTraj=spark.sparkContext.broadcast(traj)

    val distThreshold=spark.sparkContext.broadcast(args(0))
    val timeThreshold=spark.sparkContext.broadcast(args(1))

    //add knn parameters

    //flatmap sto partition

    val exec=spark.time ({

      val valid = part.flatMap(btree => {
        val bis = new ByteArrayInputStream(btree)
        val in = new ObjectInputStream(bis)
        val retrievedObject = in.readObject.asInstanceOf[STRtree3D]

        val list: util.List[Long] = retrievedObject.knn(broadcastTraj.value, distThreshold.value.toDouble, timeThreshold.value.toInt, timeThreshold.value.toInt).asInstanceOf[util.List[Long]]

        val result = new Array[Long](list.size())

        var i = 0

        while (i < list.size()) {

          result(i) = list.get(i)

          i += 1
        }


        result.toIterator
      })

      val tEncoder = Encoders.kryo(classOf[Triplet])

      val arr=valid.joinWith(indexDS, valid("value") === indexDS("id")).flatMap(join=>{
        val b=join._2.tree

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
