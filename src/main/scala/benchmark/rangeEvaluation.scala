package benchmark

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util
import java.util.List

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{PointST, Triplet}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.rand
import spatial.partition.MBBindexST
import types.Partitioner
import scala.collection.JavaConverters._

object rangeEvaluation {
  def main(args: Array[String]): Unit = {

    val start=System.currentTimeMillis()

    val spark = SparkSession.builder
      .appName("TrajectoryOctree") //.master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val trajectoryDS = spark.read.parquet("trajectories_benchmark").as[Partitioner]

    val indexDS=spark.read.parquet("index_benchmark").as[MBBindexST]

    val part=spark.read.parquet("partitions_tree_imis400_parquet").as[Array[Byte]]

    val trajectory=indexDS.orderBy(rand()).limit(1).collect() //todo check!

    val box=trajectory.head.box.get

    val broadcastMBR=spark.sparkContext.broadcast(box)

    //val distThreshold=spark.sparkContext.broadcast(args(0))
    //val timeThreshold=spark.sparkContext.broadcast(args(1))

    //add knn parameters

    //flatmap sto partition

    val exec=spark.time ({


      val arr = indexDS.flatMap(join => {
        val b = join.tree

        val bis = new ByteArrayInputStream(b.get)
        val in = new ObjectInputStream(bis)
        val traj_tree = in.readObject.asInstanceOf[STRtree3D]

        val tree_results = traj_tree.queryIDTrajectory(broadcastMBR.value).asInstanceOf[util.List[(Long, Array[PointST])]]

        tree_results.iterator().asScala
      }).collect()


      spark.stop()

      //println(arr.sortWith(_.getDistance <= _.getDistance).head)

      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
      println("End: " + (System.currentTimeMillis() - start))
      println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
    })
    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")

    println("Sparm time command: "+exec)
    println("|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|")
  }
}

/*
/root/spark-2.3.0-bin-hadoop2.7/bin/spark-shell --conf spark.executor.memory=6g \
--conf spark.executor.cores=1 \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
--conf spark.executor.instances=15 \
--conf spark.driver.memory=6g \
--conf spark.driver.cores=6 \
--conf spark.executor.memoryOverhead=1g \
--conf spark.master=yarn \
--conf spark.submit.deployMode=client \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz


/root/spark-2.3.0-bin-hadoop2.7/bin/spark-submit --conf spark.executor.memory=6g \
--conf spark.executor.cores=1 \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs:///user/root/spark_eventLog \
--conf spark.executor.instances=15 \
--conf spark.driver.memory=6g \
--conf spark.driver.cores=6 \
--conf spark.executor.memoryOverhead=1g \
--conf spark.master=yarn \
--conf spark.submit.deployMode=client \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.yarn.archive=hdfs:///user/root/spark230_jars/spark230.tar.gz
 */