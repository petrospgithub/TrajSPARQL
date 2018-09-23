package apps

import di.thesis.indexing.types.PointST
import index.SpatioTemporalIndex
import org.apache.spark.sql.SparkSession
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

    val host = prop.get("spark.host")
    val user = prop.get("spark.user")
    val pass = prop.get("spark.pass")
    val dbtable = prop.get("spark.dbtable")
    val output = prop.get("spark.output")
    val file_output = prop.get("spark.filetype")

    val partitionsPerDimension = prop.get("spark.partitionsspatialperdimension").toInt

    val temporalPartition = prop.get("spark.temporalpartition").toInt

    val traj_split = prop.get("spark.traj_split")
    val accept_traj = prop.get("spark.accept_traj")

    val path = prop.get("spark.path")
    val rtree_nodeCapacity=prop.get("spark.localindex_nodecapacity").toInt
/* */

    import spark.implicits._

    val broadcastrtree_nodeCapacity=spark.sparkContext.broadcast(rtree_nodeCapacity)
/*
    val jdbcDF = args(0) match {
      case "postgres" => spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", host)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", pass)
        .load()

      case "csv" =>
        val file: String = path
        spark.read.format("csv").option("header", "true").load(file)
    }
*/
    //jdbcDF.show()

    val traj_dataset=spark.read.parquet(path).as[MovingObject]

    /*TODO to with extend den fainetai xrhsimo ara mallon paei gia false monima*/

    val partitioner = new TrajGridPartitioner(traj_dataset = traj_dataset, partitionsPerSpatialDimension = partitionsPerDimension, temporalPartition = temporalPartition, false)

    val broadXlength = spark.sparkContext.broadcast(partitioner.xLength)
    val broadYlength = spark.sparkContext.broadcast(partitioner.yLength)
    val broadTlength = spark.sparkContext.broadcast(partitioner.tLength)
    val broadcastnumXCell = spark.sparkContext.broadcast(partitionsPerDimension)
    val broadmbbST = spark.sparkContext.broadcast(partitioner.mbbst)
    val broadGrid = spark.sparkContext.broadcast(partitioner.partitions)
    val broadcastBoundary=spark.sparkContext.broadcast(partitioner.mbbst)
    //val broadnumXcells = spark.sparkContext.broadcast(partitioner)

    /*
        partitioner.partitions.foreach(f=>{
          println(f.range)
        })
    */

    //TODO mhpws allaksoume kai dialegei to box pou pernaei apo perissotera

    val repartition = traj_dataset.map(mo => {
      val pointST: PointST = mo.getMean()

      //val extent = MbbST(pointST.getLongitude, pointST.getLongitude, pointST.getLatitude, pointST.getLatitude, pointST.getTimestamp, pointST.getTimestamp)
      val cellId = getCellId(pointST,
        broadmbbST.value, broadXlength.value,
        broadYlength.value, broadcastnumXCell.value)

      val spatial = broadGrid.value.filter(_.id == cellId)

      val target = ArraySearch.binarySearchIterative(spatial, pointST.getTimestamp)
//      Partitioner(("" + cellId + target).replace("-", "").toInt, mo.id, mo.trajectory)
      Partitioner(mo.id, mo.trajectory, mo.rowId, ("" + cellId + target).replace("-", "").toInt)
  
  })

    //repartition.foreach(f=>println(f+" "+f.trajectory))

    val partitions = repartition.select($"pid").distinct().count()

    val traj_repart = repartition.repartition(partitions.toInt, $"pid").as[Partitioner]//.drop('pid).as[MovingObject]

    val mbbrdd=traj_repart.rdd.mapPartitions(it=>{
      SpatioTemporalIndex.rtree(it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
    }, preservesPartitioning = true)

    val partitionMBBDF = spark.createDataset(mbbrdd)

    //val temp=partitionMBBDF.collect()

    partitionMBBDF.write.option("compression", "snappy").mode("overwrite").parquet("bsp_partitionMBBDF_" + output + "_parquet")
    repartition.write.option("compression", "snappy").mode("overwrite").parquet("bsp_repartition_" + output + "_parquet")

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
