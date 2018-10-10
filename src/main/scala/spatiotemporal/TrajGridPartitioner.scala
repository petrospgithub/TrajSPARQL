package spatiotemporal

import di.thesis.indexing.types.EnvelopeST
import org.apache.spark.sql.Dataset
import types.{CellST, MbbST, MovingObject, Trange}

class TrajGridPartitioner(traj_dataset: Dataset[MovingObject], partitionsPerSpatialDimension: Int, temporalPartition:Int, withExtent: Boolean) {

  val numPartitions: Int = partitionsPerSpatialDimension*temporalPartition

  val mbbst: EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset)
  val xLength: Double = math.abs(mbbst.getMaxX - mbbst.getMinX) / partitionsPerSpatialDimension
  val yLength: Double = math.abs(mbbst.getMaxY - mbbst.getMinY) / partitionsPerSpatialDimension
  val tLength: Long = math.abs(mbbst.getMaxT - mbbst.getMaxT) / temporalPartition+1

  val partitions: Array[CellST] = {

    var i=0
    val tarr=new Array[Trange](temporalPartition)

    while (i<temporalPartition) {
      tarr(i)=Trange(mbbst.getMinT+(i*tLength),mbbst.getMinT+((i+1)*tLength))
        i=i+1
    }

    val arr:Array[(CellST,Int)] = STGrid.buildGrid(partitionsPerSpatialDimension, partitionsPerSpatialDimension, tarr, xLength, yLength, tLength, mbbst.getMinX, mbbst.getMinY, mbbst.getMinT)


    arr.map(_._1)

  }


}
