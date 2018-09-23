package spatiotemporal

import org.apache.spark.sql.Dataset
import types.{CellST, MbbST, MovingObject, Trange}

class TrajGridPartitioner(traj_dataset: Dataset[MovingObject], partitionsPerSpatialDimension: Int, temporalPartition:Int, withExtent: Boolean) {

  val numPartitions: Int = partitionsPerSpatialDimension*temporalPartition

  val mbbst: MbbST = STGrid.getMinMax(traj_dataset = traj_dataset)
  val xLength: Double = math.abs(mbbst.maxx - mbbst.minx) / partitionsPerSpatialDimension
  val yLength: Double = math.abs(mbbst.maxy - mbbst.miny) / partitionsPerSpatialDimension
  val tLength: Long = math.abs(mbbst.maxt - mbbst.mint) / temporalPartition+1

  val partitions: Array[CellST] = {

    var i=0
    val tarr=new Array[Trange](temporalPartition)

    while (i<temporalPartition) {
      tarr(i)=Trange(mbbst.mint+(i*tLength),mbbst.mint+((i+1)*tLength))
        i=i+1
    }

    val arr:Array[(CellST,Int)] = STGrid.buildGrid(partitionsPerSpatialDimension, partitionsPerSpatialDimension, tarr, xLength, yLength, tLength, mbbst.minx, mbbst.miny, mbbst.mint)


    arr.map(_._1)

  }


}
