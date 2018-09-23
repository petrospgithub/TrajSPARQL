package spatiotemporal

import org.apache.spark.sql.Dataset
import types.{CellST, MbbST, MovingObject, Trange}

class TrajBSPartitioner (traj_dataset: Dataset[MovingObject], sideLength:Double,maxCostPerPartition:Double, withExtent:Boolean, t_sideLength:Long){

  val mbbst: MbbST = STGrid.getMinMax(traj_dataset = traj_dataset)

  val numXCells: Int = {
    val xCells = Math.ceil(math.abs(mbbst.maxx - mbbst.minx) / sideLength).toInt
    val maxX = mbbst.minx + xCells*sideLength
    Math.ceil(math.abs(maxX - mbbst.minx) / sideLength).toInt
  }

  val numYCells: Int = {
    val yCells = Math.ceil(math.abs(mbbst.maxy - mbbst.miny) / sideLength).toInt
    val maxY = mbbst.miny + yCells*sideLength
    Math.ceil(math.abs(maxY - mbbst.miny) / sideLength).toInt
  }

  val temporalPartition: Int = {
    Math.ceil((mbbst.maxt - mbbst.mint) / t_sideLength.toDouble).toInt
  }

  val tRange: Array[Trange]= {
    var i = 0

    val tarr = new Array[Trange](temporalPartition)

    while (i < temporalPartition) {
      tarr(i) = Trange(mbbst.mint + (i * t_sideLength), mbbst.mint + ((i + 1) * t_sideLength))
      i = i + 1
    }
    tarr
  }

  val partitions: Array[CellST] = {


    val arr:Array[(CellST,Int)] = STGrid.buildGrid(numXCells, numYCells, tRange, sideLength, sideLength, t_sideLength, mbbst.minx, mbbst.miny, mbbst.mint)


    arr.map(_._1)

  }

}
