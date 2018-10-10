package spatiotemporal

import di.thesis.indexing.types.EnvelopeST
import org.apache.spark.sql.Dataset
import types.{CellST, MbbST, MovingObject, Trange}

class TrajBSPartitioner (traj_dataset: Dataset[MovingObject], sideLength:Double,maxCostPerPartition:Double, withExtent:Boolean, t_sideLength:Long){

  val mbbst: EnvelopeST = STGrid.getMinMax(traj_dataset = traj_dataset)

  val numXCells: Int = {
    val xCells = Math.ceil(math.abs(mbbst.getMaxX - mbbst.getMinX) / sideLength).toInt
    val maxX = mbbst.getMinX + xCells*sideLength
    Math.ceil(math.abs(maxX - mbbst.getMinX) / sideLength).toInt
  }

  val numYCells: Int = {
    val yCells = Math.ceil(math.abs(mbbst.getMaxY - mbbst.getMinY) / sideLength).toInt
    val maxY = mbbst.getMinY + yCells*sideLength
    Math.ceil(math.abs(maxY - mbbst.getMinY) / sideLength).toInt
  }

  val temporalPartition: Int = {
    Math.ceil((mbbst.getMaxT - mbbst.getMinT) / t_sideLength.toDouble).toInt
  }

  val tRange: Array[Trange]= {
    var i = 0

    val tarr = new Array[Trange](temporalPartition)

    while (i < temporalPartition) {
      tarr(i) = Trange(mbbst.getMinT + (i * t_sideLength), mbbst.getMinT + ((i + 1) * t_sideLength))
      i = i + 1
    }
    tarr
  }

  val partitions: Array[CellST] = {


    val arr:Array[(CellST,Int)] = STGrid.buildGrid(numXCells, numYCells, tRange, sideLength, sideLength, t_sideLength, mbbst.getMinX, mbbst.getMinY, mbbst.getMinT)


    arr.map(_._1)

  }

}
