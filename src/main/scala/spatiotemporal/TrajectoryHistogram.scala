package spatiotemporal

import di.thesis.indexing.types.{EnvelopeST, PointST}
import org.apache.spark.sql.{Dataset, Row}
import types._
import org.apache.spark.sql.functions._
import utils.ArraySearch

object TrajectoryHistogram {

  def getCellId(pointST: PointST, mbbST: EnvelopeST, xLength: Double, yLength:Double, numXCells: Int): Int = {
    /*
    /* TODO an xreiazetai to require! */
    require(pointST.longitude >= mbbST.minx && pointST.longitude <= mbbST.maxx ||
      pointST.latitude >= mbbST.miny && pointST.latitude <= mbbST.maxy ||
      pointST.timestamp >= mbbST.mint && pointST.timestamp <= mbbST.maxt
      , s"(${pointST.longitude},${pointST.latitude},${pointST.timestamp}) out of range!")
*/

    val x = math.floor(math.abs(pointST.getLongitude - mbbST.getMinX) / xLength).toInt
    val y = math.floor(math.abs(pointST.getLatitude - mbbST.getMinY) / yLength).toInt
    //val t = math.floor(math.abs(pointST.getTimestamp - mbbST.mint) / tLength).toInt

    val cellId = y * numXCells + x

	//println(cellId)

    cellId
  }
/*
  def getCellId(p: PointST, sideLength:Double, mbb:MbbST): Int = {

    val numXCells=math.ceil(2 / sideLength).toInt

    val x = math.floor(math.abs(p.getLongitude - mbb.minx) / sideLength).toInt
    val y = math.floor(math.abs(p.getLatitude - mbb.miny) / sideLength).toInt
	y * numXCells + x
  }
  */

  /*
  protected[spatiotemporal] def buildHistogram(traj_dataset:Dataset[MovingObject], withExtent: Boolean, numXCells: Int, numYCells: Int, tarr: Array[Trange], mbbST: MbbST, xLength: Double, yLength: Double, tLength: Long): (Array[(CellST, Int)], Array[Row]) = {
    val grid = STGrid.buildGrid(numXCells, numYCells, tarr, xLength, yLength, tLength, mbbST.minx, mbbST.miny, mbbST.mint)

    val broadXlength=traj_dataset.sparkSession.sparkContext.broadcast(xLength)
    val broadYlength=traj_dataset.sparkSession.sparkContext.broadcast(yLength)
    val broadTlength=traj_dataset.sparkSession.sparkContext.broadcast(tLength)
    val broadcastnumXCell=traj_dataset.sparkSession.sparkContext.broadcast(numXCells)
    val broadmbbST=traj_dataset.sparkSession.sparkContext.broadcast(mbbST)
    val broadGrid=traj_dataset.sparkSession.sparkContext.broadcast(grid)


    import traj_dataset.sparkSession.implicits._

    val mergeRange = RectRangeMerge.toColumn.name("merge_range")

    val sparkHist= if (withExtent) {
      traj_dataset.map { case(traj:MovingObject) =>

        val pointST:PointST=traj.getMean()

        val extent = MbbST(traj.id, pointST.getLongitude, pointST.getLongitude, pointST.getLatitude, pointST.getLatitude, pointST.getTimestamp, pointST.getTimestamp)
        val cellId = getCellId(pointST,
          broadmbbST.value, broadXlength.value,
          broadYlength.value, broadcastnumXCell.value)

        val spatial=broadGrid.value.filter(_._1.id==cellId)

        val target=ArraySearch.binarySearchIterative(spatial, pointST.getTimestamp)

        ((""+cellId+target).replace("-",""), 1, extent)
      }.groupBy("_1").agg(mergeRange.as("histObject")).collect()

    } else {
      traj_dataset.map { case(traj:MovingObject) =>

        val pointST:PointST=traj.getMean()
        val cellId = getCellId(pointST,
          broadmbbST.value, broadXlength.value,
          broadYlength.value, numXCells)

        val spatial=broadGrid.value.filter(_._1.id==cellId)

        val target=ArraySearch.binarySearchIterative(spatial, pointST.getTimestamp)

        ((""+cellId+target).replace("-",""), 1)
      }.groupBy("_1").agg(sum("_2").as("sum")).collect()
    }

    (grid,sparkHist)
  }
*/

}
