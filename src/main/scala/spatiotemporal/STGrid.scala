package spatiotemporal

import org.apache.spark.sql.Dataset
import types._

object STGrid {

  def getMinMax(traj_dataset: Dataset[MovingObject]): MbbST = {

    import traj_dataset.sparkSession.implicits._

    traj_dataset.map(row => {
      row.mbbST
    }).reduce { (oldMM, newMM) =>
      val newMinX = oldMM.minx min newMM.minx
      val newMaxX = oldMM.maxx max newMM.maxx

      val newMinY = oldMM.miny min newMM.miny
      val newMaxY = oldMM.maxy max newMM.maxy

      val newMinT = oldMM.mint min newMM.mint
      val newMaxT = oldMM.maxt max newMM.maxt

      MbbST(-1,newMinX, newMaxX, newMinY, newMaxY, newMinT, newMaxT)
    }
  }
  def getCellBounds(id: Int, xCells: Int, tarr: Array[Trange],xLength: Double, yLength: Double, tLength: Long, minX: Double, minY: Double, minT: Long): Array[MbbST] = {

    val dy = id / xCells
    val dx = id % xCells

    val llx = dx * xLength + minX
    val lly = dy * yLength + minY

    val urx = llx + xLength
    val ury = lly + yLength

    val arr=new Array[MbbST](tarr.length)

    var i=0

    while (i<arr.length) {
      arr(i)=MbbST(i,llx, urx, lly,ury, tarr(i).min,tarr(i).max)
      i=i+1
    }

    arr
  }

  def buildGrid(numXCells: Int, numYCells: Int, tarr: Array[Trange], xLength: Double, yLength: Double, tLength: Long, minX: Double, minY: Double, minT: Long): Array[(CellST, Int)] =  {

    val cellarr:Array[(CellST,Int)]=new Array[(CellST,Int)](numXCells*numYCells*tarr.length)

    val i=numXCells*numYCells
    var j=0
    var z=0
    while (j<i) {
      val cellBounds:Array[MbbST] = getCellBounds(j, numXCells, tarr, xLength, yLength, tLength, minX, minY, minT)

      var k=0
      while (k<cellBounds.length) {
        cellarr(z)=(CellST(j, cellBounds(k)), 0)
        k=k+1

        z=z+1
      }
      j=j+1
    }

    cellarr
  }
}
