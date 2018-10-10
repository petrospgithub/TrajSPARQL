package spatiotemporal

import di.thesis.indexing.types.EnvelopeST
import org.apache.spark.sql.{Dataset, Encoders}
import types._

object STGrid {

  def getMinMax(traj_dataset: Dataset[MovingObject]): EnvelopeST = {

    //import traj_dataset.sparkSession.implicits._

    val enveEncoder = Encoders.kryo(classOf[EnvelopeST])

    traj_dataset.map(row => {
      row.mbbST
    })(enveEncoder).reduce { (oldMM, newMM) =>
      val newMinX = Math.min(oldMM.getMinX, newMM.getMinX)
      val newMaxX = Math.max(oldMM.getMaxX, newMM.getMaxX)

      val newMinY = Math.min(oldMM.getMinY, newMM.getMinY)
      val newMaxY = Math.max(oldMM.getMaxY, newMM.getMaxY)

      val newMinT =Math.min(oldMM.getMinT, newMM.getMinT)
      val newMaxT = Math.max(oldMM.getMaxT, newMM.getMaxT)

      val env=new EnvelopeST(newMinX, newMaxX, newMinY, newMaxY, newMinT, newMaxT)
      env.setGid(-1)

      env
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
