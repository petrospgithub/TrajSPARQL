package types

case class Partitioner2 (id:Long, trajectory:Array[CPointST], rowId:Long, pid:Long) {

  lazy val mbbST = {
    val min_t: Long = trajectory(0).getTimestamp

    val length: Int = trajectory.length

    val max_t: Long = trajectory(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = trajectory(0).getLongitude
    var newMaxX: Double = trajectory(0).getLongitude

    var newMinY: Double = trajectory(0).getLatitude
    var newMaxY: Double = trajectory(0).getLatitude

    while ( {
      i < trajectory.length
    }) {
      newMinX = Math.min(trajectory(i).getLongitude, newMinX)
      newMaxX = Math.max(trajectory(i).getLongitude, newMaxX)
      newMinY = Math.min(trajectory(i).getLatitude, newMinY)
      newMaxY = Math.max(trajectory(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = new MbbST(id, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)

    ret.setGid(id)
    //println(ret)
    ret
  }
}