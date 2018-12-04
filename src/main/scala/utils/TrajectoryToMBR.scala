package utils

import di.thesis.indexing.types.{EnvelopeST, PointST}

object TrajectoryToMBR {
  def trajMBR (id:Long, trajectory:Array[PointST]): EnvelopeST={
    val min_t: Long = trajectory(0).getTimestamp

    val length: Int = trajectory.length

    val max_t: Long = trajectory(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = trajectory(0).getLongitude
    var newMaxX: Double = trajectory(0).getLongitude

    var newMinY: Double = trajectory(0).getLatitude
    var newMaxY: Double = trajectory(0).getLatitude

    while ( {
      i < length
    }) {
      newMinX = Math.min(trajectory(i).getLongitude, newMinX)
      newMaxX = Math.max(trajectory(i).getLongitude, newMaxX)
      newMinY = Math.min(trajectory(i).getLatitude, newMinY)
      newMaxY = Math.max(trajectory(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = new EnvelopeST(newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)
    ret.setGid(id)
    ret
  }
}
