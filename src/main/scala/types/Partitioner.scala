package types

case class Partitioner(id: Option[Long], trajectory: Option[Array[CPointST]], traj_id:Option[Long], rowId:Option[Long], pid:Option[Long]) {
  lazy val mbbST: MbbST = {
    val min_t: Long = trajectory.get(0).getTimestamp

    val length: Int = trajectory.get.length

    val max_t: Long = trajectory.get(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = trajectory.get(0).getLongitude
    var newMaxX: Double = trajectory.get(0).getLongitude

    var newMinY: Double = trajectory.get(0).getLatitude
    var newMaxY: Double = trajectory.get(0).getLatitude

    while ( {
      i < length
    }) {
      newMinX = Math.min(trajectory.get(i).getLongitude, newMinX)
      newMaxX = Math.max(trajectory.get(i).getLongitude, newMaxX)
      newMinY = Math.min(trajectory.get(i).getLatitude, newMinY)
      newMaxY = Math.max(trajectory.get(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = MbbST(rowId.get, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)

    ret
  }
}