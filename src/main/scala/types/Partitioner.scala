package types

case class Partitioner(id: Long, trajectory: Array[CPointST], rowId:Long, pid:Long) {

  lazy val mbbST:MbbST= {
    val min_t = trajectory.head.getTimestamp
    val max_t = trajectory.last.getTimestamp

    var i = 1

    var newMinX = trajectory.head.getLongitude
    var newMaxX = trajectory.head.getLongitude

    var newMinY = trajectory.head.getLatitude
    var newMaxY = trajectory.head.getLatitude

    while (i < trajectory.length) {
      newMinX = trajectory(i).getLongitude min newMinX
      newMaxX = trajectory(i).getLongitude max newMaxX
      newMinY = trajectory(i).getLatitude min newMinY
      newMaxY = trajectory(i).getLatitude max newMaxY
      i = i + 1
    }

    val ret=MbbST(id, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)
    ret.setGid(id)
    //println(ret)
    ret
  }

}
