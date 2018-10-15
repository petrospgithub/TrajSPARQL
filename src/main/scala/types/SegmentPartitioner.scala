package types

case class SegmentPartitioner(override val id: Long, override val trajectory: Array[CPointST], traj_id:Long, override val rowId:Long, override val pid:Long) extends Partitioner2(id, trajectory, rowId, pid)