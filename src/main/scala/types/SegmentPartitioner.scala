package types

case class SegmentPartitioner(id: Long, trajectory: Array[CPointST], traj_id:Long, rowId:Long, pid:Long) extends Partitioner(id, trajectory, rowId, pid)