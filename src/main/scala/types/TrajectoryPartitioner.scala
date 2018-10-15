package types

case class TrajectoryPartitioner(id: Long, trajectory: Array[CPointST], rowId:Long, pid:Long) extends Partitioner(id, trajectory, rowId, pid)