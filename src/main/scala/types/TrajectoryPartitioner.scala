package types

case class TrajectoryPartitioner(override val id: Long, override val trajectory: Array[CPointST], override val rowId:Long, override val pid:Long) extends Partitioner2(id, trajectory, rowId, pid)