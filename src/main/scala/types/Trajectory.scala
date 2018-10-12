package types

case class Trajectory(override val id: Long, override val trajectory: Array[CPointST], override val rowId:Long)
  extends MovingObject