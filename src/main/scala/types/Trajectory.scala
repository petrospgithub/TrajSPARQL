package types

import di.thesis.indexing.types.PointST

case class Trajectory(override val id: Long, override val trajectory: Array[PointST], override val rowId:Long)
  extends MovingObject