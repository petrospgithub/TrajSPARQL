package types

import di.thesis.indexing.types.PointST

case class MovingSpatial(id: Long, trajectory: Array[PointST], lineString: String)

