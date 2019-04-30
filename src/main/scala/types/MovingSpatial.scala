package types

import di.thesis.indexing.types.PointST

case class MovingSpatial(id: Option[Long], trajectory: Option[Array[PointST]], rowId:Option[Long], lineString: Option[String])

