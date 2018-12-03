package types

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString}
import di.thesis.indexing.types.{EnvelopeST, PointST}
import distance.Distance

import scala.collection.mutable.ArrayBuffer

case class Segment(override val id: Long, override val trajectory: Array[PointST], traj_id:Long, override val rowId:Long)
  extends MovingObject
