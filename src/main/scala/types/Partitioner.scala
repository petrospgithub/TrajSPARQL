package types

import java.io.{ByteArrayInputStream, ObjectInputStream}

import di.thesis.indexing.types.{EnvelopeST, PointST}
import org.locationtech.jts.index.strtree.STRtree
import utils.TrajectorySerialization

case class Partitioner(id: Option[Long], trajectory: Option[Array[PointST]], traj_id:Option[Long], rowId:Option[Long], pid:Option[Long]) {
  lazy val mbbST: EnvelopeST = {
    val min_t: Long = trajectory.get(0).getTimestamp

    val length: Int = trajectory.get.length

    val max_t: Long = trajectory.get(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = trajectory.get(0).getLongitude
    var newMaxX: Double = trajectory.get(0).getLongitude

    var newMinY: Double = trajectory.get(0).getLatitude
    var newMaxY: Double = trajectory.get(0).getLatitude

    while ( {
      i < length
    }) {
      newMinX = Math.min(trajectory.get(i).getLongitude, newMinX)
      newMaxX = Math.max(trajectory.get(i).getLongitude, newMaxX)
      newMinY = Math.min(trajectory.get(i).getLatitude, newMinY)
      newMaxY = Math.max(trajectory.get(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = new EnvelopeST(newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)
    ret.setGid(id.get)
    ret
  }
}


case class PartitionerBlob(id: Option[Long], trajectory: Option[Array[Byte]], traj_id:Option[Long], rowId:Option[Long], pid:Option[Long]) {
  lazy val mbbST: EnvelopeST = {

    val retrievedObject = TrajectorySerialization.deserialize(trajectory.get)

    val min_t: Long = retrievedObject(0).getTimestamp

    val length: Int = retrievedObject.length

    val max_t: Long = retrievedObject(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = retrievedObject(0).getLongitude
    var newMaxX: Double = retrievedObject(0).getLongitude

    var newMinY: Double = retrievedObject(0).getLatitude
    var newMaxY: Double = retrievedObject(0).getLatitude

    while ( {
      i < length
    }) {
      newMinX = Math.min(retrievedObject(i).getLongitude, newMinX)
      newMaxX = Math.max(retrievedObject(i).getLongitude, newMaxX)
      newMinY = Math.min(retrievedObject(i).getLatitude, newMinY)
      newMaxY = Math.max(retrievedObject(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = new EnvelopeST(newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)
    ret.setGid(id.get)
    ret
  }
}


//todo trajectory as blob!
/*
case class PartitionerBlob(id: Option[Long], traj_blob: Option[Array[Byte]], traj_id:Option[Long], rowId:Option[Long], pid:Option[Long]) {
  lazy val mbbST: MbbST = {
//TODO

    val min_t: Long = trajectory(0).getTimestamp

    val length: Int = trajectory.length

    val max_t: Long = trajectory(length - 1).getTimestamp

    var i = 1

    var newMinX: Double = trajectory(0).getLongitude
    var newMaxX: Double = trajectory(0).getLongitude

    var newMinY: Double = trajectory(0).getLatitude
    var newMaxY: Double = trajectory(0).getLatitude

    while ( {
      i < length
    }) {
      newMinX = Math.min(trajectory(i).getLongitude, newMinX)
      newMaxX = Math.max(trajectory(i).getLongitude, newMaxX)
      newMinY = Math.min(trajectory(i).getLatitude, newMinY)
      newMaxY = Math.max(trajectory(i).getLatitude, newMaxY)
      i = i + 1
    }

    val ret = MbbST(rowId.get, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)

    ret
  }

}
*/