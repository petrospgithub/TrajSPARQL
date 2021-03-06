package index

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.EnvelopeST
import spatial.partition.{MBBindexST, MBBindexSTBlob}
import types.{MbbST, MovingSpatial, Partitioner, PartitionerBlob}
import utils.{TrajectorySerialization, TrajectoryToMBR}

object SpatioTemporalIndex {
  def rtree(it: Iterator[Partitioner], datasetMBB: EnvelopeST, nodeCapacity: Int): MBBindexST = {
    try {
      val row = it.next()

      val rowId = row.rowId.get
      val pid = row.pid.get

      val rtree3D: STRtree3D = new STRtree3D(nodeCapacity)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope: EnvelopeST = row.mbbST

      envelope.setGid(rowId)

      rtree3D.insert(envelope)
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //var i=1

      while (it.hasNext) {
        val row = it.next()
        val envelope: EnvelopeST = row.mbbST
        envelope.setGid(row.rowId.get)

        rtree3D.insert(envelope)

        if (minX > envelope.getMinX) {
          minX = envelope.getMinX
        }
        if (maxX < envelope.getMaxX) {
          maxX = envelope.getMaxX
        }

        if (minY > envelope.getMinY) {
          minY = envelope.getMinY
        }
        if (maxY < envelope.getMaxY) {
          maxY = envelope.getMaxY
        }

        if (minT > envelope.getMinT) {
          minT = envelope.getMinT
        }
        if (maxT < envelope.getMaxT) {
          maxT = envelope.getMaxT
        }

        //i=i+1
      }

      rtree3D.build()
      /** *****************************/

      val bos = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(rtree3D)
      out.flush()
      val yourBytes = bos.toByteArray.clone()

      out.close()
      /** *****************************/

      val temp = MbbST(pid, minX, maxX, minY, maxY, minT, maxT)

      MBBindexST(Some(pid), Some(temp), Some(yourBytes))

    } catch {
      case _: NoSuchElementException => MBBindexST(None, None, None)
    }
  }

  def rtree_store_traj(it: Iterator[Partitioner], datasetMBB: EnvelopeST, nodeCapacity: Int): MBBindexST = {
    try {
      val row = it.next()

      val rowId = row.rowId.get
      val pid = row.pid.get

      val rtree3D: STRtree3D = new STRtree3D(nodeCapacity)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope: EnvelopeST = row.mbbST

      envelope.setGid(rowId)

      rtree3D.insert(envelope)
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //var i=1

      while (it.hasNext) {
        val row = it.next()
        val envelope: EnvelopeST = row.mbbST
        envelope.setGid(row.rowId.get)

        rtree3D.insert(envelope)

        if (minX > envelope.getMinX) {
          minX = envelope.getMinX
        }
        if (maxX < envelope.getMaxX) {
          maxX = envelope.getMaxX
        }

        if (minY > envelope.getMinY) {
          minY = envelope.getMinY
        }
        if (maxY < envelope.getMaxY) {
          maxY = envelope.getMaxY
        }

        if (minT > envelope.getMinT) {
          minT = envelope.getMinT
        }
        if (maxT < envelope.getMaxT) {
          maxT = envelope.getMaxT
        }

        //i=i+1
      }

      rtree3D.build()
      /** *****************************/

      val bos = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(rtree3D)
      out.flush()
      val yourBytes = bos.toByteArray.clone()

      out.close()
      /** *****************************/

      val temp = MbbST(pid, minX, maxX, minY, maxY, minT, maxT)

      MBBindexST(Some(pid), Some(temp), Some(yourBytes))

    } catch {
      case _: NoSuchElementException => MBBindexST(None, None, None)
    }
  }

  def rtreeblob(it: Iterator[PartitionerBlob], datasetMBB: EnvelopeST, nodeCapacity: Int): MBBindexSTBlob = {
    try {
      val row = it.next()

      val rowId = row.rowId.get
      val pid = row.pid.get

      val rtree3D: STRtree3D = new STRtree3D(nodeCapacity)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope: EnvelopeST = row.mbbST

      envelope.setGid(rowId)

      rtree3D.insert(envelope)
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //var i=1

      while (it.hasNext) {
        val row = it.next()
        val envelope: EnvelopeST = row.mbbST
        envelope.setGid(row.rowId.get)

        rtree3D.insert(envelope)

        if (minX > envelope.getMinX) {
          minX = envelope.getMinX
        }
        if (maxX < envelope.getMaxX) {
          maxX = envelope.getMaxX
        }

        if (minY > envelope.getMinY) {
          minY = envelope.getMinY
        }
        if (maxY < envelope.getMaxY) {
          maxY = envelope.getMaxY
        }

        if (minT > envelope.getMinT) {
          minT = envelope.getMinT
        }
        if (maxT < envelope.getMaxT) {
          maxT = envelope.getMaxT
        }

        //i=i+1
      }

      rtree3D.build()
      /** *****************************/

      val bos = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(rtree3D)
      out.flush()
      val yourBytes = bos.toByteArray.clone()

      out.close()
      /** *****************************/

      val temp = new EnvelopeST(minX, maxX, minY, maxY, minT, maxT)
      temp.setGid(pid)

      /** *****************************/

      val bos2 = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out2 = new ObjectOutputStream(bos2)
      out2.writeObject(temp)
      out2.flush()
      val yourBytes2 = bos2.toByteArray.clone()

      out2.close()
      /** *****************************/

      MBBindexSTBlob(Some(pid), Some(yourBytes2), Some(yourBytes))

    } catch {
      case _: NoSuchElementException => MBBindexSTBlob(None, None, None)
    }
  }

  def rtreeblob_store_traj(it: Iterator[PartitionerBlob], datasetMBB: EnvelopeST, nodeCapacity: Int): MBBindexSTBlob = {
    try {
      val row = it.next()

      val rowId = row.rowId.get
      val pid = row.pid.get

      val rtree3D: STRtree3D = new STRtree3D(nodeCapacity)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope: EnvelopeST = row.mbbST

      envelope.setGid(rowId)

      rtree3D.insert(envelope, TrajectorySerialization.deserialize(row.trajectory.get))
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //var i=1

      while (it.hasNext) {
        val row = it.next()
        val envelope: EnvelopeST = row.mbbST
        envelope.setGid(row.rowId.get)

        rtree3D.insert(envelope, TrajectorySerialization.deserialize(row.trajectory.get))

        if (minX > envelope.getMinX) {
          minX = envelope.getMinX
        }
        if (maxX < envelope.getMaxX) {
          maxX = envelope.getMaxX
        }

        if (minY > envelope.getMinY) {
          minY = envelope.getMinY
        }
        if (maxY < envelope.getMaxY) {
          maxY = envelope.getMaxY
        }

        if (minT > envelope.getMinT) {
          minT = envelope.getMinT
        }
        if (maxT < envelope.getMaxT) {
          maxT = envelope.getMaxT
        }

        //i=i+1
      }

      rtree3D.build()
      /** *****************************/

      val bos = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out = new ObjectOutputStream(bos)
      out.writeObject(rtree3D)
      out.flush()
      val yourBytes = bos.toByteArray.clone()

      out.close()
      /** *****************************/

      val temp = new EnvelopeST(minX, maxX, minY, maxY, minT, maxT)
      temp.setGid(pid)

      /** *****************************/

      val bos2 = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out2 = new ObjectOutputStream(bos2)
      out2.writeObject(temp)
      out2.flush()
      val yourBytes2 = bos2.toByteArray.clone()

      out2.close()
      /** *****************************/

      MBBindexSTBlob(Some(pid), Some(yourBytes2), Some(yourBytes))

    } catch {
      case _: NoSuchElementException => MBBindexSTBlob(None, None, None)
    }
  }


  def rtreeblob_mbb_spatial(pid:Long, it: Iterator[Partitioner], datasetMBB: EnvelopeST, nodeCapacity: Int): MBBindexSTBlob = {
    try {
      val row = it.next()

      val rowId = row.rowId.get
      val traj = row.trajectory.get

     // val rtree3D: STRtree3D = new STRtree3D(nodeCapacity)

      //rtree3D.setDatasetMBB(datasetMBB)

      val envelope: EnvelopeST = TrajectoryToMBR.trajMBR(rowId, traj)

      envelope.setGid(rowId)

      //rtree3D.insert(envelope, TrajectorySerialization.deserialize(row.trajectory.get))
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //var i=1

      while (it.hasNext) {
        val row = it.next()
        //val envelope: EnvelopeST = row.mbbST
        //envelope.setGid(row.rowId.get)

        //rtree3D.insert(envelope, TrajectorySerialization.deserialize(row.trajectory.get))

        val envelope: EnvelopeST = TrajectoryToMBR.trajMBR(rowId, traj)

        if (minX > envelope.getMinX) {
          minX = envelope.getMinX
        }
        if (maxX < envelope.getMaxX) {
          maxX = envelope.getMaxX
        }

        if (minY > envelope.getMinY) {
          minY = envelope.getMinY
        }
        if (maxY < envelope.getMaxY) {
          maxY = envelope.getMaxY
        }

        if (minT > envelope.getMinT) {
          minT = envelope.getMinT
        }
        if (maxT < envelope.getMaxT) {
          maxT = envelope.getMaxT
        }

        //i=i+1
      }

   //   rtree3D.build()
      /** *****************************/

  //    val bos = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
   //   val out = new ObjectOutputStream(bos)
     // out.writeObject(rtree3D)
    //  out.flush()
    //  val yourBytes = bos.toByteArray.clone()

     // out.close()
      /** *****************************/

      val temp = new EnvelopeST(minX, maxX, minY, maxY, minT, maxT)
      temp.setGid(pid)

      /** *****************************/

      val bos2 = new ByteArrayOutputStream()
      //  ObjectOutput out = null;
      //  try {
      val out2 = new ObjectOutputStream(bos2)
      out2.writeObject(temp)
      out2.flush()
      val yourBytes2 = bos2.toByteArray.clone()

      out2.close()
      /** *****************************/

      MBBindexSTBlob(Some(pid), Some(yourBytes2), None)

    } catch {
      case _: NoSuchElementException => MBBindexSTBlob(None, None, None)
    }
  }

}

