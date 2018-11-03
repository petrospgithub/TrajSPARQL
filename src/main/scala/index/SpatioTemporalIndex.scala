package index

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.EnvelopeST
import org.apache.commons.lang3.SerializationUtils
import spatial.partition.MBBindexST
import types.{MbbST, Partitioner}

object SpatioTemporalIndex {
  def rtree(it: Iterator[Partitioner], datasetMBB: EnvelopeST, nodeCapacity:Int): MBBindexST = {
    try {
      val row = it.next()

      val rowId=row.rowId.get
      val pid=row.pid.get

      val rtree3D: STRtree3D=new STRtree3D(nodeCapacity)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope:EnvelopeST=row.mbbST
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
        val envelope:EnvelopeST=row.mbbST
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
      /*******************************/

      val bos = new ByteArrayOutputStream()
    //  ObjectOutput out = null;
    //  try {
       val out = new ObjectOutputStream(bos)
        out.writeObject(rtree3D)
        out.flush()
        val yourBytes = bos.toByteArray

       val temp=MbbST(pid,minX,maxX,minY,maxY,minT,maxT)

       MBBindexST(Some(pid), Some(temp), Some(yourBytes))

    //  } finally {
   //     try {
   //       bos.close();
    //    } catch (IOException ex) {
          // ignore close exception
  //  /    }
  //    }
//
      /*******************************/
      /*******************************/


      /*******************************/

    } catch {
      case _: NoSuchElementException => MBBindexST(None,None,None)
    }
  }
}

