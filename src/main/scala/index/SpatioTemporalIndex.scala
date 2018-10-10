package index

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.JavaSerializer
import di.thesis.indexing.spatiotemporaljts.STRtree3D
import di.thesis.indexing.types.{EnvelopeST, PointST}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol
import spatial.partition.MBBindexST
import types.{MbbST, MovingObject, Partitioner}

object SpatioTemporalIndex {
  def rtree(it: Array[Partitioner], datasetMBB: EnvelopeST, nodeCapacity:Int): Iterator[MBBindexST] = {
    try {
      val row = it.head
      val rtree3D: STRtree3D=new STRtree3D(nodeCapacity:Int)

      rtree3D.setDatasetMBB(datasetMBB)

      val envelope:EnvelopeST=row.mbbST
      //val envelope = new EnvelopeST(boundary.getMinX, boundary.getMaxX, boundary.getMinY, boundary.getMaxY, boundary.getMinT, boundary.getMaxT)
      envelope.setGid(row.rowId)

      rtree3D.insert(envelope)
      var minX = envelope.getMinX
      var minY = envelope.getMinY

      var maxX = envelope.getMaxX
      var maxY = envelope.getMaxY

      var minT = envelope.getMinT
      var maxT = envelope.getMaxT

      //println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      //println(envelope)
      //println(minX+"\t"+maxX+"\t"+minY+"\t"+maxY+"\t"+minT+"\t"+maxT)

      var i=0

      while (i<it.length) {
        val row = it(i)
        val envelope:EnvelopeST=row.mbbST
        //val envelope = new EnvelopeST(boundary.minx, boundary.maxx, boundary.miny, boundary.maxy, boundary.mint, boundary.maxt)
        envelope.setGid(row.rowId)

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

        i=i+1
      }

      rtree3D.build()
      /*******************************/

      import org.nustaq.serialization.FSTConfiguration

      val conf = FSTConfiguration.createDefaultConfiguration

      val yourBytes: Array[Byte] = conf.asByteArray(rtree3D)

      val bos: ByteArrayOutputStream = new ByteArrayOutputStream

      /*******************************/
      /*******************************/

      val temp=MbbST(row.pid,minX,maxX,minY,maxY,minT,maxT)

      /*******************************/
      Iterator(MBBindexST(row.pid, temp, yourBytes))

    } catch {
      case _: NoSuchElementException => Iterator()
    }
  }
}

