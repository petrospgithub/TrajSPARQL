package index

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.vividsolutions.jts.geom.{Envelope, Geometry, Polygon}
import com.vividsolutions.jts.io.WKTReader
import di.thesis.indexing.spatialextension.STRtreeObjID
import di.thesis.indexing.types.GidEnvelope
import org.apache.spark.sql.Row
import org.geotools.geometry.jts.JTS
import spatial.partition.MBBindexSpatial
import types.{MovingSpatial, PartitionerBlob}
import utils.{OGCtrasnformations, ReadPostgresGeom, TrajectorySerialization}

import scala.collection.mutable.ArrayBuffer

object SpatialIndex {
  def rtree(it: Array[Row], datasetMBB: Envelope, nodeCapacity:Int): Iterator[MBBindexSpatial] = {
    try {
      val row = it.head


      val index=row.getAs[Int](row.fieldIndex("partitionid"))

      val spatialIndex: STRtreeObjID = new STRtreeObjID(nodeCapacity)
      spatialIndex.setDatasetEnvelope(datasetMBB)

      val geom: Geometry = ReadPostgresGeom.hex2Geom(row.get(row.fieldIndex("geom")).toString)
      val srid=geom.getSRID
      val boundary = geom.getEnvelopeInternal

      val envelope = new GidEnvelope(geom.getEnvelopeInternal)

      //envelope.init(boundary)
      try {
        envelope.setGid(row.getAs[Int](row.fieldIndex("gid")).asInstanceOf[Int])
      }catch {
        case e:ClassCastException => envelope.setGid(row.getAs[String](row.fieldIndex("gid")).toInt)
      }

      spatialIndex.insert(envelope, geom)
      var minX = boundary.getMinX
      var minY = boundary.getMinY
      var maxX = boundary.getMaxX
      var maxY = boundary.getMaxY

      var i=0

      while (i<it.length) {
        val row = it(i)
        val geom: Geometry = ReadPostgresGeom.hex2Geom(row.get(row.fieldIndex("geom")).toString)
        val boundary = geom.getEnvelopeInternal

        val envelope = new GidEnvelope(boundary)

        try {
          envelope.setGid(row.getAs[Int](row.fieldIndex("gid")).asInstanceOf[Int])
        }catch {
          case e:ClassCastException => envelope.setGid(row.getAs[String](row.fieldIndex("gid")).toInt)
        }

        spatialIndex.insert(envelope, geom)

        if (minX > boundary.getMinX) {
          minX = boundary.getMinX
        }
        if (maxX < boundary.getMaxX) {
          maxX = boundary.getMaxX
        }

        if (minY > boundary.getMinY) {
          minY = boundary.getMinY
        }
        if (maxY < boundary.getMaxY) {
          maxY = boundary.getMaxY
        }
        i=i+1
      }

      val partitionMBB = new Envelope(minX, maxX, minY, maxY)
      val polygon: Polygon = JTS.toGeometry(partitionMBB)
      polygon.setSRID(srid)

     // println(polygon)

      val bos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)

      out.writeObject(spatialIndex.build())
      out.flush()

      val yourBytes: Array[Byte] = bos.toByteArray

      Iterator(MBBindexSpatial(index,OGCtrasnformations.seriliaze2ogcgeom(polygon), yourBytes))

    } catch {
      case _: NoSuchElementException => Iterator()
      //case _: NullPointerException => Iterator()
      //case _: ClassCastException => Iterator()
    }
  }

  def rtree(index:Int, it: Array[MovingSpatial], datasetMBB: Envelope, nodeCapacity:Int): Iterator[MBBindexSpatial] = {
   // try {
      val row = it.head
      val spatialIndex: STRtreeObjID = new STRtreeObjID(nodeCapacity)
      //spatialIndex.setDatasetEnvelope(datasetMBB)

      val geom: Geometry = new WKTReader().read(row.lineString.get)
      val srid=geom.getSRID
      val boundary = geom.getEnvelopeInternal
      val envelope = new GidEnvelope(boundary)

      //envelope.init(boundary)
      envelope.setGid(row.id.get)

      spatialIndex.insert(envelope, envelope)
      var minX = boundary.getMinX
      var minY = boundary.getMinY
      var maxX = boundary.getMaxX
      var maxY = boundary.getMaxY

      var i=0

      while (i<it.length) {
        val row = it(i)
        val geom: Geometry = new WKTReader().read(row.lineString.get)
        val boundary = geom.getEnvelopeInternal
        val envelope = new GidEnvelope(boundary)

        //envelope.init(boundary)
        envelope.setGid(row.id.get)

        spatialIndex.insert(envelope, envelope)

        if (minX > boundary.getMinX) {
          minX = boundary.getMinX
        }
        if (maxX < boundary.getMaxX) {
          maxX = boundary.getMaxX
        }

        if (minY > boundary.getMinY) {
          minY = boundary.getMinY
        }
        if (maxY < boundary.getMaxY) {
          maxY = boundary.getMaxY
        }
        i=i+1
      }

      val partitionMBB = new Envelope(minX, maxX, minY, maxY)
      val polygon: Polygon = JTS.toGeometry(partitionMBB)
      polygon.setSRID(srid)

      val bos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)

      out.writeObject(spatialIndex)
      out.flush()

      val yourBytes: Array[Byte] = bos.toByteArray

      Iterator(MBBindexSpatial(index,OGCtrasnformations.seriliaze2ogcgeom(polygon), yourBytes))

   // } catch {
    //  case _: NoSuchElementException => Iterator()
   //   case _: NullPointerException => Iterator()
    //  case _: ClassCastException => Iterator()
  //  }
  }

  def rtree(index:Int, it: Array[MovingSpatial]): Iterator[PartitionerBlob] = {

 //   PartitionerBlob(Some(mo.id), Some(TrajectorySerialization.serialize(mo.trajectory)), None, Some(mo.rowId), Some(partition_id))


    // try {
  //  val row = it.head
  //  val spatialIndex: STRtreeObjID = new STRtreeObjID(nodeCapacity)
    //spatialIndex.setDatasetEnvelope(datasetMBB)

  //  val geom: Geometry = new WKTReader().read(row.lineString)
  //  val srid=geom.getSRID
  //  val boundary = geom.getEnvelopeInternal
  //  val envelope = new GidEnvelope(boundary)

    //envelope.init(boundary)
 //   envelope.setGid(row.id)

  //  spatialIndex.insert(envelope, envelope)
  //  var minX = boundary.getMinX
  //  var minY = boundary.getMinY
  //  var maxX = boundary.getMaxX
  //  var maxY = boundary.getMaxY

    var i=0

    val buffer=new ArrayBuffer[PartitionerBlob]()

    while (i<it.length) {
      val row = it(i)

      PartitionerBlob(Some(row.id.get), Some(TrajectorySerialization.serialize(row.trajectory.get)), None, Some(row.rowId.get), Some(index))

      i=i+1
    }

    buffer.iterator
    // } catch {
    //  case _: NoSuchElementException => Iterator()
    //   case _: NullPointerException => Iterator()
    //  case _: ClassCastException => Iterator()
    //  }
  }

}
