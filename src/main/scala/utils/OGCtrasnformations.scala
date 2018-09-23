package utils

import java.nio.{ByteBuffer, ByteOrder}

import com.esri.core.geometry.Geometry.Type
import com.esri.core.geometry.{GeometryEngine, OperatorImportFromESRIShape, SpatialReference}
import com.esri.core.geometry.ogc.OGCGeometry
import com.vividsolutions.jts.geom.Geometry

object OGCtrasnformations {
  def dserialize(input:Array[Byte]):OGCGeometry = {
    val bb:ByteBuffer = ByteBuffer.wrap(input)

    val byteArr: Array[Byte] = input
    val SIZE_WKID = 4
    val SIZE_TYPE = 1
    val offset = SIZE_WKID + SIZE_TYPE
    val shapeBuffer = ByteBuffer.wrap(byteArr, offset, byteArr.length - offset).slice().order(ByteOrder.LITTLE_ENDIAN)
    val wkid=bb.getInt(0)
    val esriGeom = OperatorImportFromESRIShape.local().execute(0, Type.Unknown, shapeBuffer)

    val spatialReference = SpatialReference.create(wkid)

    OGCGeometry.createFromEsriGeometry(esriGeom, spatialReference)
  }

  def seriliaze2ogcgeom( geom:Geometry ) : Array[Byte] = {

    //val wkid = geom.getSRID
    val g0: OGCGeometry = OGCGeometry.fromText(geom.toString)

    val SIZE_WKID = 4
    val SIZE_TYPE = 1

    // first get shape buffer for geometry
    val shape: Array[Byte] = GeometryEngine.geometryToEsriShape(g0.getEsriGeometry)

    val shapeWithData: Array[Byte] = new Array[Byte](shape.length + SIZE_WKID + SIZE_TYPE)

    System.arraycopy(shape, 0, shapeWithData, SIZE_WKID + SIZE_TYPE, shape.length)

    //BytesWritable hiveGeometryBytes = new BytesWritable(shapeWithData)

    import java.nio.ByteBuffer
    val bb = ByteBuffer.allocate(4)
    bb.putInt(geom.getSRID)
    System.arraycopy(bb.array, 0, shapeWithData, 0, SIZE_WKID)

    val index: Int = g0.geometryType() match {
      case "Point" => 1
      case "LineString" => 2
      case "Polygon" => 3
      case "MultiPoint" => 4
      case "MultiLineString" => 5
      case "MultiPolygon" => 6
      case _ => 0
    }

    shapeWithData(SIZE_WKID) = index.asInstanceOf[Byte]

    //BytesWritable ret = new BytesWritable(shapeWithData);
    shapeWithData
  }
}
