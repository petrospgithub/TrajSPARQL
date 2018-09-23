package utils

import java.nio.{ByteBuffer, ByteOrder}

import com.esri.core.geometry.ogc.OGCGeometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import com.esri.core.geometry.Geometry.Type
import com.esri.core.geometry.{OperatorImportFromESRIShape, SpatialReference}

object ReadPostgresGeom {

  def hex2Geom( input:String ) : Geometry = {
    val aux: Array[Byte] = WKBReader.hexToBytes(input)
    val geom: Geometry = new WKBReader().read(aux)
    geom
  }

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

}
