package utils

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

object TransformSRID {
  def toMeters(geom:Geometry):Geometry={

    val sourceCRS:CoordinateReferenceSystem = CRS.decode("EPSG:4326")
    val targetCRS:CoordinateReferenceSystem = CRS.decode("EPSG:3857")

    val transform:MathTransform = CRS.findMathTransform(sourceCRS, targetCRS);
    JTS.transform(geom, transform)
  }

  def toMeters(lon:Double, lat:Double):Point={
    val sourceCRS:CoordinateReferenceSystem = CRS.decode("EPSG:4326")
    val targetCRS:CoordinateReferenceSystem = CRS.decode("EPSG:3857")

    val geometryFactory:GeometryFactory=new GeometryFactory()
    val sourceGeometry:Geometry= geometryFactory.createPoint(new Coordinate(lat, lon))
    val transform:MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)
    val targetGeometry:Point = JTS.transform(sourceGeometry, transform).asInstanceOf[Point]
    targetGeometry
  }
}
