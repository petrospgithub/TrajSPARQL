package utils

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Point}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

object TransformSRID {
  def toMeters(geom:Geometry):Geometry={

    val sourceCRS:CoordinateReferenceSystem = CRS.decode("EPSG:4326", true)
    val targetCRS:CoordinateReferenceSystem = CRS.decode("EPSG:3857", true)

    val transform:MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)
    JTS.transform(geom, transform)
  }

  def to4326(geom:Geometry):Geometry={

    val sourceCRS:CoordinateReferenceSystem = CRS.decode("EPSG:3857", true)
    val targetCRS:CoordinateReferenceSystem = CRS.decode("EPSG:4326", true)

    val transform:MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)
    JTS.transform(geom, transform)
  }

  def toMeters(lon:Double, lat:Double):Point={
    val sourceCRS:CoordinateReferenceSystem = CRS.decode("EPSG:4326",true)
    val targetCRS:CoordinateReferenceSystem = CRS.decode("EPSG:3857", true)

    val geometryFactory:GeometryFactory=new GeometryFactory()
    val sourceGeometry:Geometry= geometryFactory.createPoint(new Coordinate(lon, lat))
    val transform:MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)
    val targetGeometry:Point = JTS.transform(sourceGeometry, transform).asInstanceOf[Point]
    targetGeometry
  }

  val RADIUS = 6378137.0

  def y2lat(aY: Double): Double = Math.toDegrees(Math.atan(Math.exp(aY / RADIUS)) * 2 - Math.PI / 2)

  def x2lon(aX: Double): Double = Math.toDegrees(aX / RADIUS)

  /* These functions take their angle parameter in degrees and return a length in meters */

  def lat2y(aLat: Double): Double = Math.log(Math.tan(Math.PI / 4 + Math.toRadians(aLat) / 2)) * RADIUS

  def lon2x(aLong: Double): Double = Math.toRadians(aLong) * RADIUS

  def myMeters(lon:Double, lat:Double):(Double,Double)={
    val x=lon2x(lon)
    val y=lat2y(lat)

    (x,y)
  }

}
