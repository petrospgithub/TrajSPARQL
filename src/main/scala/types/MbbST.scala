package types

import com.vividsolutions.jts.geom.{Envelope, Polygon}
import di.thesis.indexing.types.{EnvelopeST, PointST}
import distance.Distance
import org.geotools.geometry.jts.JTS
import stOperators.Intersects
import utils.TransformSRID

case class MbbST(var id:Long,minx:Double, maxx:Double, miny:Double, maxy:Double, mint:Long, maxt:Long) extends
  EnvelopeST(minx,maxx,miny,maxy,mint,maxt){

  this.setGid(id)

  def getCenter:PointST ={
    CPointST ((maxx+minx)/2, (maxy+miny)/2, (maxt+mint)/2)
  }

  override def intersects(p:PointST):Boolean={
    Intersects.apply(minx,maxx,miny, maxy, mint, maxt,
      p.getLongitude, p.getLongitude, p.getLatitude, p.getLatitude, p.getTimestamp, p.getTimestamp)
  }

  def intersects(mbb:MbbST):Boolean={
    Intersects.apply(minx,maxx,miny, maxy, mint, maxt,
      mbb.minx, mbb.maxx, mbb.miny, mbb.maxy, mbb.mint, mbb.maxt)
  }

  def extend(mbb:MbbST):MbbST={
    MbbST( -1,
      minx min mbb.minx,
      maxx max mbb.maxx,
      miny min mbb.miny,
      maxy max mbb.maxy,
      mint min mbb.mint,
      maxt max mbb.maxt
    )
  }

  def extend(mbb:EnvelopeST):MbbST={
    MbbST(-1,
      minx min mbb.getMinX,
      maxx max mbb.getMaxX,
      miny min mbb.getMinY,
      maxy max mbb.getMaxY,
      mint min mbb.getMinT,
      maxt max mbb.getMaxT
    )
  }

  lazy val tSideLength: Long = maxt-mint

  lazy val spatialLength: Double = Distance.getHaversineDistance(miny, minx, maxy, maxx)

  lazy val spatialArea: Double = {
    val env:Envelope=new Envelope()
    env.init(minx,maxx,miny,maxy)
    val polygon: Polygon = JTS.toGeometry(env)
    polygon.setSRID(4326)
    TransformSRID.toMeters(polygon).asInstanceOf[Polygon].getArea
  }

  lazy val spatialPerimeter: Double = {
    val env:Envelope=new Envelope()
    env.init(minx,maxx,miny,maxy)
    val polygon: Polygon = JTS.toGeometry(env)
    polygon.setSRID(4326)
    TransformSRID.toMeters(polygon).asInstanceOf[Polygon].getLength
  }

  lazy val JTSwkt:String ={
    val env:Envelope=new Envelope()
    env.init(minx,maxx,miny,maxy)
    val polygon: Polygon = JTS.toGeometry(env)
//    TransformSRID.toMeters(polygon).asInstanceOf[Polygon].toText
    polygon.toText
  }

}

