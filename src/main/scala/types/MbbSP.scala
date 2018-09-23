package types

import com.vividsolutions.jts.geom.Envelope

case class MbbSP(minx:Double, maxx:Double, miny:Double, maxy:Double) extends Envelope (minx,maxx,miny,maxy)
