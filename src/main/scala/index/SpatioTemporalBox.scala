package index

import di.thesis.indexing.types.EnvelopeST

case class SpatioTemporalBox(id:Int, ogc_geom:EnvelopeST, tree:Array[Byte])
