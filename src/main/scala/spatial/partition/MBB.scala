package spatial.partition

import types.MbbST

case class MBBindexSpatial(id:Long, ogc_geom:Array[Byte], tree:Array[Byte])

case class MBBindexST(id:Long, box:MbbST, tree:Array[Byte])
