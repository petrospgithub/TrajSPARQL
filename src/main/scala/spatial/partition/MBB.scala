package spatial.partition

import types.MbbST

case class MBBindexSpatial(id:Long, ogc_geom:Array[Byte], tree:Array[Byte])

case class MBBindexST(id:Option[Long], box:Option[MbbST], tree:Option[Array[Byte]])

case class MBBindexSTBlob(id:Option[Long], box:Option[Array[Byte]], tree:Option[Array[Byte]])
