package utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import di.thesis.indexing.types.PointST

object TrajectorySerialization {

  def serialize(traj:Array[PointST]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(traj)
    out.flush()
    bos.toByteArray.clone()
  }

  def deserialize(traj:Array[Byte]): Array[PointST] = {
    val bis = new ByteArrayInputStream(traj)
    val in = new ObjectInputStream(bis)
    val retrievedObject = in.readObject.asInstanceOf[Array[PointST]]
    retrievedObject
  }
}
