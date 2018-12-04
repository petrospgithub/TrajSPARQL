package utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import di.thesis.indexing.types.PointST
import types.MbbST

object MbbSerialization {
  def serialize(traj:MbbST): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(traj)
    out.flush()
    bos.toByteArray.clone()
  }

  def deserialize(traj:Array[Byte]): MbbST = {
    val bis = new ByteArrayInputStream(traj)
    val in = new ObjectInputStream(bis)
    val retrievedObject = in.readObject.asInstanceOf[MbbST]
    retrievedObject
  }
}
