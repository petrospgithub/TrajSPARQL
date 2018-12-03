package types

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, LineString}
import di.thesis.indexing.types.{EnvelopeST, PointST}
import distance.Distance
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait MovingObject {
  def id: Long
  def trajectory: Array[PointST]
  def rowId:Long

  def getMean(): PointST = {
    var sumx=0.0
    var sumy=0.0
    var sumt=0L

    var i=0

    while(i<trajectory.length) {
      sumx=sumx+trajectory(i).getLongitude
      sumy=sumy+trajectory(i).getLatitude
      sumt=sumt+trajectory(i).getTimestamp
      i=i+1
    }
    val pointst=new PointST()

    pointst.setLongitude(sumx/trajectory.length)
    pointst.setLatitude(sumy/trajectory.length)
    pointst.setTimestamp(sumt/trajectory.length)

    pointst
  }

  def getMedian(): PointST = {
    trajectory.length % 2 match {
      case 0 =>
        val point_a=trajectory(trajectory.length/2)
        val point_b=trajectory((trajectory.length/2) + 1)

        val pointst=new PointST()

        pointst.setLongitude((point_a.getLongitude+point_b.getLongitude)/2)
        pointst.setLatitude((point_a.getLatitude+point_b.getLatitude)/2)
        pointst.setTimestamp((point_a.getTimestamp+point_b.getTimestamp)/2)

        pointst

      case 1 => {
        trajectory((trajectory.length/2) + 1)
      }
    }
  }
  /*
    lazy val mbbST:MbbST= {
      val min_t = trajectory.head.getTimestamp
      val max_t = trajectory.last.getTimestamp

      var i = 1

      var newMinX = trajectory.head.getLongitude
      var newMaxX = trajectory.head.getLongitude

      var newMinY = trajectory.head.getLatitude
      var newMaxY = trajectory.head.getLatitude

      while (i < trajectory.length) {
        newMinX = trajectory(i).getLongitude min newMinX
        newMaxX = trajectory(i).getLongitude max newMaxX
        newMinY = trajectory(i).getLatitude min newMinY
        newMaxY = trajectory(i).getLatitude max newMaxY
        i = i + 1
      }

      val ret=MbbST(id, newMinX, newMaxX, newMinY, newMaxY, min_t, max_t)
      ret.setGid(id)
      //println(ret)
      ret
    }
  */
  lazy val mbbST:EnvelopeST= {
    val min_t = trajectory.head.getTimestamp
    val max_t = trajectory.last.getTimestamp

    var i = 1

    var newMinX = trajectory.head.getLongitude
    var newMaxX = trajectory.head.getLongitude

    var newMinY = trajectory.head.getLatitude
    var newMaxY = trajectory.head.getLatitude

    while (i < trajectory.length) {
      newMinX = trajectory(i).getLongitude min newMinX
      newMaxX = trajectory(i).getLongitude max newMaxX
      newMinY = trajectory(i).getLatitude min newMinY
      newMaxY = trajectory(i).getLatitude max newMaxY
      i = i + 1
    }

    val ret=new EnvelopeST(newMinX, newMaxX, newMinY, newMaxY, min_t, max_t);
    ret.setGid(id)
    //println(ret)
    ret
  }

  lazy val linestring:LineString={

    var i=0
    val coord:ArrayBuffer[Coordinate]=new ArrayBuffer[Coordinate]()

    while(i<trajectory.length){
      coord.append(new Coordinate(trajectory(i).getLongitude, trajectory(i).getLatitude))
      i=i+1
    }

    new GeometryFactory().createLineString(coord.toArray)
  }

  lazy val sampling:Double={

    var i=1
    //val coord:ArrayBuffer[Coordinate]=new ArrayBuffer[Coordinate]()
    var sum=0L
    while(i<trajectory.length){
      sum=sum+(trajectory(i).getTimestamp-trajectory(i-1).getTimestamp)
      i=i+1
    }
    sum.toDouble/trajectory.length.toDouble
  }

  lazy val length:Double={
    var i=1
    //val coord:ArrayBuffer[Coordinate]=new ArrayBuffer[Coordinate]()
    var sum=0.0
    while(i<trajectory.length){
      sum=sum+Distance.getEuclideanDistance(
        trajectory(i).getLatitude, trajectory(i).getLongitude,
        trajectory(i-1).getLatitude, trajectory(i-1).getLongitude
      )
      i=i+1
    }
    sum
  }

  lazy val duration:Long={
    trajectory.last.getTimestamp-trajectory.head.getTimestamp
  }

  lazy val avg_speed:Double={
    (Distance.getEuclideanDistance(
      trajectory.head.getLatitude,trajectory.head.getLongitude,
      trajectory.last.getLatitude,trajectory.last.getLongitude)
      / (trajectory.last.getTimestamp - trajectory.head.getTimestamp).toDouble) //*1.943844492

  }

  lazy val genericTrajectory:mutable.WrappedArray[Row]={

    val arr=new Array[Row](trajectory.length)

    var i=0



    while (i<trajectory.length) {
      arr(i)=Row(trajectory(i).getLongitude, trajectory(i).getLatitude, trajectory(i).getTimestamp)
      i=i+1
    }
    arr
  }

}

  /*
trait MovingObject(id: Long, trajectory: Array[CPointST], rowId:Long) {

}

*/