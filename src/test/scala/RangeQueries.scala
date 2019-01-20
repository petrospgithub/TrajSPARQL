import java.sql.{DriverManager, ResultSet}

import di.thesis.indexing.types.EnvelopeST
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import utils.MbbSerialization

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class RangeQueries  extends FunSuite {

  private val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
  private val stmt = con.createStatement
  private var rs:Option[ResultSet]=None
  private var env:Option[EnvelopeST]=None

  //TODO

  def rangeArrStruct_BF():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT rowId FROM imis400_temp" +
      " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeArrStruct_index():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400 as a INNER JOIN " +
      " (SELECT ST_IndexIntersects(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM index_imis400 ) as b" +
      "  ON (b.trajectory_id=a.rowId) " +
      " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }
/*
  def rangeArrStruct_index_pid():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400_pid as a INNER JOIN " +
      "(SELECT ST_IndexIntersects(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400) as b ON (b.trajectory_id=a.pid) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)

/*
    rs=Some(stmt.executeQuery(sql))

    while (rs.get.next()){
      val obj=rs.get.getObject(1)
      println(obj)
    }*/

    stmt.execute(sql)

    System.currentTimeMillis()-start
  }
*/
  def rangeBinary_BF():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }


    val start=System.currentTimeMillis()

    val sql = " SELECT rowId " +
      " FROM imis400_temp_binary " +
      " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeBinary_index():Long={
    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400_binary as a INNER JOIN " +
      " (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM index_imis400_binary ) as b " +
      " ON (b.trajectory_id=a.rowId) " +
      " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)

    stmt.execute(sql)
    System.currentTimeMillis()-start
  }
/*
  def rangeBinary_index_pid():Long={
    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400_binary_pid as a INNER JOIN " +
      "(SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binary) as b ON (b.trajectory_id=a.pid) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)

    stmt.execute(sql)
    System.currentTimeMillis()-start
  }
*/
  /*
  def rangeBinaryTraj():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }


    val start=System.currentTimeMillis()

    val sql=" SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM index_imis400_temp_binaryTraj ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)

    stmt.executeQuery(sql)

    System.currentTimeMillis()-start
  }
*/
  def rangeBinaryTraj_Part():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }


    val start=System.currentTimeMillis()

    val sql=(" SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) ".format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) FROM partition_index_imis400_binaryTraj) as a JOIN index_imis400_binaryTraj as b ON (a.trajectory_id=b.id) ").format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)
/*
    rs=Some(stmt.executeQuery(sql))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)
      println(obj)
      // env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }
*/

    stmt.execute(sql)

    System.currentTimeMillis()-start
  }


  test("Thesis range queries") {

    //stmt.execute(" ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar ")

    stmt.execute(" SET hive.auto.convert.join=true ")
    stmt.execute(" SET hive.enforce.bucketing=true ")
    stmt.execute(" SET hive.optimize.bucketmapjoin.sortedmerge = true ")
    stmt.execute(" SET hive.auto.convert.sortmerge.join=true ")
    stmt.execute(" SET hive.optimize.bucketmapjoin = true ")
    stmt.execute(" SET hive.auto.convert.join.noconditionaltask = true ")
    stmt.execute(" SET hive.auto.convert.join.noconditionaltask.size = 10000000 ")

    stmt.execute(" SET hive.vectorized.execution.enabled=true ")
    stmt.execute(" SET hive.exec.parallel=true ")
    stmt.execute(" SET mapred.compress.map.output=true ")
    stmt.execute(" SET mapred.output.compress=true ")
    stmt.execute(" SET hive.cbo.enable=true ")
    stmt.execute(" SET hive.stats.autogather=true ")
    stmt.execute(" SET hive.optimize.ppd=true ")
    stmt.execute(" SET hive.optimize.ppd.storage=true ")
    stmt.execute(" SET hive.vectorized.execution.reduce.enabled=true ")
    stmt.execute(" SET hive.stats.fetch.column.stats=true ")
    stmt.execute(" SET hive.tez.auto.reducer.parallelism=true ")

    stmt.execute(" set hive.server2.tez.initialize.default.sessions=true ")
    stmt.execute(" set hive.prewarm.enabled=true ")
    stmt.execute(" set hive.prewarm.numcontainers=15 ")
    stmt.execute(" set tez.am.container.reuse.enabled=true ")
    stmt.execute(" set hive.server2.enable.doAs=false ")


    stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructor AS 'di.thesis.hive.mbb.MbbSTUDF' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersects AS 'di.thesis.hive.stoperations.IndexIntersects3D' ")


    stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3DBinary AS 'di.thesis.hive.stoperations.ST_Intersects3DBinary' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructorBinary AS 'di.thesis.hive.mbb.MbbSTUDFBinary' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersectsBinary AS 'di.thesis.hive.stoperations.IndexIntersects3DBinary' ")

    stmt.execute(" CREATE TEMPORARY FUNCTION IndexIntersectsTraj AS 'di.thesis.hive.stoperations.IndexIntersectsTraj' ")

    val buffer_rangeArrStructBF=new ArrayBuffer[Long]
    val buffer_rangeArrStruct_INDEX=new ArrayBuffer[Long]
   // val buffer_rangeArrStruct_PID=new ArrayBuffer[Long]

    val buffer_rangeBinary_BF=new ArrayBuffer[Long]
    val buffer_rangeBinary_INDEX=new ArrayBuffer[Long]
   // val buffer_rangeBinary_PID=new ArrayBuffer[Long]


  //  val buffer_rangeBinaryTraj=new ArrayBuffer[Long]

    val buffer_rangeBinaryTraj_part=new ArrayBuffer[Long]

    var i=0

    while (i < 3) {

      buffer_rangeArrStructBF.append(rangeArrStruct_BF())

      i=i+1
    }

    i=0

    while (i < 3) {

      buffer_rangeArrStruct_INDEX.append(rangeArrStruct_index())

      i=i+1
    }
/*
    i=0

    while (i < 3) {

      buffer_rangeArrStruct_PID.append(rangeArrStruct_index_pid())

      i=i+1
    }
*/

    i=0

    while (i<3) {

      buffer_rangeBinary_BF.append(rangeBinary_BF())

      i=i+1

    }

    i=0

    while (i<3) {

      buffer_rangeBinary_INDEX.append(rangeBinary_index())

      i=i+1

    }

    i=0
/*
    while (i < 3) {

      buffer_rangeBinary_PID.append(rangeBinary_index_pid())

      i=i+1
    }
*/
    i = 0

    while (i < 3) {

      buffer_rangeBinaryTraj_part.append(rangeBinaryTraj_Part())

      i = i + 1

    }

    println("Arr struct mean time: "+ buffer_rangeArrStructBF.sum /buffer_rangeArrStructBF.length.toDouble)
    println("Arr struct index mean time: "+ buffer_rangeArrStruct_INDEX.sum /buffer_rangeArrStruct_INDEX.length.toDouble)
   // println("Arr struct index pid: "+ buffer_rangeArrStruct_PID.sum /buffer_rangeArrStruct_PID.length.toDouble)

    println("Binary mean time: "+ buffer_rangeBinary_BF.sum /buffer_rangeBinary_BF.length.toDouble)
    println("Binary index mean time: "+ buffer_rangeBinary_INDEX.sum /buffer_rangeBinary_INDEX.length.toDouble)
  //  println("Binary index pid: "+ buffer_rangeBinary_PID.sum /buffer_rangeBinary_PID.length.toDouble)
    println("Binary rangeBinaryTraj_Part mean time: "+ buffer_rangeBinaryTraj_part.sum /buffer_rangeBinaryTraj_part.length.toDouble)

/*
    try {

      i = 0

      while (i < 3) {

        buffer_rangeBinaryTraj_part.append(rangeBinaryTraj_Part())

        i = i + 1

      }

      i = 0

      while (i < 3) {

        buffer_rangeBinaryTraj.append(rangeBinaryTraj())

        i = i + 1

      }

      println("Arr struct mean time: "+ buffer_rangeArrStructBF.sum /buffer_rangeArrStructBF.length.toDouble)
      println("Arr struct index mean time: "+ buffer_rangeArrStruct_INDEX.sum /buffer_rangeArrStruct_INDEX.length.toDouble)
      println("Arr struct index pid: "+ buffer_rangeArrStruct_PID.sum /buffer_rangeArrStruct_PID.length.toDouble)

      println("Binary mean time: "+ buffer_rangeBinary_BF.sum /buffer_rangeBinary_BF.length.toDouble)
      println("Binary index mean time: "+ buffer_rangeBinary_INDEX.sum /buffer_rangeBinary_INDEX.length.toDouble)
      println("Binary index pid: "+ buffer_rangeBinary_PID.sum /buffer_rangeBinary_PID.length.toDouble)
      println("Binary traj mean time: "+ buffer_rangeBinaryTraj.sum /buffer_rangeBinaryTraj.length.toDouble)

      println("Binary rangeBinaryTraj_Part mean time: "+ buffer_rangeBinaryTraj_part.sum /buffer_rangeBinaryTraj_part.length.toDouble)

    } catch {
      case e:Exception => {
        println("Arr struct mean time: "+ buffer_rangeArrStructBF.sum /buffer_rangeArrStructBF.length.toDouble)
        println("Arr struct index mean time: "+ buffer_rangeArrStruct_INDEX.sum /buffer_rangeArrStruct_INDEX.length.toDouble)
        println("Arr struct index pid: "+ buffer_rangeArrStruct_PID.sum /buffer_rangeArrStruct_PID.length.toDouble)

        println("Binary mean time: "+ buffer_rangeBinary_BF.sum /buffer_rangeBinary_BF.length.toDouble)
        println("Binary index mean time: "+ buffer_rangeBinary_INDEX.sum /buffer_rangeBinary_INDEX.length.toDouble)
        println("Binary index pid: "+ buffer_rangeBinary_PID.sum /buffer_rangeBinary_PID.length.toDouble)

        println("Binary traj mean time: "+ buffer_rangeBinaryTraj.sum /buffer_rangeBinaryTraj.length.toDouble)

        println("Binary rangeBinaryTraj_Part mean time: "+ buffer_rangeBinaryTraj_part.sum /buffer_rangeBinaryTraj_part.length.toDouble)

        System.exit(0)
      }
    }
*/
  }
}

//imis

/*
import java.sql.{DriverManager, ResultSet}

import di.thesis.indexing.types.EnvelopeST
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import utils.MbbSerialization

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class RangeQueries  extends FunSuite {

  private val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
  private val stmt = con.createStatement
  private var rs:Option[ResultSet]=None
  private var env:Option[EnvelopeST]=None

  //TODO

  def rangeArrStruct_BF():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT rowId FROM imis400_temp" +
      " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeArrStruct_index():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400 as a INNER JOIN " +
      " (SELECT ST_IndexIntersects(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ),tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM index_imis400 WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ) , box, 0.1, 0.1, 0.1, 0.1, 0, 0)) as b" .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      "  ON (b.trajectory_id=a.rowId) " +
      " WHERE ST_Intersects3D(MbbConstructor( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeBinary_BF():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }


    val start=System.currentTimeMillis()

    val sql = " SELECT rowId " +
      " FROM imis400_temp_binary " +
      " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0) "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)


    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeBinary_index():Long={
    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }

    val start=System.currentTimeMillis()

    val sql = " SELECT * FROM trajectories_imis400_binary as a INNER JOIN " +
      " (SELECT ST_IndexIntersectsBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM index_imis400_binary WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), box, 0.1, 0.1, 0.1, 0.1, 0, 0)) as b " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " ON (b.trajectory_id=a.rowId) " +
      " WHERE ST_Intersects3DBinary(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), trajectory, 0.1, 0.1, 0.1, 0.1, 0, 0)  "
        .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT)

    stmt.execute(sql)
    System.currentTimeMillis()-start
  }

  def rangeBinaryTraj():Long={

    rs=Some(stmt.executeQuery("select box from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    while (rs.get.next()) {
      val obj=rs.get.getObject(1)

      env=Some(MbbSerialization.deserialize(obj.asInstanceOf[Array[Byte]]))

    }


    val start=System.currentTimeMillis()

    val sql="SELECT IndexIntersectsTraj(MbbConstructorBinary( %s, %s, %s, %s, CAST(%s as BIGINT), CAST(%s as BIGINT) ), tree, 0.1, 0.1, 0.1, 0.1, 0, 0) " .format(env.get.getMinX, env.get.getMaxX, env.get.getMinY, env.get.getMaxY, env.get.getMinT, env.get.getMaxT) +
      " FROM index_imis400_binaryTraj "

    stmt.execute(sql)

    System.currentTimeMillis()-start  }


  test("Thesis range queries") {

    stmt.execute("ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar")

    stmt.execute(" SET hive.auto.convert.join=true ")
    stmt.execute(" SET hive.enforce.bucketing=true ")
    stmt.execute(" SET hive.optimize.bucketmapjoin.sortedmerge = true ")
    stmt.execute(" SET hive.auto.convert.sortmerge.join=true ")
    stmt.execute(" SET hive.optimize.bucketmapjoin = true ")
    stmt.execute(" SET hive.auto.convert.join.noconditionaltask = true ")
    stmt.execute(" SET hive.auto.convert.join.noconditionaltask.size = 10000000 ")

    stmt.execute(" SET hive.vectorized.execution.enabled=true ")
    stmt.execute(" SET hive.exec.parallel=true ")
    stmt.execute(" SET mapred.compress.map.output=true ")
    stmt.execute(" SET mapred.output.compress=true ")
    stmt.execute(" SET hive.cbo.enable=true ")
    stmt.execute(" SET hive.stats.autogather=true ")
    stmt.execute(" SET hive.optimize.ppd=true ")
    stmt.execute(" SET hive.optimize.ppd.storage=true ")
    stmt.execute(" SET hive.vectorized.execution.reduce.enabled=true ")
    stmt.execute(" SET hive.stats.fetch.column.stats=true ")
    stmt.execute(" SET hive.tez.auto.reducer.parallelism=true ")

    stmt.execute(" set hive.server2.tez.initialize.default.sessions=true ")
    stmt.execute(" set hive.prewarm.enabled=true ")
    stmt.execute(" set hive.prewarm.numcontainers=15 ")
    stmt.execute(" set tez.am.container.reuse.enabled=true ")
    stmt.execute(" set hive.server2.enable.doAs=false ")


    stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructor AS 'di.thesis.hive.mbb.MbbSTUDF' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersects AS 'di.thesis.hive.stoperations.IndexIntersects3D' ")


    stmt.execute(" CREATE TEMPORARY FUNCTION ST_Intersects3DBinary AS 'di.thesis.hive.stoperations.ST_Intersects3DBinary' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION MbbConstructorBinary AS 'di.thesis.hive.mbb.MbbSTUDFBinary' ")
    stmt.execute(" CREATE TEMPORARY FUNCTION ST_IndexIntersectsBinary AS 'di.thesis.hive.stoperations.IndexIntersects3DBinary' ")

    stmt.execute(" CREATE TEMPORARY FUNCTION IndexIntersectsTraj AS 'di.thesis.hive.stoperations.IndexIntersectsTraj' ")

    val buffer_rangeArrStructBF=new ArrayBuffer[Long]
    val buffer_rangeArrStruct_INDEX=new ArrayBuffer[Long]

    val buffer_rangeBinary_BF=new ArrayBuffer[Long]
    val buffer_rangeBinary_INDEX=new ArrayBuffer[Long]


    val buffer_rangeBinaryTraj=new ArrayBuffer[Long]

    var i=0

    while (i < 3) {

      buffer_rangeArrStructBF.append(rangeArrStruct_BF())

      i=i+1
    }

    i=0

    while (i < 3) {

      buffer_rangeArrStruct_INDEX.append(rangeArrStruct_index())

      i=i+1
    }


    i=0

    while (i<3) {

      buffer_rangeBinary_BF.append(rangeBinary_BF())

      i=i+1

    }

    i=0

    while (i<3) {

      buffer_rangeBinary_INDEX.append(rangeBinary_index())

      i=i+1

    }

    i=0

    while (i<3) {

      buffer_rangeBinaryTraj.append(rangeBinaryTraj())

      i=i+1

    }


    println("Arr struct mean time: "+ buffer_rangeArrStructBF.sum /buffer_rangeArrStructBF.length.toDouble)
    println("Arr struct index mean time: "+ buffer_rangeArrStruct_INDEX.sum /buffer_rangeArrStruct_INDEX.length.toDouble)

    println("Binary mean time: "+ buffer_rangeBinary_BF.sum /buffer_rangeBinary_BF.length.toDouble)
    println("Binary index mean time: "+ buffer_rangeBinary_INDEX.sum /buffer_rangeBinary_INDEX.length.toDouble)

    println("Binary mean time: "+ buffer_rangeBinaryTraj.sum /buffer_rangeBinaryTraj.length.toDouble)
  }
}

 */