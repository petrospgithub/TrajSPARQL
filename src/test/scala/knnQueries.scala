import java.sql.DriverManager

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class knnQueries extends FunSuite {

  private val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
  private val stmt = con.createStatement

  def knnArrStruct_BF(): Long = {

    val rs = stmt.executeQuery("select rowId from imis400_temp distribute by rand() sort by rand() limit 1")

    var id: Option[Long] = None

    while (rs.next()) {
      id = Some(rs.getLong(1))
    }

    val start = System.currentTimeMillis()
    /*
        val sql = " SELECT a.rowId, ToOrderedList_arr( DTW_arr(a.trajectory, b.trajectory, 50, 'Euclidean', 21600, 21600), b.rowId, '-k -1', a.trajectory, b.trajectory) " +
          "FROM (SELECT * FROM imis400_temp WHERE rowId=" + id.get + ") as a CROSS JOIN imis400_temp as b " +
          " GROUP BY a.rowId  "
    */

    val sql=" SELECT c.rowId, ToOrderedList_arr( DTW_arr(c.trajectory, trajectories_imis400_pid.trajectory, 50, 'Euclidean', 21600, 21600), trajectories_imis400_pid.rowId, '-k -1', c.trajectory, trajectories_imis400_pid.trajectory " +
      " FROM " +
      " (SELECT IndexTrajKNN_arr(a.trajectory,b.tree, 2000.0, 21600, 21600, a.rowId) " +
      " FROM (SELECT * FROM trajectories_imis400_pid WHERE rowId=" + id.get + ") as a JOIN partition_index_imis400 as b ) as c " +
      " JOIN trajectories_imis400_pid ON (c.trajectory_id==trajectories_imis400_pid.pid) " +
      " GROUP BY c.rowId "

    stmt.execute(sql)

    System.currentTimeMillis() - start
  }

  def knnArrStruct_index(): Long = {

    val rs = stmt.executeQuery("select rowId from imis400_temp distribute by rand() sort by rand() limit 1")

    var id: Option[Long] = None

    while (rs.next()) {
      id = Some(rs.getLong(1))
    }

    val start = System.currentTimeMillis()

    val sql = " SELECT " +
      " trajectories_imis400.rowId, ToOrderedList_arr( DTW_arr(trajectories_imis400.trajectory, final.trajectory, 50, 'Euclidean', 21600, 21600), final.trajectory_id, '-k -1', trajectories_imis400.trajectory, final.trajectory) " +
      " FROM ( SELECT IndexTrajKNN_arr(c.trajectory, d.tree, 2000.0, 21600, 21600, c.rowId) " +
      " FROM " +
      " ( SELECT IndexTrajKNN_arr(a.trajectory,b.tree, 2000.0, 21600, 21600, a.rowId) FROM ( SELECT * FROM trajectories_imis400 where rowId=" + id.get + " ) as a JOIN partition_index_imis400 as b ) as c " +
      " INNER JOIN index_imis400 as d ON (c.trajectory_id=d.id) ) as final INNER JOIN trajectories_imis400 ON (final.trajectory_id=trajectories_imis400.rowId) " +
      " GROUP BY trajectories_imis400.rowId "

    stmt.execute(sql)


    System.currentTimeMillis() - start
  }

  def knnBinary_BF(): Long = {

    val rs = stmt.executeQuery("select rowId from imis400_temp distribute by rand() sort by rand() limit 1")

    var id: Option[Long] = None

    while (rs.next()) {
      id = Some(rs.getLong(1))
    }


    val start = System.currentTimeMillis()
    /*
        val sql = "SELECT a.rowId, ToOrderedListBinary( DTW_binary(a.trajectory, b.trajectory, 50, 'Euclidean', 21600, 21600), b.rowId, '-k -1', a.trajectory, b.trajectory) " +
          " FROM (SELECT * FROM trajectories_imis400_binary WHERE rowId=" + id.get + ") as a CROSS JOIN trajectories_imis400_binary as b " +
          " GROUP BY a.rowId "
    */

    val sql=" SELECT c.rowId, ToOrderedListBinary( DTW_binary(c.trajectory, trajectories_imis400_pid.trajectory, 50, 'Euclidean', 21600, 21600), trajectories_imis400_pid.rowId, '-k -1', c.trajectory, trajectories_imis400_pid.trajectory " +
      " FROM " +
      " (SELECT IndexTrajKNN_arr(a.trajectory,b.tree, 2000.0, 21600, 21600, a.rowId) " +
      " FROM (SELECT * FROM trajectories_imis400_binary_pid WHERE rowId=" + id.get + ") as a JOIN partition_index_imis400_binary as b ) as c " +
      " JOIN trajectories_imis400_binary_pid ON (c.trajectory_id==trajectories_imis400_pid.pid) " +
      " GROUP BY c.rowId "

    stmt.execute(sql)

    System.currentTimeMillis() - start
  }

  def knnBinary_index(): Long = {

    val rs = stmt.executeQuery("select rowId from imis400_temp distribute by rand() sort by rand() limit 1")

    var id: Option[Long] = None

    while (rs.next()) {
      id = Some(rs.getLong(1))
    }


    val start = System.currentTimeMillis()

    val sql = " SELECT " +
      " trajectories_imis400_binary.rowId, ToOrderedListBinary( DTW_binary(trajectories_imis400_binary.trajectory, final.trajectory, 50, 'Euclidean', 21600, 21600), final.trajectory_id, '-k -1', trajectories_imis400_binary.trajectory, final.trajectory) " +
      " FROM ( SELECT IndexTrajKNN_binary(c.trajectory, d.tree, 2000.0, 21600, 21600, c.rowId) " +
      " FROM " +
      " ( SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 2000.0, 21600, 21600, a.rowId) FROM ( SELECT * FROM trajectories_imis400_binary where rowId=" + id.get + " ) as a JOIN partition_index_imis400_binary as b ) as c " +
      " INNER JOIN index_imis400_binary as d ON (c.trajectory_id=d.id) ) as final INNER JOIN trajectories_imis400_binary ON (final.trajectory_id=trajectories_imis400_binary.rowId) " +
      " GROUP BY trajectories_imis400_binary.rowId "

    stmt.execute(sql)

    System.currentTimeMillis() - start
  }

  def knnBinaryTraj(): Long = {
    val rs = stmt.executeQuery("select rowId from imis400_temp distribute by rand() sort by rand() limit 1")

    var id: Option[Long] = None

    while (rs.next()) {
      id = Some(rs.getLong(1))
    }


    val start = System.currentTimeMillis()

    val sql = "SELECT IndexStoreTrajKNN_binary(c.trajectory, d.tree, 2000.0, 21600, 21600, c.rowId, 'DTW', 'Euclidean', 1, 50, 0.0, 0 ) " +
      " FROM ( SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 2000.0, 21600, 21600, a.rowId) FROM (SELECT * FROM trajectories_imis400_binary where rowId=" + id.get + ") as a JOIN partition_index_imis400_binary as b ) as c" +
      " INNER JOIN index_imis400_binaryTraj as d ON (c.trajectory_id=d.id) "

    stmt.execute(sql)

    System.currentTimeMillis() - start
  }

  test("Thesis range queries") {

    stmt.execute("ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar ")

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


    /*
    TODO add knn udfs!
     */

    stmt.execute(" CREATE TEMPORARY FUNCTION IndexTrajKNN_arr AS 'di.thesis.hive.similarity.IndexKNN'")
    stmt.execute(" CREATE TEMPORARY FUNCTION IndexTrajKNN_binary AS 'di.thesis.hive.similarity.IndexKNNBinary'")


    stmt.execute(" CREATE TEMPORARY FUNCTION DTW_arr AS 'di.thesis.hive.similarity.DTWUDF'")
    stmt.execute(" CREATE TEMPORARY FUNCTION DTW_binary AS 'di.thesis.hive.similarity.DTWBinary'")


    stmt.execute(" CREATE TEMPORARY FUNCTION IndexStoreTrajKNN_binary AS 'di.thesis.hive.similarity.IndeKNNBinaryStoreTraj'")

    stmt.execute(" CREATE TEMPORARY FUNCTION ToOrderedList_arr AS 'di.thesis.hive.similarity.ToOrderedList'")
    stmt.execute(" CREATE TEMPORARY FUNCTION ToOrderedListBinary AS 'di.thesis.hive.similarity.ToOrderedListBinary'")


    val buffer_knnArrStruct_BF = new ArrayBuffer[Long]
    val buffer_knnArrStruct_INDEX = new ArrayBuffer[Long]

    val buffer_knnBinary_BF = new ArrayBuffer[Long]
    val buffer_knnBinary_INDEX = new ArrayBuffer[Long]

    val buffer_knnBinaryTraj = new ArrayBuffer[Long]

    var i = 0

    while (i < 3) {

      buffer_knnArrStruct_BF.append(knnArrStruct_BF())

      i=i+1
    }

    i=0

    while (i < 3) {

      buffer_knnArrStruct_INDEX.append(knnArrStruct_index())

      i=i+1
    }

    i = 0

    while (i < 3) {

      buffer_knnBinary_BF.append(knnBinary_BF())

      i = i + 1

    }

    i = 0

    while (i < 3) {

      buffer_knnBinary_INDEX.append(knnBinary_index())

      i = i + 1

    }

    i = 0

    while (i < 3) {

      buffer_knnBinaryTraj.append(knnBinaryTraj())

      i = i + 1

    }

    println("Arr struct mean time: " + buffer_knnArrStruct_BF.sum / buffer_knnArrStruct_BF.length.toDouble)
    println("Arr struct index mean time: " + buffer_knnArrStruct_INDEX.sum / buffer_knnArrStruct_INDEX.length.toDouble)

    println("Binary mean time: " + buffer_knnBinary_BF.sum / buffer_knnBinary_BF.length.toDouble)
    println("Binary index mean time: " + buffer_knnBinary_INDEX.sum / buffer_knnBinary_INDEX.length.toDouble)

    println("Binary traj time: " + buffer_knnBinaryTraj.sum / buffer_knnBinaryTraj.length.toDouble)

  }
}

