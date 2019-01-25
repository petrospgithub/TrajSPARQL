package pidknn

import java.sql.{DriverManager, ResultSet}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class knnSelf_binaryPID extends FunSuite {

  private val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
  private val stmt = con.createStatement
  private var rs:Option[ResultSet]=None

  def knnBinary_BF(): Long = {

    rs = Some(stmt.executeQuery("select id from index_imis400_binary distribute by rand() sort by rand() limit 1"))

    var id: Option[Long] = None

    while (rs.get.next()) {
      id = Some(rs.get.getLong(1))
    }


    val start = System.currentTimeMillis()
    /*
        val sql = "SELECT a.rowId, ToOrderedListBinary( DTW_binary(a.trajectory, b.trajectory, 50, 'Euclidean', 21600, 21600), b.rowId, '-k -1', a.trajectory, b.trajectory) " +
          " FROM (SELECT * FROM trajectories_imis400_binary WHERE rowId=" + id.get + ") as a CROSS JOIN trajectories_imis400_binary as b " +
          " GROUP BY a.rowId "
    */

    val sql = " SELECT c.rowId, ToOrderedListBinary( DTW_binary(c.trajectory, trajectories_imis400_binary_pid.trajectory, 50, 'Euclidean', 604800, 604800), trajectories_imis400_binary_pid.rowId, '-k -1', c.trajectory, trajectories_imis400_binary_pid.trajectory) " +
      " FROM " +
      " (SELECT IndexTrajKNN_binary(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) " +
      " FROM (SELECT * FROM trajectories_imis400_binary_pid WHERE pid=" + id.get + ") as a JOIN partition_index_imis400_binary as b ) as c " +
      " JOIN trajectories_imis400_binary_pid ON (c.trajectory_id==trajectories_imis400_binary_pid.pid) " +
      " GROUP BY c.rowId "
    /*
        println(sql)

        while (rs.get.next()) {
          println(rs.get.getObject(1))
          println(rs.get.getObject(2))
        }
    */
    stmt.execute(sql)

    System.currentTimeMillis() - start
  }
  test("Thesis pidknn.knnSelf queries") {

    //stmt.execute("ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar ")

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

    val buffer_knnBinary_BF = new ArrayBuffer[Long]

    var i = 0

    while (i < 3) {

      buffer_knnBinary_BF.append(knnBinary_BF())

      i = i + 1

    }
    println("Binary mean time: " + buffer_knnBinary_BF.sum / buffer_knnBinary_BF.length.toDouble)

    con.close()

  }
}

