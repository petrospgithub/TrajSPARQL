package pidknn

import java.sql.{DriverManager, ResultSet}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class knnSelf_arrIndex extends FunSuite {

  private val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
  private val stmt = con.createStatement
  private var rs:Option[ResultSet]=None

  def knnArrStruct_index(): Long = {
/*
    rs = Some(stmt.executeQuery("select id from index_imis400 distribute by rand() sort by rand() limit 1"))

    var id: Option[Long] = None

    while (rs.get.next()) {
      id = Some(rs.get.getLong(1))
    }
*/
    val start = System.currentTimeMillis()

    val sql = " SELECT " +
      " trajectories_imis400.rowId, ToOrderedList_arr( DTW_arr(trajectories_imis400.trajectory, final.trajectory, 50, 'Euclidean', 604800, 604800), final.trajectory_id, '-k -1', trajectories_imis400.trajectory, final.trajectory) " +
      " FROM ( SELECT IndexTrajKNN_arr(c.trajectory, d.tree, 40000.1, 604800, 604800, c.rowId) " +
      " FROM " +
      " ( SELECT IndexTrajKNN_arr(a.trajectory,b.tree, 40000.1, 604800, 604800, a.rowId) FROM trajectories_imis400  as a JOIN partition_index_imis400 as b ) as c " +
      " INNER JOIN index_imis400 as d ON (c.trajectory_id=d.id) ) as final INNER JOIN trajectories_imis400 ON (final.trajectory_id=trajectories_imis400.rowId) " +
      " GROUP BY trajectories_imis400.rowId "
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
    stmt.execute(" SET hive.auto.convert.join.noconditionaltask.size = 256000000 ")
    stmt.execute(" SET hive.vectorized.execution.enabled=true ")
    stmt.execute(" SET hive.vectorized.execution.reduce.enabled=true ")

    stmt.execute(" SET hive.exec.parallel=true ")
    stmt.execute(" SET mapred.compress.map.output=true ")
    stmt.execute(" SET mapred.output.compress=true ")
    stmt.execute(" SET hive.cbo.enable=true ")
    stmt.execute(" SET hive.stats.autogather=true ")
    stmt.execute(" SET hive.optimize.ppd=true ")
    stmt.execute(" SET hive.optimize.ppd.storage=true ")

    stmt.execute("SET hive.tez.auto.reducer.parallelism=true")

    stmt.execute(" SET hive.stats.fetch.column.stats=true ")
    stmt.execute(" SET hive.tez.auto.reducer.parallelism=true ")
    stmt.execute("set hive.optimize.index.filter=true")

    stmt.execute(" set hive.prewarm.enabled=true ")
    stmt.execute(" set hive.prewarm.numcontainers=3 ")
    stmt.execute(" set hive.server2.tez.initialize.default.sessions=true ")
    stmt.execute(" set tez.am.container.reuse.enabled=true ")
    stmt.execute(" set hive.server2.enable.doAs=false ")

    stmt.execute("set tez.grouping.min-size=16777216")
    stmt.execute("set tez.grouping.max-size=256000000")

    stmt.execute("set hive.merge.mapfiles=false")

    stmt.execute("set tez.runtime.pipelined-shuffle.enabled=true")
    stmt.execute("set tez.runtime.pipelined.sorter.lazy-allocate.memory=true")

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


    val buffer_knnArrStruct_index = new ArrayBuffer[Long]


    var i = 0

    while (i < 3) {

      buffer_knnArrStruct_index.append(knnArrStruct_index())

      i = i + 1
    }
    println("Arr struct index mean time: " + buffer_knnArrStruct_index.sum / buffer_knnArrStruct_index.length.toDouble)
    con.close()

  }
}

