import java.sql.{DriverManager, ResultSet}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/*
nohup apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=6144 --hiveconf mapreduce.map.java.opts=-Xmx8192m --hiveconf mapreduce.reduce.memory.mb=6144 --hiveconf mapreduce.reduce.java.opts=-Xmx8192m > hiveserver.out &


ss -lptn 'sport = :10000'

 */

@RunWith(classOf[JUnitRunner])
class CreateTables  extends FunSuite {

  test("Thesis create tables") {

    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement
    var rs:Option[ResultSet]=None

    var create = ""
    var insert = ""

    /* Array + Struct */

    create = " CREATE EXTERNAL TABLE oasa400_temp(  id BIGINT,  trajectory ARRAY<STRUCT<longitude:DOUBLE, latitude:DOUBLE, `timestamp`:BIGINT >>,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_repartition_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_oasa400_temp(  id BIGINT,  box STRUCT<id:BIGINT, minx:DOUBLE, maxx:DOUBLE, miny:DOUBLE, maxy:DOUBLE, mint:BIGINT, maxt:BIGINT >,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_partitionMBBDF_oasa400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_oasa400(tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_tree_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_oasa400( id BIGINT, " + " trajectory ARRAY<STRUCT<longitude:DOUBLE, latitude:DOUBLE, `timestamp`:BIGINT >>,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_oasa400(  id BIGINT,  box STRUCT<id:BIGINT, minx:DOUBLE, maxx:DOUBLE, miny:DOUBLE, maxy:DOUBLE, mint:BIGINT, maxt:BIGINT >,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_oasa400 SELECT id, trajectory, rowId, pid FROM oasa400_temp  "

    stmt.execute(insert)

    insert = " INSERT INTO index_oasa400 SELECT id, box, tree FROM index_oasa400_temp "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_oasa400 compute statistics")
    stmt.execute("analyze table trajectories_oasa400 compute statistics for columns")

    stmt.execute("analyze table index_oasa400 compute statistics")
    stmt.execute("analyze table index_oasa400 compute statistics for columns")

    /* Array + Struct */


    /* Binary */

    create = " CREATE EXTERNAL TABLE oasa400_temp_binary (  id BIGINT,  trajectory BINARY,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_repartition_binary_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_oasa400_temp_binary (  id BIGINT,  box BINARY,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_partitionMBBDF_binary_oasa400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_oasa400_binary (tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_tree_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_oasa400_binary ( id BIGINT, trajectory BINARY,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_oasa400_binary (  id BIGINT,  box BINARY,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_oasa400_binary SELECT id, trajectory, rowId, pid FROM oasa400_temp_binary  "

    stmt.execute(insert)

    insert = " INSERT INTO index_oasa400_binary SELECT id, box, tree FROM index_oasa400_temp_binary "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_oasa400_binary compute statistics")
    stmt.execute("analyze table trajectories_oasa400_binary compute statistics for columns")

    stmt.execute("analyze table index_oasa400_binary compute statistics")
    stmt.execute("analyze table index_oasa400_binary compute statistics for columns")

    /* Binary */


    /* Binary store trajectories @ index */

    create = " CREATE EXTERNAL TABLE oasa400_temp_binaryTraj (  id BIGINT,  trajectory BINARY,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_repartition_binary_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_oasa400_temp_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_partitionMBBDF_binary_oasa400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_oasa400_binaryTraj (tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_traj_tree_binary_oasa400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_oasa400_binaryTraj ( id BIGINT, trajectory BINARY,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_oasa400_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_oasa400_binaryTraj SELECT id, trajectory, rowId, pid FROM oasa400_temp_binary  "

    stmt.execute(insert)

    insert = " INSERT INTO index_oasa400_binaryTraj SELECT id, box, tree FROM index_oasa400_temp_binary "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_oasa400_binaryTraj compute statistics")
    stmt.execute("analyze table trajectories_oasa400_binaryTraj compute statistics for columns")

    stmt.execute("analyze table index_oasa400_binaryTraj compute statistics")
    stmt.execute("analyze table index_oasa400_binaryTraj compute statistics for columns")

    /* Binary store trajectories @ index */

    if (rs.isDefined) {
      rs.get.close()
    }
    stmt.close()
    con.close()


  }

}

//imis

/*
import java.sql.{DriverManager, ResultSet}

import com.google.gson.JsonParser
import di.thesis.indexing.types.EnvelopeST
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import utils.MbbSerialization

/*
nohup apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=6144 --hiveconf mapreduce.map.java.opts=-Xmx8192m --hiveconf mapreduce.reduce.memory.mb=6144 --hiveconf mapreduce.reduce.java.opts=-Xmx8192m > hiveserver.out &


ss -lptn 'sport = :10000'

 */

@RunWith(classOf[JUnitRunner])
class CreateTables  extends FunSuite {

  test("Thesis create tables") {

    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement
    var rs:Option[ResultSet]=None

    var create = ""
    var insert = ""

    /* Array + Struct */

    create = " CREATE EXTERNAL TABLE imis400_temp(  id BIGINT,  trajectory ARRAY<STRUCT<longitude:DOUBLE, latitude:DOUBLE, `timestamp`:BIGINT >>,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_repartition_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_imis400_temp(  id BIGINT,  box STRUCT<id:BIGINT, minx:DOUBLE, maxx:DOUBLE, miny:DOUBLE, maxy:DOUBLE, mint:BIGINT, maxt:BIGINT >,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_partitionMBBDF_imis400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_imis400(tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_tree_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_imis400( id BIGINT, " + " trajectory ARRAY<STRUCT<longitude:DOUBLE, latitude:DOUBLE, `timestamp`:BIGINT >>,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_imis400(  id BIGINT,  box STRUCT<id:BIGINT, minx:DOUBLE, maxx:DOUBLE, miny:DOUBLE, maxy:DOUBLE, mint:BIGINT, maxt:BIGINT >,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_imis400 SELECT id, trajectory, rowId, pid FROM imis400_temp  "

    stmt.execute(insert)

    insert = " INSERT INTO index_imis400 SELECT id, box, tree FROM index_imis400_temp "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_imis400 compute statistics")
    stmt.execute("analyze table trajectories_imis400 compute statistics for columns")

    stmt.execute("analyze table index_imis400 compute statistics")
    stmt.execute("analyze table index_imis400 compute statistics for columns")

    /* Array + Struct */


    /* Binary */

    create = " CREATE EXTERNAL TABLE imis400_temp_binary (  id BIGINT,  trajectory BINARY,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_repartition_binary_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_imis400_temp_binary (  id BIGINT,  box BINARY,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_partitionMBBDF_binary_imis400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_imis400_binary (tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_tree_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_imis400_binary ( id BIGINT, trajectory BINARY,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_imis400_binary (  id BIGINT,  box BINARY,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_imis400_binary SELECT id, trajectory, rowId, pid FROM imis400_temp_binary  "

    stmt.execute(insert)

    insert = " INSERT INTO index_imis400_binary SELECT id, box, tree FROM index_imis400_temp_binary "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_imis400_binary compute statistics")
    stmt.execute("analyze table trajectories_imis400_binary compute statistics for columns")

    stmt.execute("analyze table index_imis400_binary compute statistics")
    stmt.execute("analyze table index_imis400_binary compute statistics for columns")

    /* Binary */


    /* Binary store trajectories @ index */

    create = " CREATE EXTERNAL TABLE imis400_temp_binaryTraj (  id BIGINT,  trajectory BINARY,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_repartition_binary_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE EXTERNAL TABLE index_imis400_temp_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_partitionMBBDF_binary_imis400_parquet' "

    stmt.execute(create)


    create = " CREATE EXTERNAL TABLE partition_index_imis400_binaryTraj (tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_traj_tree_binary_imis400_parquet' "

    stmt.execute(create)

    create = " CREATE TABLE trajectories_imis400_binaryTraj ( id BIGINT, trajectory BINARY,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)


    create = " CREATE TABLE index_imis400_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO 20 BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

    stmt.execute(create)

    insert = " INSERT INTO trajectories_imis400_binaryTraj SELECT id, trajectory, rowId, pid FROM imis400_temp_binary  "

    stmt.execute(insert)

    insert = " INSERT INTO index_imis400_binaryTraj SELECT id, box, tree FROM index_imis400_temp_binary "

    stmt.execute(insert)

    stmt.execute("analyze table trajectories_imis400_binaryTraj compute statistics")
    stmt.execute("analyze table trajectories_imis400_binaryTraj compute statistics for columns")

    stmt.execute("analyze table index_imis400_binaryTraj compute statistics")
    stmt.execute("analyze table index_imis400_binaryTraj compute statistics for columns")

    /* Binary store trajectories @ index */

    if (rs.isDefined) {
      rs.get.close()
    }
    stmt.close()
    con.close()


  }

}

 */