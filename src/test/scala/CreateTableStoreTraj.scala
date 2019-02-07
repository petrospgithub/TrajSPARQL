import java.sql.{DriverManager, ResultSet}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/*
nohup /root/apache-hive-2.3.3-bin/bin/hive --service hiveserver2 --hiveconf hive.root.logger=INFO,console --hiveconf mapreduce.map.memory.mb=7168 --hiveconf mapreduce.map.java.opts=-Xmx5734m --hiveconf mapreduce.reduce.memory.mb=7168 --hiveconf mapreduce.reduce.java.opts=-Xmx5734m > hiveserver.out &

ss -lptn 'sport = :10000'

hdfs dfs -rm -r /tmp/hive && hdfs dfs -mkdir /tmp/hive && hdfs dfs -chmod -R 777 /tmp/hive

 */

@RunWith(classOf[JUnitRunner])
class CreateTableStoreTraj  extends FunSuite {

  test("Thesis create tables") {

    val con = DriverManager.getConnection("jdbc:hive2://83.212.100.24:10000/default", "root", "dithesis13@")
    val stmt = con.createStatement
    var rs:Option[ResultSet]=None

    var create = ""
    var insert = ""

    //mvn test -Dtest=CreateTables -q -DargLine="-Dbuckets=30"

    val buckets_num = Integer.valueOf(System.getProperty("buckets"))


        /* Binary store trajectories @ index */
    /*
        create = " CREATE EXTERNAL TABLE imis400_temp_binaryTraj (  id BIGINT,  trajectory BINARY,  traj_id BIGINT,  rowId BIGINT,  pid BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_repartition_binary_imis400_parquet' "

        stmt.execute(create)
    */
        create = " CREATE EXTERNAL TABLE index_imis400_temp_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY ) STORED AS PARQUET LOCATION 'hdfs:///user/root/octree_traj_partitionMBBDF_binary_imis400_parquet' "

        stmt.execute(create)


     //   create = " CREATE EXTERNAL TABLE partition_index_imis400_binaryTraj (tree BINARY )  STORED AS PARQUET LOCATION 'hdfs:///user/root/partitions_traj_tree_binary_imis400_parquet' "

       // stmt.execute(create)
    /*
        create = " CREATE TABLE trajectories_imis400_binaryTraj ( id BIGINT, trajectory BINARY,  rowId BIGINT,  pid BIGINT) CLUSTERED BY (rowId) SORTED BY (rowId) INTO "+buckets_num+" BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

        stmt.execute(create)
    */

        create = " CREATE TABLE index_imis400_binaryTraj (  id BIGINT,  box BINARY,  tree BINARY )  CLUSTERED BY (id) SORTED BY (id) INTO "+buckets_num+" BUCKETS STORED AS ORC TBLPROPERTIES(\"orc.compress\"=\"snappy\") "

        stmt.execute(create)
    /*
        insert = " INSERT INTO trajectories_imis400_binaryTraj SELECT id, trajectory, rowId, pid FROM imis400_temp_binaryTraj "

        stmt.execute(insert)
    */
        insert = " INSERT INTO index_imis400_binaryTraj SELECT id, box, tree FROM index_imis400_temp_binaryTraj "

        stmt.execute(insert)

       // stmt.execute("analyze table trajectories_imis400_binaryTraj compute statistics")
       // stmt.execute("analyze table trajectories_imis400_binaryTraj compute statistics for columns")

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