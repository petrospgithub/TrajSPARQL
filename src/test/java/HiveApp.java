import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HiveApp extends TestCase {

    public void test() throws Exception {
        //System.out.println("Hello World!");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example").master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset ds = spark.read().parquet("/home/petros/IdeaProjects/data/sample2").limit(20);

       ds.show();

      //  Dataset index = spark.read().parquet("/home/petros/IdeaProjects/data/bsp_partitionMBBDF_oasa_600_parquet");//.limit(200);


    //    index.printSchema();
    //    index.show();


  //      spark.sql("ADD JAR file:////home/petros/IdeaProjects/TrajSPARQL/bigspatial_frameworks/hivemall-all-0.5.0-incubating.jar");

        spark.sql("ADD JAR file:////home/petros/IdeaProjects/HiveTrajSPARQL/target/HiveTrajSPARQL-jar-with-dependencies.jar");


     //   spark.sql("ADD JAR hdfs:///user/root/hiveThesis/HiveTrajSPARQL-jar-with-dependencies.jar");

     //   spark.sql("ADD JAR file:////root/implementation/HiveTrajSPARQL/HiveTrajSPARQL-jar-with-dependencies.jar");

        //Dataset join = ds.crossJoin(ds);

        //System.out.println(join.count());

        ds.createOrReplaceTempView("trajectories_oasa600");
  //      index.createOrReplaceTempView("trajectories_oasa600_index");

        //System.exit(0);

    //    spark.sql("DROP FUNCTION IndexTrajKNN");

        spark.sql(" CREATE TEMPORARY FUNCTION StartPoint AS 'di.thesis.hive.extras.StartPoint'");
        spark.sql(" CREATE TEMPORARY FUNCTION EndPoint AS 'di.thesis.hive.extras.EndPoint'");
        spark.sql(" CREATE TEMPORARY FUNCTION Trajectory2Linestring AS 'di.thesis.hive.extras.Trajectory2Linestring'");
        spark.sql(" CREATE TEMPORARY FUNCTION MbbConstructor AS 'di.thesis.hive.mbb.MbbSTUDF'");
        spark.sql(" CREATE TEMPORARY FUNCTION TrajBoxDist AS 'di.thesis.hive.similarity.TrajBoxUDF'");

        spark.sql(" CREATE TEMPORARY FUNCTION DTW AS 'di.thesis.hive.similarity.DtwDebugging'");


      //  spark.sql(" CREATE TEMPORARY FUNCTION LCSS AS 'di.thesis.hive.similarity.LCSS'");
    //    spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN AS 'di.thesis.hive.similarity.IndexKNN'");
        spark.sql(" CREATE TEMPORARY FUNCTION TrajLength AS 'di.thesis.hive.statistics.Length'");
        spark.sql(" CREATE TEMPORARY FUNCTION TrajDuration AS 'di.thesis.hive.statistics.Duration'");
        spark.sql(" CREATE TEMPORARY FUNCTION IndexTrajKNN AS 'di.thesis.hive.similarity.IndexKNN'");
        spark.sql(" CREATE TEMPORARY FUNCTION ST_Intersects3D AS 'di.thesis.hive.stoperations.ST_Intersects3D'");
        spark.sql(" CREATE TEMPORARY FUNCTION each_top_k AS 'hivemall.tools.EachTopKUDTF'"); //TODO CHECK LEFT OUTER JOIN https://hivemall.incubator.apache.org/userguide/misc/topk.html

      //  spark.sql(" CREATE TEMPORARY FUNCTION to_ordered_list AS 'di.thesis.hive.similarity.ToOrderedList'");

        spark.sql(" CREATE TEMPORARY FUNCTION to_ordered_list AS 'di.thesis.hive.similarity.OrderedListDebbuging'");

        spark.sql(" CREATE TEMPORARY FUNCTION LoggerPolygon AS 'di.thesis.hive.test.LoggerPolygon'"); //LoggerPolygon

        spark.sql("SELECT a.rowId, b.rowId, DTW(a.trajectory, b.trajectory, 50, 'Euclidean', 21600, 21600) FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600 as b WHERE a.rowId=0 ").foreach(f->{
            System.out.print(f.get(0));
            System.out.print("\t");
            System.out.print(f.get(1));
            System.out.print("\t");
            System.out.println(f.get(2));

        });

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");


        spark.sql("SELECT a.rowId, to_ordered_list( DTW(a.trajectory, b.trajectory, 50, 'Euclidean', 21600, 21600), b.rowId, '-k -3', a.trajectory, b.trajectory) FROM (SELECT * FROM trajectories_oasa600 WHERE rowId=0) as a CROSS JOIN trajectories_oasa600 as b GROUP BY a.rowId ").foreach(f->{
            System.out.print(f.get(0));
            System.out.print("\t");
            System.out.println(f.get(1));
        });

        /*
        spark.sql("SELECT a.rowId, to_ordered_list( DTW(a.trajectory, b.trajectory, 20, 'Euclidean', 21600, 21600), b.rowId, '-k -2', a.rowId, a.trajectory, b.trajectory) FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600 as b GROUP BY a.rowId ").foreach(f->{
            System.out.print(f.get(0));
            System.out.print("\t");
            System.out.println(f.get(1));

        });
*/



/*
        spark.sql("SELECT MbbConstructor(trajectory) FROM trajectories_oasa600 ").foreach(f->{
            System.out.println(f.get(0));
        });

        spark.sql("SELECT * FROM trajectories_oasa600 ").foreach(f->{
            System.out.println(f);
        });


        spark.sql("SELECT StartPoint(trajectory) FROM trajectories_oasa600 as a ").show();
        spark.sql("SELECT EndPoint(trajectory) FROM trajectories_oasa600 ").show();
*/
        /*
        spark.sql("SELECT Trajectory2Linestring(trajectory) FROM trajectories_oasa600 ").foreach(f->{
            System.out.println(f.get(0));
        });


        spark.sql("SELECT TrajLength(trajectory, 'Euclidean') FROM trajectories_oasa600 ").show();
        spark.sql("SELECT TrajDuration(trajectory) FROM trajectories_oasa600 ").show();


        Dataset q1=spark.sql("SELECT * FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600 as b ");

        System.out.println(q1.count());
        q1.show();

        spark.sql("SELECT * " +
                "FROM trajectories_oasa600 as a " +
                "WHERE ST_Intersects3D(" +
                "MbbConstructor( 2630991.637102703,2636234.7851190665,4576784.466741486,4579538.82665573,CAST(1480881000 as BIGINT), CAST(1480885200 as BIGINT) ), trajectory, 0.0, 0.0, 0.0, 0.0, 0, 0" +
                ")").show();

        spark.sql("SELECT TrajBoxDist(trajectory, MbbConstructor( 2630991.637102703,2636234.7851190665,4576784.466741486,4579538.82665573,CAST(1480881000 as BIGINT), CAST(1480885200 as BIGINT) ) ) FROM trajectories_oasa600 as ").show();

        spark.sql("SELECT DTW(a.trajectory, b.trajectory, 10, 'Euclidean', 100000.0, 1000, 1000) FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600 as b ").show();

        spark.sql("SELECT LCSS(a.trajectory, b.trajectory, 'Euclidean', 1000.0, 10000) FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600 as b ").show();

        spark.sql("SELECT * FROM trajectories_oasa600_index ").show();

        spark.sql("SELECT * FROM trajectories_oasa600_index as a CROSS JOIN trajectories_oasa600_index as b").show();

        spark.sql("SELECT * FROM trajectories_oasa600_index as a CROSS JOIN trajectories_oasa600_index as b WHERE ST_Intersects3D(a.box, b.box, 0.0, 0.0, 0.0, 0.0, 0, 0 )").show();

        spark.sql("SELECT * FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600_index as b WHERE ST_Intersects3D(a.trajectory, b.box, 0.0, 0.0, 0.0, 0.0, 0, 0 )").show();

        spark.sql("SELECT * FROM trajectories_oasa600 as a CROSS JOIN trajectories_oasa600_index as b WHERE a.pid=b.id").show();

        spark.sql("SELECT a.id, explode(ST_IndexIntersects(a.box, b.tree, 0.0, 0.0, 0.0, 0.0, 0, 0)) FROM trajectories_oasa600_index as a CROSS JOIN trajectories_oasa600_index as b WHERE ST_Intersects3D(a.box, b.box, 0.0, 0.0, 0.0, 0.0, 0, 0 )").show();


        spark.sql("SELECT * from trajectories_oasa600 WHERE rowId=131").show();


        TODO
        ST_IndexIntersects me mbb kai trajectories me mbb

        an xreiaste ta index based pragmata na ginoun se UDFT




        TODO CHECK
        Dataset q2=spark.sql("SELECT * FROM trajectories_oasa600 as a LEFT OUTER JOIN trajectories_oasa600 as b LIMIT 10");

        System.out.println(q2.count());
        q2.show();

*/
        spark.stop();

    }

}
