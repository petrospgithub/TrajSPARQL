package preprocessing

import org.apache.spark.sql.SparkSession

object HiveAttempt {
  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder
      .appName("HiveAttempt").master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE TABLE IF NOT EXISTS raw (ts LONG, id INT, lon DOUBLE, lat DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n' ")

    spark.sql("LOAD DATA LOCAL INPATH 'imis3yearsExample/*' INTO TABLE raw ")

    spark.sql("SELECT * FROM raw ").show()

    spark.sql("CREATE TEMPORARY VIEW data_with_diff AS SELECT cast(id as LONG), ts, lon, lat, ts- lag(ts,1) OVER w as diff FROM raw WINDOW w AS (PARTITION BY id ORDER BY ts) ")//.show()

    spark.sql("CREATE TEMPORARY VIEW sampling AS SELECT lead(ts,1) OVER w - ts as diff FROM raw WINDOW w AS (PARTITION BY id ORDER BY ts) ")

    //spark.sql("SELECT * FROM sampling ").show()

    spark.sql("SELECT avg(diff) as mean FROM sampling ").show()

    spark.sql("SELECT stddev(diff) as stddev FROM sampling ").show()

    spark.sql("ADD JAR bigspatial_frameworks/HiveUDFjars-1.0-SNAPSHOT.jar")//.show()

    spark.sql("CREATE TEMPORARY FUNCTION row_sequence AS 'state.rownumber.UDFRowSequenceCond'")//.show()

    //spark.sql("SELECT * FROM sampling ").show()

    //spark.sql("SELECT CASE isNull(diff) WHEN false THEN diff ELSE 0.0 END FROM sampling ").show()

    spark.sql("CREATE TEMPORARY VIEW traj_temp AS SELECT id, cast(ts as LONG) as timestamp, lon as longitude, lat as latitude, diff, row_sequence(id, CASE isNull(diff) WHEN false THEN diff ELSE 0.0 END , 100 ) as traj_id FROM data_with_diff ")//.show()

    spark.sql("CREATE TEMPORARY VIEW moving_object AS SELECT id, collect_list(struct(longitude, latitude, timestamp)) as trajectory FROM traj_temp GROUP BY id, traj_id")//.show()

    spark.sql("SELECT * FROM moving_object").show()

    spark.sql("DESCRIBE moving_object").collect().foreach(f=>println(f))

    /**/

    spark.close()
  }
}
/*
create external table table_name (
  id int,
  myfields string
)
location '/my/location/in/hdfs';
 */