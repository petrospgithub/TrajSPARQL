package spatial.partition

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import com.vividsolutions.jts.io.{ParseException, WKTReader}
import index.SpatialIndex
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, Row, RowFactory, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialRDD.SpatialDF
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import utils.{OGCtrasnformations, ReadPostgresGeom}

//TODO add wkt or postgreshex support

object GeoSparkPartitioner {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("GeoSparkPartitioner")//.master("local[*]")
      .getOrCreate()
/**/
      val input: InputStream =new FileInputStream( System.getProperty("user.dir")+"/config/geospark_storeHDFS.properties")

      val prop: Properties = new Properties()
      prop.load(input)

      val host = prop.getProperty("spark.host")
      val user = prop.getProperty("spark.user")
      val pass = prop.getProperty("spark.pass")
      val dbtable = prop.getProperty("spark.dbtable")
      val output=prop.getProperty("spark.output")
      val file_output=prop.getProperty("spark.filetype")
      val geospark_repartition=prop.getProperty("spark.geospark_repartition").toInt
      val geospark_sample_number=prop.getProperty("spark.geospark_sample_number").toLong
      val path=prop.getProperty("spark.path")
      val datatype=prop.getProperty("spark.datatype")
      val srid=prop.getProperty("spark.srid").toInt
      val rtree_nodeCapacity=prop.getProperty("spark.localindex_nodecapacity").toInt
      val name=prop.getProperty("spark.name")
  /*  */

    /*
    val prop = spark.sparkContext.getConf
    val host = prop.get("spark.host")
    val user = prop.get("spark.user")
    val pass = prop.get("spark.pass")
    val dbtable = prop.get("spark.dbtable")
    val output = prop.get("spark.output")
    val file_output = prop.get("spark.filetype")
    val geospark_repartition = prop.get("spark.geospark_repartition").toInt
    val geospark_sample_number = prop.get("spark.geospark_sample_number").toLong
    val path = prop.get("spark.path")
    val datatype = prop.get("spark.datatype")
    val srid = prop.get("spark.srid").toInt
    val rtree_nodeCapacity = prop.get("spark.localindex_nodecapacity").toInt
    val name = prop.get("spark.name")
*/

    val broadcastSRID = spark.sparkContext.broadcast(srid)
    val broadcastrtree_nodeCapacity = spark.sparkContext.broadcast(rtree_nodeCapacity)
    val broadcastDatatype = spark.sparkContext.broadcast(datatype)

    val jdbcDF = spark.read.format("csv").option("header", "true").load(path)

    val newSchema = jdbcDF.schema.add("geom2Text", StringType).add("ogc", BinaryType)

    import spark.implicits._
    import org.apache.spark.sql.catalyst.encoders.RowEncoder

    val encoder = RowEncoder(newSchema)

    val newDF = jdbcDF.map(row => {

      val seq = row.toSeq
      val schema = row.schema

      val geom2Text = broadcastDatatype.value match {
        case "hexstring" =>
          try {
            val index = row.fieldIndex("geom")
            ReadPostgresGeom.hex2Geom(row.get(index).toString)
          } catch {
            case e: NullPointerException => {
              @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
              log.warn("NullPointerException geom2text hexstring: " + row.toString + "\t" + row.fieldIndex("geom"))
            }
          }
        case "wkt" =>
          val ret: String = broadcastSRID.value match {
            case 4326 => row.get(row.fieldIndex("geom")).toString
            case _ => {
              val geom = new WKTReader().read(row.get(row.fieldIndex("geom")).toString)
              geom.setSRID(broadcastSRID.value)
              val sourceCRS: CoordinateReferenceSystem = CRS.decode("EPSG:" + broadcastSRID.value)
              val targetCRS: CoordinateReferenceSystem = CRS.decode("EPSG:4326")

              val transform: MathTransform = CRS.findMathTransform(sourceCRS, targetCRS)

              val targetGeometry = JTS.transform(geom, transform)
              targetGeometry.toText
            }
          }
        case _ => throw new RuntimeException("")
      }

      try {
        val temp_geom = new WKTReader().read(geom2Text.toString)
        temp_geom.setSRID(4326)
        val ogc: Array[Byte] = OGCtrasnformations.seriliaze2ogcgeom(temp_geom)

        val newSeq = seq :+ geom2Text :+ ogc
        val newSchema = row.schema.add("geom2Text", StringType).add("ogc", BinaryType)

        val newRow: Row = new GenericRowWithSchema(newSeq.toArray, newSchema)

        newRow
      } catch {
        case e: ParseException =>
          @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
          log.warn("JTS ParseException: " + row.toString + "\t" + row.fieldIndex("geom"))

          val newSeq = seq :+ "" :+ Array[Byte]()
          val newSchema = row.schema.add("geom2Text", StringType).add("ogc", BinaryType)

          val newRow: Row = new GenericRowWithSchema(newSeq.toArray, newSchema)

          newRow
        case e: NullPointerException =>
          @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
          log.warn("NullPointerException read geom2Text: " + row.toString + "\t" + geom2Text)

          val newSeq = seq :+ "" :+ Array[Byte]()
          val newSchema = row.schema.add("geom2Text", StringType).add("ogc", BinaryType)

          val newRow: Row = new GenericRowWithSchema(newSeq.toArray, newSchema)

          newRow

      }
    })(encoder).filter(row=>{
      val index = row.fieldIndex("geom2Text")
      val check=row.get(index).toString

      if (check=="")
        false
      else
        true
    })

    val rdd = newDF.rdd.map(row => {
        val index = row.fieldIndex("geom2Text")
        val geom=new WKTReader().read(row.get(index).toString)
        if (geom.isValid)
          (geom, row.asInstanceOf[Object])
        else
          (geom.buffer(0.00001), row.asInstanceOf[Object])
          //(geom.getBoundary, row.asInstanceOf[Object])
      })
/*
    rdd.foreach(x=>{
      @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
      log.warn("mapping: " + x._1.toText)
    })
*/
    val spatialDF = new SpatialDF

    spatialDF.setSampleNumber(geospark_sample_number)
    spatialDF.setRawSpatialRDD(rdd.repartition(geospark_repartition).toJavaRDD())
    spatialDF.analyze()
    val broadcastBoundary = spark.sparkContext.broadcast(spatialDF.getBoundaryEnvelope)

    name match { //todo add rtree!!!
      case "quadtree" => spatialDF.spatialPartitioning(GridType.QUADTREE)
      case "voronoi" => spatialDF.spatialPartitioning(GridType.VORONOI)
      case "hilbert" => spatialDF.spatialPartitioning(GridType.HILBERT)
    }


    //todo!!!
    val repartSchema = newSchema.add("partitionid", IntegerType)//.add("ogc", BinaryType)

    val repartEncoder = RowEncoder(repartSchema)

    val repart_rdd=spatialDF.getSpatialPartitionedRDD.rdd.mapPartitionsWithIndex((index, it) => {
      it.map(row=>{
        val seq = row.asInstanceOf[GenericRowWithSchema].toSeq
        val schema = row.asInstanceOf[GenericRowWithSchema].schema

        val newSeq = seq :+ index
        val newSchema = row.asInstanceOf[GenericRowWithSchema].schema.add("partitionid", IntegerType)

        val newRow: Row = new GenericRowWithSchema(newSeq.toArray, newSchema)

        newRow
      })
    })

    val mbbrdd=repart_rdd.mapPartitions(it=>{
      SpatialIndex.rtree(it.toArray, broadcastBoundary.value, broadcastrtree_nodeCapacity.value)
    })

    val partitionMBBDF = spark.createDataFrame(mbbrdd)
    val repartitionDF = spark.createDataFrame(repart_rdd, repartSchema)


        //newDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_raw_ogc" + "_" + output + "_parquet")
    partitionMBBDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_index" + "_" + output + "_parquet")
    repartitionDF.write.option("compression", "snappy").mode("overwrite").parquet("geospark_ogc_repartition" + "_" + output + "_parquet")


/**/

    spark.close()

    /*
    temp.foreach(x=>{
      if (x!=null) {
        val ogc=ReadPostgresGeom.dserialize(x.ogc_geom)
        println(ogc.asText())
        //println(ogc.getEsriSpatialReference.getID)
      }
    })

    println("Done!")
    */
  }
}
