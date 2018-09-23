package spatial.partition

import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.io.{ParseException, WKTReader}
import dbis.stark.STObject
import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner, SpatialPartitioner}
import org.apache.spark.sql.{Row, SparkSession}
import index.SpatialIndex
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform
import utils.{OGCtrasnformations, ReadPostgresGeom}

object StarkPartitioner {
  def main(args: Array[String]): Unit = {
    /*
    val spark = args(1) match {
      case "cluster" => SparkSession.builder
        .appName("StarkPartitioner")
        .getOrCreate()
      case _ => SparkSession.builder
        .appName("StarkPartitioner")
        .master("local[*]")
        .getOrCreate()
    }
*/

    val spark = SparkSession.builder
        .appName("StarkPartitioner")
        .getOrCreate()
/*
    val input: InputStream = new FileInputStream(System.getProperty("user.dir") + "/config/stark_storeHDFS.properties")

    val prop: Properties = new Properties()
    prop.load(input)

    val host = prop.getProperty("host")
    val user = prop.getProperty("user")
    val pass = prop.getProperty("pass")
    val dbtable = prop.getProperty("dbtable")
    val output = prop.getProperty("output")
    val file_output = prop.getProperty("filetype")
    val partitionsPerDimension = prop.getProperty("partitionsPerDimension")
    val sideLength = prop.getProperty("sideLength")
    val maxCostPerPartition = prop.getProperty("maxCostPerPartition")
    val path = prop.getProperty("path")
    val datatype = prop.getProperty("datatype")
    val srid = prop.getProperty("srid").toInt
    val rtree_nodeCapacity = prop.getProperty("localIndex_nodeCapacity").toInt
    val name = prop.getProperty("name")
*/


  val prop=spark.sparkContext.getConf
    val host = prop.get("spark.host")
    val user = prop.get("spark.user")
    val pass = prop.get("spark.pass")
    val dbtable = prop.get("spark.dbtable")
    val output = prop.get("spark.output")
    val file_output = prop.get("spark.filetype")
    val partitionsPerDimension = prop.get("spark.partitionsperdimension")
    val sideLength = prop.get("spark.sidelength")
    val maxCostPerPartition = prop.get("spark.maxcostperpartition")
    val path = prop.get("spark.path")
    val datatype = prop.get("spark.datatype")
    val srid = prop.get("spark.srid").toInt
    val rtree_nodeCapacity = prop.get("spark.localindex_nodecapacity").toInt
    val name = prop.get("spark.name")
/* */

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
              log.warn("mapping: " + row.toString + "\t" + row.fieldIndex("geom"))
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
          log.warn("mapping: " + row.toString + "\t" + row.fieldIndex("geom"))

          val newSeq = seq :+ "" :+ Array[Byte]()
          val newSchema = row.schema.add("geom2Text", StringType).add("ogc", BinaryType)

          val newRow: Row = new GenericRowWithSchema(newSeq.toArray, newSchema)

          newRow
        case e: NullPointerException =>
          @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
          log.warn("mapping: " + row.toString + "\t" + geom2Text)

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

    val rdd = newDF.rdd
      .map(row => {
        val index = row.fieldIndex("geom2Text")
        (STObject.apply(row.get(index).toString), row)
      })


    val minMax = SpatialPartitioner.getMinMax(rdd)
    val env = new Envelope(minMax._1, minMax._2, minMax._3, minMax._4)
    val broadcastBoundary = spark.sparkContext.broadcast(env)


    val partitioner = name match {
      case "grid" => new SpatialGridPartitioner(rdd, partitionsPerDimension = partitionsPerDimension.toInt, false, minMax, dimensions = 2)
      case "bsp" => new BSPartitioner(rdd, sideLength = sideLength.toDouble, maxCostPerPartition = maxCostPerPartition.toInt)
    }

    val repartSchema = newSchema.add("partitionid", IntegerType)//.add("ogc", BinaryType)

    val repartEncoder = RowEncoder(repartSchema)

    val repart_rdd=rdd.partitionBy(partitioner).values.mapPartitionsWithIndex((index, it) => {
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


    partitionMBBDF.na.drop.write.option("compression", "snappy").mode("overwrite").parquet("geospark_index" + "_" + output + "_parquet")
    repartitionDF.write.option("compression", "snappy").mode("overwrite").parquet("geospark_ogc_repartition" + "_" + output + "_parquet")

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
