import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.Date
import scala.collection.mutable.ListBuffer

object Datagen extends App {
  val startDate = "2012-05-10"
  val endDate = "2022-12-17"
  val numberOfUsers = 10
  val numberOfRecords = 20
  val metricsSize = 5
  val upperMetricBound = 10000.0
  val allItemsValue = "*"
  val dimensions = List(
    ("cities", Array("New York", "Chicago", "San Francisco", "Mexico", "Kyiv")),
    ("accountSettings", Array("private", "public", "default")),
    ("genders", Array("female", "male", "gen1", "gen2", "gen0", "gen53", "unknown")),
    ("countries" -> Array("Ukraine", "GB", "US", "UK", "UAE")),
    ("browsers" -> Array("Opera", "Chrome", "Safari", "IE", "Thor"))
  )


  val randomizer = new scala.util.Random
  val dimSparkColumns: List[StructField] = dimensions.map(_._1).map(name => StructField(name, StringType))
  val partitionColumns: List[StructField] = List(StructField("user_id", StringType), StructField("date", LongType))
  val finalSchema = StructType(partitionColumns ::: dimSparkColumns ::: List(
    StructField("count", DoubleType),
    StructField("unique_count", DoubleType),
    StructField("sum", DoubleType),
    StructField("median", DoubleType),
    StructField("avg", DoubleType)))


  def prepareDims(dims: List[(String, Array[String])]) = {
    val dimensionCombs = ListBuffer[List[Any]]()
    for (_ <- 1 to dims.size){
      val dimKeys = dims.take(randomizer.nextInt(dimensions.size)).map(_._2).map(v => v.apply(randomizer.nextInt(v.length)))
      val metrics = Array.fill(randomizer.nextInt(metricsSize))(randomizer.nextDouble() * upperMetricBound).toList.map(d => d.floor)
      dimensionCombs += (
        dimKeys ::: metrics
        )
    }

    serialise(dimensionCombs.toList)
  }

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value
  }

  def fillMissedElements(blobElements: List[Any]) = {
    val currentMetrics = blobElements.collect{ case a: Double => a }
    val currentDims = blobElements.collect{ case p: String => p }
    val prepDimesnions = currentDims ++ Array.fill(dimensions.size - currentDims.size)(allItemsValue)
    val prepMetrics = currentMetrics ++ Array.fill(metricsSize - currentMetrics.size)(0.0)
    prepDimesnions ::: prepMetrics
  }

  def prep(user_id: String, ts: Long, encodedDimensions: List[List[Any]]) = {
    val filledBlobs = encodedDimensions
      .map(blob => fillMissedElements(blob))
    filledBlobs
      .map(blobList => List(user_id, ts) ::: blobList)
  }

  def generateData(
                    startDate: String,
                    endDate: String,
                    numberOfUsers: Int = 100,
                    numberOfRecords: Int = 1000,
                  )
  = {
    val startEpoch = Date.valueOf(startDate).getTime
    val endEpoch = Date.valueOf(endDate).getTime
    val epochDifference = endEpoch - startEpoch

    val userList = (1 to numberOfUsers).toList.map { u => "user-" + u }
    userList.flatMap { user =>
      (1 to numberOfRecords)
        .map { _ =>
          val randomTimestamp = startEpoch + (randomizer.nextDouble() * epochDifference).toLong
          val blobs = prepareDims(dimensions)
          (user, randomTimestamp, blobs)
        }
    }
  }

  def parseSerializedRecords(dataTuple: List[(String, Long, Array[Byte])]) = {
    dataTuple
      .map(l => (l._1, l._2, deserialise(l._3)))
      .flatMap { case (s: String, l: Long, a: List[List[String]]) =>
        prep(s, l, a)
      }
  }

  def generateDF(sparkSession: SparkSession, recordsRDD: RDD[Row]) = {
      sparkSession.createDataFrame(recordsRDD, finalSchema)
  }

  def generateRDD(sparkSession: SparkSession, parsedRecords: List[List[Any]])= {
    sparkSession.sparkContext.parallelize(parsedRecords).map(r => Row.fromSeq(r))
  }


  def run() = {
    val clientRecords = generateData(startDate, endDate, numberOfUsers, numberOfRecords)
//    clientRecords.take(20).foreach(println)

    val parsedRecords = parseSerializedRecords(clientRecords)
//    parsedRecords.take(25).foreach(println)
//
    val sparkSession = SparkSession.builder
      .appName("datagen")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .getOrCreate()
//
    val rdd = generateRDD(sparkSession, parsedRecords)
//    rdd.take(5).foreach(println)
//
    val df = generateDF(sparkSession, rdd)
  }
  run()
}
