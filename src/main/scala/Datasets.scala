import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Datasets extends App {
  val sparkSession = SparkSession.builder
    .appName("datasets")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate()

  val schema = StructType( Array(
    StructField("street", StringType,true),
    StructField("zip", IntegerType, true),
    StructField("price", IntegerType, true)
  ))

  val rawListings1 = Seq(
    Row("strrt1", 12323, 12),
    Row("strret1", 123, 12),
    Row("strrt2", 13, 1200)
  )

  val rawListings = Seq(("strrt1", 12323, 12), ("strret1", 123, 12), ("strrt2", 13, 1200), ("strret1", 123, 120))
  val df = sparkSession.createDataFrame(rawListings)

  case class Listing(street: String, zip: Int, price: Int)
  val listingSchema = Encoders.product[Listing].schema
  listingSchema.printTreeString()
  val listingsCaseSeq = Seq(Listing("strrt1", 12323, 12), Listing("strret1", 123, 12),
    Listing("strrt2", 13, 1200), Listing("strreet01", 123, 120), Listing("strrt2", 13, 600))
  val df1 = sparkSession.createDataFrame(listingsCaseSeq)

  df.show()
  df1.show()

//  df1.groupBy("zip").avg("price").show()
//
  import sparkSession.implicits._
  val ds1: Dataset[Listing]= df1.as[Listing]
  ds1
    .groupByKey(l => l.zip)
    .agg(avg("price").as[Double]).show()


}
