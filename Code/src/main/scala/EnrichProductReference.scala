package main.scala
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object EnrichProductReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EnrichProductReference").master("local[*]")
      .getOrCreate()

    val config : Config = ConfigFactory.load("application.conf")
    val inputLocation = config.getString("paths.inputLocation")
    val outputLocation = config.getString("paths.outputLocation")

    // Reading valid data
    val validFileSchema  = StructType(List(
      StructField("Sale_ID", StringType, nullable = false),
      StructField("Product_ID", StringType, nullable = false),
      StructField("Quantity_Sold", IntegerType, nullable = false),
      StructField("Vendor_ID", StringType, nullable = false),
      StructField("Sale_Date", TimestampType, nullable = false),
      StructField("Sale_Amount", DoubleType, nullable = false),
      StructField("Sale_Currency", StringType, nullable = false)
    ))

    val productPriceReferenceSchema = StructType(List(
      StructField("Product_ID",StringType, nullable = true),
      StructField("Product_Name",StringType, nullable = true),
      StructField("Product_Price",IntegerType, nullable = true),
      StructField("Product_Price_Currency",StringType, nullable = true),
      StructField("Product_updated_date",TimestampType, nullable = true)
    ))

    val dateToday = LocalDate.now()
    val yesterday = dateToday.minusDays(1)

    val currDayZoneSuffix  = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    //val currDayZoneSuffix  = "_19072020"

    val validDataDF = spark.read
      .schema(validFileSchema)
      .option("delimiter", "|")
      .option("header", value = true)
      .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)
    validDataDF.createOrReplaceTempView("validData")

    //Reading Project Reference
    val productPriceReferenceDF = spark.read
      .schema(productPriceReferenceSchema)
      .option("delimiter", "|")
      .option("header", value = true)
      .csv(inputLocation + "Products")
    productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

    val productEnrichedDF = spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, " +
      "a.Quantity_Sold, a.Vendor_ID, a.Sale_Date, " +
      "b.Product_Price * a.Quantity_Sold AS Sale_Amount, " +
      "a.Sale_Currency " +
      "from validData a INNER JOIN productPriceReferenceDF b " +
      "ON a.Product_ID = b.Product_ID")

    productEnrichedDF.write
      .option("header", value = true)
      .option("delimiter","|")
      .mode("overwrite")
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)

  }
}
