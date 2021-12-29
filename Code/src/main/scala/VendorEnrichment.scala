package main.scala
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object VendorEnrichment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VendorEnrichment").master("local[*]")
      .getOrCreate()

    //paths
    val config : Config = ConfigFactory.load("application.conf")
    val inputLocation = config.getString("paths.inputLocation")
    val outputLocation = config.getString("paths.outputLocation")

    //Dates
    val dateToday = LocalDate.now()
    val currDayZoneSuffix  = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    //val currDayZoneSuffix  = "_19072020"


    val productEnrichedInputSchema = StructType(List(
      StructField("Sale_ID",StringType, nullable = true),
      StructField("Product_ID",StringType, nullable = true),
      StructField("Product_Name",StringType, nullable = true),
      StructField("Quantity_Sold",IntegerType, nullable = true),
      StructField("Vendor_ID",StringType, nullable = true),
      StructField("Sale_Date",TimestampType, nullable = true),
      StructField("Sale_Amount",DoubleType, true),
      StructField("Sale_Currency",StringType, true)
    ))

    val vendorReferenceSchema = StructType(List(
      StructField("Vendor_ID",StringType, true),
      StructField("Vendor_Name",StringType, true),
      StructField("Vendor_Add_Street",StringType, true),
      StructField("Vendor_Add_City",StringType, true),
      StructField("Vendor_Add_State",StringType, true),
      StructField("Vendor_Add_Country",StringType, true),
      StructField("Vendor_Add_Zip",StringType, true),
      StructField("Vendor_Updated_Date",TimestampType, true)
    ))

    val usdReferenceSchema = StructType(List(
      StructField("Currency", StringType, true),
      StructField("Currency_Code", StringType, true),
      StructField("Exchange_Rate", FloatType, true),
      StructField("Currency_Updated_Date", TimestampType, true)
    ))

    //Reading the required zones

    val productEnrichedDF = spark.read
      .schema(productEnrichedInputSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)
    productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

    val usdReferenceDF = spark.read
      .schema(usdReferenceSchema)
      .option("delimiter", "|")
      .csv(inputLocation + "USD_Rates")
    usdReferenceDF.createOrReplaceTempView("usdReferenceDF")

    val vendorReferenceDF = spark.read
      .schema(vendorReferenceSchema)
      .option("delimiter", "|")
      .option("header", false)
      .csv(inputLocation + "Vendors")
    vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")

    val vendorEnrichedDF = spark.sql("select a.*, b.Vendor_Name FROM " +
      "productEnrichedDF a INNER JOIN vendorReferenceDF b " +
      "ON a.Vendor_ID = b.Vendor_ID")
    vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

    val usdEnriched = spark.sql("select *, ROUND((a.Sale_Amount / b.Exchange_Rate),2) AS Amount_USD " +
      "from vendorEnrichedDF a JOIN usdReferenceDF b " +
      "ON a.Sale_Currency = b.Currency_Code")

    val reportDF = spark.sql("select Sale_ID, Product_ID, Product_Name, " +
      "Quantity_Sold, Vendor_ID, Sale_Date, Sale_Amount, " +
      "Sale_Currency, Vendor_Name, Amount_USD FROM usdEnriched")

    // Mysql connectivity

    /*reportDF.write.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://localhost:3306/gkcstoredb",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "finalsales",
        "user" -> "root",
        "password" -> "gkcodelabs"
      ))
      .mode("append")
      .save()*/







  }

}
