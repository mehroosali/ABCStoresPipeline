package main.scala
import org.apache.spark.sql.types._

object ABCFunctions {
  def read_schema(schema_arg : String): StructType = {
    var sch : StructType = new StructType
    val split_values = schema_arg.split(",").toList

    val d_types = Map(
      "StringType" -> StringType,
      "IntegerType" -> IntegerType,
      "TimestampType" -> TimestampType,
      "DoubleType" -> DoubleType
    )

    for(i <- split_values){
      val columnVal = i.split(" ").toList
      sch = sch.add(columnVal.head, d_types(columnVal(1)), nullable = true)
    }
    sch

  }

}
