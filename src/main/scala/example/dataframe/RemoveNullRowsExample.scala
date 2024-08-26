package example.dataframe

import org.apache.spark.sql.SparkSession

object RemoveNullRowsExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val filePath="src/main/scala/data/csv_files/small_zipcode.csv"

  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(filePath)
  df.printSchema()
  df.show(false)

  df.na.drop().show(false)

  //all/any
  df.na.drop("any").show(false)

  df.na.drop(Seq("population","type")).show(false)

}
