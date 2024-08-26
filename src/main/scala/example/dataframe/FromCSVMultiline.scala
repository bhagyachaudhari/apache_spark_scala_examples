package example.dataframe

import org.apache.spark.sql.SparkSession

object FromCSVMultiline extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()


  val df = spark.read
    .option("header",true)
    .option("delimiter",",")
    .option("multiLine",true)
    .option("quotes","\"")
    .csv("src/main/scala/data/csv_files/address-multiline.csv")

  df.show(false)
}









