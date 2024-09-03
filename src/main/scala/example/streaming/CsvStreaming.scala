/*
package example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CsvStreaming extends App {

    /*val conf = new SparkConf().setAppName("SparkStreaming")
    val sc = new SparkContext(conf)*/
    val spark = SparkSession.builder().appName("Spark Streaming").master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    // Read text from socket
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    socketDF.isStreaming // Returns True for DataFrames that have streaming sources

    socketDF.printSchema

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("C:\\tmp\\spark_output\\csv") // Equivalent to format("csv").load("/path/to/directory")
}*/
