package example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Simple {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Working with Spark Streaming").master("local[1]").getOrCreate()

    val initDF = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

    println("Streaming DF: " + initDF.isStreaming)

    val resultDF = initDF.withColumn("result", col("value")+lit(1))

    resultDF.writeStream.outputMode("append").option("truncate", false).format("console").start().awaitTermination()
  }
}
