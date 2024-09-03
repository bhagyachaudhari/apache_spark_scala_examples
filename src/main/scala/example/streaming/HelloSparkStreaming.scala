package example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object HelloSparkStreaming{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\bhagy\\Desktop\\hadoop")
    val sparkSession = SparkSession.builder().appName("Hello Spark Streaming").master("local[*]").getOrCreate()
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("eventTime", TimestampType, nullable = false),
      StructField("clickedResource", StringType, nullable = false)
    ))
    val df = sparkSession.readStream.schema(schema).json("src/main/scala/data/json_files/")
    df.printSchema()

    df
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }

}