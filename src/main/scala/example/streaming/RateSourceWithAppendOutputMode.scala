package example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object RateSourceWithAppendOutputMode {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\bhagy\\Desktop\\hadoop")
    val spark =
      SparkSession
        .builder()
        .appName("Rate Source with Append Output Mode")
        .master("local[*]")
        .getOrCreate()

    val df = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 3)
      .load()

    import spark.implicits._

    df.printSchema()

    val query = df
      .withColumn("double_value", $"value" * 2)
      .withColumn("triple_value", $"value" * 3)
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .option("truncate", value = false)
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}
