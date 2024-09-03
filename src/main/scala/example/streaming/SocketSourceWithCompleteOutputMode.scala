package example.streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

/**
 *
 * PS C:\Program Files (x86)\Nmap> .\ncat.exe -lk 8888
hello world
world is beautiful
hi
 */
object SocketSourceWithCompleteOutputMode {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\bhagy\\Desktop\\hadoop")
    val spark =
      SparkSession
      .builder()
        .appName("Socket Source Complete Output Mode")
        .master("local[*]")
        .getOrCreate()

    val df: DataFrame = spark
      .readStream
      .format("socket")
      .options(Map("host" -> "localhost", "port" -> "8888"))
      .load()

    df.printSchema()

    import spark.implicits._

    val ds: Dataset[String] = df.as[String]

    val wordCountDf: DataFrame = ds
        .flatMap(_.split(" "))
        .groupBy("value")
        .count()

    val query: StreamingQuery = wordCountDf
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }
}
