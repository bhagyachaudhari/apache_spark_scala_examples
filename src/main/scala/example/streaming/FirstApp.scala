package example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FirstApp extends App{

  /*val conf = new SparkConf().setAppName("SparkStreaming")
  val sc = new SparkContext(conf)*/
  val spark = SparkSession.builder().appName("Spark Streaming").master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))
  val streamRDD = ssc.socketTextStream("127.0.0.1", 2222)
  val wordCounts = streamRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
