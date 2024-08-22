package example

import org.apache.spark.sql.SparkSession

object FileWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSet API").master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    val file_path = "src/main/scala/data/text_files/data.txt"
    val file = sc.textFile(file_path)
    file.foreach(println)

    val wCount = file.flatMap(line => line.split(","))
    wCount.foreach(println)

    var mapOp = wCount.map(w => (w,1))
    mapOp.foreach(println)
  }
}