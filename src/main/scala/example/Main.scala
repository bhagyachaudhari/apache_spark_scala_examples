package example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("DataSet API").master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val rdd1 = sc.sparkContext.parallelize(Seq(("A", 1), ("B", 2), ("C", 3)))
    rdd1.foreach(println)

    val file_path = "src/main/scala/data/text_files/test.txt"
    val file = sc.sparkContext.textFile(file_path)
    file.foreach(println)

    val rdd2 = file.flatMap(_.split(" "))
    rdd2.foreach(println)

    val rdd3 = sc.range(20).toDF().rdd
    rdd3.foreach(println)
  }
}