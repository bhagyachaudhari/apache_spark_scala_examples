package example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSetAPI {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSet API").master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val file_path = "src/main/scala/data/csv_files/data.csv"

    val options = Map("header" -> "true",
      "delimiter" -> "|",
      "inferSchema" -> "true")

    val df: DataFrame = spark.read
      .options(options)
      .csv(path = file_path)

    df.show()
    df.printSchema()

   /* df.select("id", "name").show()
    val column = df("dob")
    col("dob")
    import spark.implicits._
    $"dob"
    df.select(col("dob"), $"dob").show()*/
    // Column Functions.. col, lit, concat

    df.createTempView("df")
    spark.sql("select * from df").show()

    //SQL expression

    spark.stop()
  }

}

