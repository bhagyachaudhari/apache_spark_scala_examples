package example

import org.apache.spark.sql.SparkSession

object WriteToJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Write To Json").master("local[1]").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (12, "Alice", "20-09-2004", 20),
      (13, "Bob", "20-08-1998", 26),
      (14, "Charlie", "10-09-2001", 23),
      (15, "Danny", "02-09-1990", 34)
    ).toDF("id", "name", "dob", "age")

//    data.show()

    /*
    * Write Modes in Spark
    * overwrite
    * append
    * errorifexists or error : Default Mode
    * ignore
    * */

    data.write.mode(saveMode = "ignore").json("src/main/scala/target/json_output")

//    val dataDF = spark.read.option("header", value = true).json("src/main/scala/atomic/target/json_output")
//
//    dataDF.show()

    spark.stop()
  }

}
