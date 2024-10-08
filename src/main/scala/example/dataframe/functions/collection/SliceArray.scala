package example.dataframe.functions.collection

import org.apache.spark.sql.functions.{col, slice}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SliceArray extends App {


  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()

  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++","Pascal","Spark")),
    Row("Michael,Rose,",List("Spark","Java","C++","Scala","PHP")),
    Row("Robert,,Williams",List("CSharp","VB",".Net","C#.net",""))
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.show(false)
  df.printSchema()


  val splitDF2 = df.withColumn("languages",
    slice(col("languagesAtSchool"),2,3))
    .drop("languagesAtSchool")
  splitDF2.printSchema()
  splitDF2.show(false)

  df.createOrReplaceTempView("PERSON")
  spark.sql("select name, slice(languagesAtSchool,2,3) as NameArray from PERSON")
    .show(false)


}
