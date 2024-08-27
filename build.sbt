ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "HelloScalaProject"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.2"
// https://mvnrepository.com/artifact/com.databricks/spark-xml
//libraryDependencies += "com.databricks" %% "spark-xml" % "0.2.0"
// https://mvnrepository.com/artifact/com.thoughtworks.xstream/xstream
//libraryDependencies += "com.thoughtworks.xstream" % "xstream" % "1.4.17"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.2"

