ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "sparkScala_demo"
  )

val  SparkVersion = "3.5.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "net.snowflake" % "snowflake-jdbc" % "3.13.3",
  "net.snowflake" %% "spark-snowflake" % "2.9.1-spark_2.4"
)



