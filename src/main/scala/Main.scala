import org.apache.spark.sql.functions.{col, concat, lit, year}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Main{
  def main(args: Array[String]): Unit = {
    // Initialisation de SparkSession
    val spark = SparkSession.builder()
      .appName("sparkScala_demo")
      .master("local[*]")
      .getOrCreate()

    // Lecture des données depuis un fichier CSV
    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/AAPL.csv")

    // suppression des lignes contenant des valeurs nulles
    val cleanedDf = rawDf.na.drop()

    // Prétraitement : une nouvelle colonne indiquant l'année extrait d'une colonne de date
    val preprocessedDf = cleanedDf.withColumn("year", year(col("Date")))

    val snowflakeOptions = Map(
      "sfURL" -> "<your_snowflake_account_url>",
      "sfAccount" -> "<your_snowflake_account_name>",
      "sfUser" -> "<your_username>",
      "sfPassword" -> "<your_password>",
      "sfDatabase" -> "<your_database>",
      "sfSchema" -> "<your_schema>",
      "sfWarehouse" -> "<your_warehouse>",
      "sfRole" -> "<your_role>",
    )

    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    /* Écriture des données nettoyées dans un fichier Parquet
    preprocessedDf.write
      .mode(SaveMode.Overwrite)
      .parquet("data/preprocessed_data.parquet")


    spark.stop()
  /*def main(args: Array[String]) : Unit ={
    val spark= SparkSession.builder()
      .appName("sparkScala_demo")
      .master("local[*]")
      .config("spark.driver.bindAdress","127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header",value= true)
      .option("inferSchema", value=true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    /*df.select("Date","Open", "Close").show()
    val column=df("Date")
    col("Date")
    import spark.implicits._
    $"Date"

    df.select(col("Date"), $"Open", df("Close")).show() */

    val column= df("Open")
    val newcolumn= (column + 2.0).as("column increased")
    val stringcolumn= column.cast(StringType).as("column string")

    val litcolumn= lit(2.0)
    val newconcatcolumn= concat(column, lit("hello world"))

    df.select(column, newcolumn, stringcolumn, newconcatcolumn)
      /*.filter(newcolumn > 2.0)
      .filter(column === stringcolumn)*/
      .show(truncate =false )*/


*/

  }
}