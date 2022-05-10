package org.shellinterview

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

object SparkColumnUDF extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Column UDF")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

  import spark.implicits._

  val df = List(
    ("I LIKE food"),
    ("   this    fun")
  ).toDF("words")
    .withColumn("clean_words", removeAllWhitespace(col("words")))
    .withColumn("clean_words1", regexp_replace(col("words"), "\\s+", ""))
  df.show()


}
