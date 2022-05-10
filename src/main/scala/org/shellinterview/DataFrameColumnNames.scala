package org.shellinterview

import org.apache.spark.sql.{Column, SparkSession}

object DataFrameColumnNames extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Column Name")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._
  val df = Seq(
    (1, "shankar"),
    (2, "ramesh")).toDF("id", "name")

  val renameColnames = Map(
    "id" -> "empId",
    "name" -> "empName"
  )

  val dfColNamesUpdated = renameColnames.foldLeft(df)((acc, columnName) => acc.withColumnRenamed(columnName._1, columnName._2))
  dfColNamesUpdated.show()
}
