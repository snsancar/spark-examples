package org.shellinterview

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMain extends App {

  val spark = SparkSession
    .builder()
    .appName("spark-examples")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._


  val events = spark.read
    .option("header", true)
    .csv("/Users/shankar/Desktop/Shell/archive/events.csv")

}
