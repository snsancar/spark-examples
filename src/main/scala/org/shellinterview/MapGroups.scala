package org.shellinterview

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object MapGroups extends App {

  val spark = SparkSession
    .builder()
    .appName("Test")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._

  case class Sources(id: Int, serialNumber: String, source: String, company: String)

  def groupingkey(sources: Sources): Int = {
    sources.id
  }

  val ds = Seq((1,"123ABC", "system1", "Acme"),
    (7,"123ABC", "system2", "Acme"))
    .toDF("id", "serialNumber", "source", "company")

  ds.groupBy("serialNumber")
    .agg(
      collect_list("id").alias("id"),
      collect_list("source").alias("source"),
      collect_set("company").alias("company")
    )
    .show(false)



}
