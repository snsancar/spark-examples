package org.shellinterview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, to_timestamp}
import org.apache.spark.sql.types.{LongType, TimestampType}

object JsonReading extends App {

  val spark = SparkSession
    .builder()
    .appName("Test")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._

//  val jsonString = """"[{"name":"a","info":{"age":"1","grade":"b"},"other":7},{"random":"x"}, {...}]"""".stripMargin
//  val df2 = spark.read.json(Seq(jsonString).toDS)

//  val data = Seq(("1/1/2017 0:00", "1/1/2017 0:35"))
//
//  val df = data.toDF("pickup_dt", "drop_dt")
//
//  df
//    .withColumn("pickup_dt", to_timestamp(col("pickup_dt"), "d/M/yyyy H:mm"))
//    .withColumn("drop_dt", to_timestamp(col("drop_dt"), "d/M/yyyy H:mm"))
//    .withColumn("diff", (col("drop_dt").cast(LongType) - col("pickup_dt").cast(LongType)) / 60)
//    .show(false)

//  | Id |  count |
//  | 0  |   5    |
//    | 1  |   3    |
//    | 4  |   6    |

  val data = Seq((0, 5), (1, 3), (4, 6))
  val df = data.toDF("Id", "count")

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.Window

  val w = Window.partitionBy($"id")
    .rowsBetween(Window.unboundedPreceding, -1)

  df
    .select( min("Id").alias("minId"), max("Id").alias("maxId"))
    .withColumn("allIds", sequence(col("minId"), col("maxId")))
    .show(false)

}
