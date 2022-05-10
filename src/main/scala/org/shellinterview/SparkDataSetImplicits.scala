package org.shellinterview

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkDataSetImplicits extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Dataset Implicits")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._

  case class Emp(id: Int, name: String)
  val ds = Seq((1, "shankar")).toDF("id", "name").as[Emp]
  ds.checkPointOnlyForBatchDS()

  /** Add a decorator when the dataset is not streaming */
  implicit class DatasetImpl[T](dataset: Dataset[T]) extends AnyRef {
    def checkPointOnlyForBatchDS(): Dataset[T] = if (dataset.isStreaming) dataset else dataset.checkpoint()
  }
}
