package org.shellinterview

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

trait TestSparkSession extends BeforeAndAfterAll {
  this: Suite =>
  @transient private var _spark: SparkSession = _
  final implicit lazy val spark: SparkSession = _spark

  def createSparkSession(): SparkSession =
     SparkSession
      .builder()
      .appName("Test")
      .master("local[2]")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    override def beforeAll(): Unit = {
      super.beforeAll()
      _spark = createSparkSession()
    }

    override def afterAll(): Unit =
      if (_spark != null) {
        Try(_spark.close())
        _spark = null
      }
}