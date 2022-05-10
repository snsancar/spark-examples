package org.shellinterview

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.slf4j.LoggerFactory

object StreamingApp extends App {

  private val log = LoggerFactory.getLogger(getClass.getName)

  lazy implicit val spark: SparkSession = SparkSession.builder()
    .appName("Structured Streaming")
    .getOrCreate()


  def setSparkExecutorLogLevel(spark: SparkSession, level: Level): Unit = {
    spark.sparkContext.parallelize(Seq("")).foreachPartition(_ => {
      LogManager.getRootLogger().setLevel(level)
    })
  }

  val conf = ConfigFactory.load()
  setSparkExecutorLogLevel(spark, Level.WARN)

  case class Emp(id: Int, name: String)
  val dsData = Emp(1, "shankar")

  import spark.implicits._
  val ds = spark.createDataset[Emp](Seq(dsData))
//  val ds = spark.emptyDataset[DsSet]

  val query: StreamingQuery = ds
    .writeStream
    .outputMode(OutputMode.Update())
    .options(Map("checkpointLocation" -> conf.getString("check-point-path")))
    .foreach(new CustomForeachWriter())
    .start()

}
