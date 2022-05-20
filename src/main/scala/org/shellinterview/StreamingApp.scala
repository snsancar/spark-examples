package org.shellinterview

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.Try

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

  for {
    conf         <- Try(ConfigFactory.load().resolve())
    appConfig    <- AppConfig.fromConfig(conf)
    appEngine    <- AppEngine.create(appConfig)
    streamingQuery = appEngine.createStreamingQuery()
  } yield streamingQuery.awaitTermination()

}
