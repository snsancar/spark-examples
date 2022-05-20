package org.shellinterview

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Try


object AppEngine {
  def create(appConfig: AppConfig)(implicit
                                   spark: SparkSession): Try[AppEngine] = {
    Try(new AppEngine(appConfig))
  }

  def createStreamingQuery(ds: Dataset[Emp], appConfig: AppConfig): StreamingQuery = {
    ds
      .writeStream
      .outputMode(OutputMode.Update())
      .options(Map("checkpointLocation" -> appConfig.application.checkPointPath))
      .foreach(new CustomForeachWriter())
      .start()
  }
}

case class Emp(id: Int, name: String)


class AppEngine(appConfig: AppConfig)(implicit
                                      spark: SparkSession) extends Serializable {
  def createStreamingQuery(): StreamingQuery = {

    val dsData = Emp(1, "shankar")

    import spark.implicits._
    val ds: Dataset[Emp] = spark.createDataset[Emp](Seq(dsData))
    //  val ds = spark.emptyDataset[DsSet]

    AppEngine.createStreamingQuery(ds, appConfig)
  }
}
