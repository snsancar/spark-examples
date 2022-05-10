package org.shellinterview

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, from_unixtime, row_number, to_date, weekofyear}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object EventsUtil {

  case class Items(itemId: String, count: BigInt)

  def read_events(spark: SparkSession, filePath: String) = {

    spark.read
    .option("header", true)
    .csv("/Users/shankar/Desktop/Shell/archive/events.csv")
  }

  def enrichEvents(events: DataFrame) = {
     events
      .withColumn("date", to_date(
        from_unixtime(col("timestamp")/1000, "yyyy-MM-dd HH:mm:ss.SSSS")))
      .withColumn("week", weekofyear(to_date(
        from_unixtime(col("timestamp")/1000, "yyyy-MM-dd HH:mm:ss.SSSS"))))
  }

  def getTop10ItemsViewed(spark: SparkSession, eventsWithDate: DataFrame): Dataset[Items] = {
    import spark.implicits._
    val eventsGroupedPerView = eventsWithDate
      .filter(col("event") === "view")
      .groupBy("itemid").count()

    eventsGroupedPerView
      .orderBy(desc("count"))
      .limit(10).as[Items]
  }

  def getTop10ItemsViewedPerWeek(eventsWithDate: DataFrame) = {
    val window = Window.partitionBy("week").orderBy(desc("count"))

    eventsWithDate
      .filter(col("event") === "view")
      .groupBy("week", "itemid").count()
      .withColumn("row", row_number().over(window))
      .where(col("row") < 11 )
      .orderBy(asc("week"), asc("row"))
  }

}
