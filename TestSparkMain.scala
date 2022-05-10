package org.shellinterview

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util
import org.shellinterview.EventsUtil


class TestSparkMain extends AnyWordSpec with Matchers with TestSparkSession {

  "Test.run" should {
    "succeed" in {

      import spark.implicits._
      val events = Seq(("1433221332117","257597","view","355908",""),
        ("1433221332117","257598","view","355908",""),
        ("1433221332117","257598","view","355908",""),
        ("1433221332117","257597","view","355909",""),
        ("1433221332117","257597","view","355910",""))
        .toDF("timestamp","visitorid","event","itemid","transactionid")

      val enrichedEvents = EventsUtil.enrichEvents(events)

      val top10ItemsDS = EventsUtil.getTop10ItemsViewed(spark, enrichedEvents)
      val topMostItemId = top10ItemsDS.limit(1).collect().map(_.itemId)
      topMostItemId should be equals "355908"

      val top10Items = top10ItemsDS.map(_.itemId).collectAsList()
      val expectedTop10Items = List("355908", "355909", "355910")

      top10Items should contain theSameElementsInOrderAs expectedTop10Items
    }
  }
}
