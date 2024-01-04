package kr.justkode.aggregator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object WatermarkAggregator {
  case class Event(adId: Long, eventId: Long, timestamp: Timestamp)

  def aggClick(df: DataFrame): DataFrame = {
    df.filter(col("eventId") === 1)
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("adId")
      )
      .count()
  }
}
