package kr.justkode.aggregator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicAggregator {
  def aggProductPerformanceReport(df: DataFrame): DataFrame = {
    df.groupBy(col("productId"))
      .agg(
        sum("price") as "price",
        count("*") as "sales"
      )
  }
}
