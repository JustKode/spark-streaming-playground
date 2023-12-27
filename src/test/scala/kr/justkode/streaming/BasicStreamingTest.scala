package kr.justkode.streaming

import kr.justkode.aggregator.BasicAggregator.aggProductPerformanceReport
import kr.justkode.util.SparkStreamingTestRunner
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest._


class BasicStreamingTest extends SparkStreamingTestRunner {
  val schema = StructType(
    Seq(
      StructField("productId", DataTypes.LongType, true),
      StructField("userId", DataTypes.LongType, true),
      StructField("price", DataTypes.LongType, true),
      StructField("timestamp", DataTypes.TimestampType, true)
    )
  )

  before {
    logs.reset()
  }

  "result" should "400" in {
    logs.addData("""{"productId":1,"userId":1,"price":100,"timestamp":"2023-12-31T09:00:00.000Z"}""")
    logs.addData("""{"productId":1,"userId":2,"price":100,"timestamp":"2023-12-31T09:00:00.000Z"}""")
    logs.addData("""{"productId":1,"userId":3,"price":100,"timestamp":"2023-12-31T09:00:00.000Z"}""")
    logs.addData("""{"productId":1,"userId":4,"price":100,"timestamp":"2023-12-31T09:00:00.000Z"}""")

    val df = getDataFrameFromJsonRecordsBySchema(schema)
    val streamingQuery = aggProductPerformanceReport(df)
      .writeStream
      .format("memory")
      .queryName("agg")
      .outputMode("complete")
      .start()

    streamingQuery.processAllAvailable()

    val result = spark.sql("select * from agg").collectAsList().get(0).getAs[Long]("price")
    assert(result === 400)
  }
}
