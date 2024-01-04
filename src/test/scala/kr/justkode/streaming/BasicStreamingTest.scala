package kr.justkode.streaming

import kr.justkode.aggregator.BasicAggregator.aggProductPerformanceReport
import kr.justkode.util.SparkStreamingTestRunner
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

class BasicStreamingTest extends SparkStreamingTestRunner {
  val schema = StructType(
    Seq(
      StructField("productId", DataTypes.LongType, true),
      StructField("userId", DataTypes.LongType, true),
      StructField("price", DataTypes.LongType, true),
      StructField("timestamp", DataTypes.TimestampType, true)
    )
  )

  after {
    logs.reset()
    FileUtils.deleteDirectory(new File(checkpointLocation))
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

  "test" should "non-exception" in {
    logs.addData("""{"id": 1, "content": "hello"}""")
    logs.addData("""{"id": 2, "content": "hello!"}""")
    logs.addData("""{"id": 3, "content": "hello!!"}""")
    logs.addData("""{"id": 3, "co""")

    val schema = StructType(
      Seq(
        StructField("id", DataTypes.LongType, true),
        StructField("content", DataTypes.StringType, true)
      )
    )

    val df = logs.toDF()
      .select(from_json(col("value"), schema) as "data")
      .select("data.*")

    val streamingQuery = df.writeStream
      .format("memory")
      .queryName("agg2")
      .outputMode("append")
      .start()

    streamingQuery.processAllAvailable()

    val result = spark.sql("select * from agg2").collect()
    info(rowListToString(result.toSeq))
  }
}
