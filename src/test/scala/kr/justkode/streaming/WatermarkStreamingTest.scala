package kr.justkode.streaming

import kr.justkode.aggregator.WatermarkAggregator.{Event, aggClick}
import kr.justkode.util.SparkStreamingTestRunner
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.StreamingTestUtil.{getClock, getStreamingQuery}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.streaming.OutputMode

import java.io.File
import java.util.Calendar

class WatermarkStreamingTest extends SparkStreamingTestRunner {
  val schema = caseClassToStructType[Event]

  after {
    logs.reset()
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  "row count / sum of imp" should "2, 10 / 2, 14" in {
    val currentTime = Calendar.getInstance()
    currentTime.set(2024, 0, 1, 0, 0)

    logs.addData(caseClassObjectToJson(Event(1, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(1, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(2, 0, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(2, 0, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(3, 0, calenderToTimestamp(currentTime))))
    logs.addData("{asdfd}")

    val df = getDataFrameFromJsonRecordsBySchema(schema)
    val sink = new MemorySink
    val clock = getClock(currentTime.getTimeInMillis)

    val streamingQuery = getStreamingQuery(aggClick(df), clock, sink, checkpointLocation, OutputMode.Update)
    streamingQuery.processAllAvailable()

    info("=== 1 ===")
    info(rowListToString(sink.allData))

    assert(sink.allData.size == 2)
    assert(sink.allData.head.getAs[Long]("count") == 2L)

    clock.advance(1000 * 60 * 5)
    currentTime.add(Calendar.MINUTE, 5)

    logs.addData(caseClassObjectToJson(Event(1, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(2, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(3, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(4, 1, calenderToTimestamp(currentTime))))

    currentTime.add(Calendar.MINUTE, -22)
    logs.addData(caseClassObjectToJson(Event(1, 1, calenderToTimestamp(currentTime))))
    logs.addData(caseClassObjectToJson(Event(2, 1, calenderToTimestamp(currentTime))))
    streamingQuery.processAllAvailable()

    info("=== 2 ===")
    info(rowListToString(sink.allData))

    assert(sink.allData.size == 10)
    assert(sink.allData.foldLeft(0L)((x, y) => x + y.getAs[Long]("count")) == 14L)
  }
}
