package kr.justkode.util

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.util.TimeZone

trait SparkStreamingTestRunner extends AnyFlatSpec
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Logging {

  Logger.getLogger("kr.justkode").setLevel(Level.INFO)

  val spark = SparkUtil.getSparkSession()

  import spark.implicits._

  implicit val ctx = spark.sqlContext

  spark.sparkContext.setLogLevel("WARN")

  val checkpointLocation = "/tmp/spark-structured-streaming-unit-test"
  val logs: MemoryStream[String] = MemoryStream[String]
  val memorySink = new MemorySink

  override def beforeAll(): Unit = {
    log.info("************Test Started************")
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  override def afterAll(): Unit = {
    log.info("************Test Ended************")
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  protected def getDataFrameFromJsonRecordsBySchema(schema: StructType): DataFrame = {
    logs.toDF()
      .select(from_json(col("value"), schema) as "data")
      .select("data.*")
  }

  protected def getTimestampMs(datetime: String): Long = {
    val timestampUsec: Long = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString(datetime),
      TimeZone.getTimeZone("KST").toZoneId
    ).get

    timestampUsec / 1000
  }
}
