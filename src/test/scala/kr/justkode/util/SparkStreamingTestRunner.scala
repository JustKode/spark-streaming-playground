package kr.justkode.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

trait SparkStreamingTestRunner extends AnyFlatSpec
  with Logging
  with BeforeAndAfter
  with BeforeAndAfterAll {

  Logger.getLogger("kr.justkode").setLevel(Level.INFO)

  val spark = SparkUtil.getSparkSession()

  import spark.implicits._

  implicit val ctx = spark.sqlContext

  val logs: MemoryStream[String] = MemoryStream[String]
  val memorySink = new MemorySink

  override def beforeAll(): Unit = {
    log.info("************Test Started************")


  }

  before {
    memorySink.clear()
  }
}
