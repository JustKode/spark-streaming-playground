package kr.justkode.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.apache.spark.sql.catalyst.ScalaReflection

import java.io.File
import java.sql.Timestamp
import java.util.Calendar

trait SparkStreamingTestRunner extends AnyFlatSpec
  with BeforeAndAfter
  with BeforeAndAfterAll {

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  val spark = SparkUtil.getSparkSession()

  import spark.implicits._

  implicit val ctx = spark.sqlContext

  spark.sparkContext.setLogLevel("WARN")

  val checkpointLocation = "/tmp/spark-structured-streaming-unit-test"
  val logs: MemoryStream[String] = MemoryStream[String]
  val memorySink = new MemorySink

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(checkpointLocation))
  }

  protected def getDataFrameFromJsonRecordsBySchema(schema: StructType): DataFrame = {
    logs.toDF()
      .select(from_json(col("value"), schema) as "data")
      .select("data.*")
  }

  protected def caseClassObjectToJson[T](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  protected def caseClassObjectToJson[T](objList: Seq[T]): String = {
    objList.foldLeft("")((x, y) => x + mapper.writeValueAsString(y) + '\n').trim
  }

  protected def caseClassToStructType[T: scala.reflect.runtime.universe.TypeTag]: StructType = {
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
  }

  protected def calenderToTimestamp(calender: Calendar): Timestamp = {
    new Timestamp(calender.getTimeInMillis / 1000)
  }

  protected def rowListToString(rows: Seq[Row]): String = {
    rows.foldLeft("")((x, row) => x + row.toString() + '\n').trim
  }
}
