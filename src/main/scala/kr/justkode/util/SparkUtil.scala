package kr.justkode.util

import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 1)
      .master("local[2]")
      .getOrCreate()
  }
}
