package org.apache.spark.sql

import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.util.ManualClock

object StreamingTestUtil {
  def getStreamingQuery(df: DataFrame,
                        clock: ManualClock,
                        sink: MemorySink,
                        checkpoint: String,
                        outputMode: OutputMode): StreamingQuery = {
    df.sparkSession
      .streams
      .startQuery(
        userSpecifiedName = Some("spark-structured-streaming-unit-test"),
        userSpecifiedCheckpointLocation = Some(checkpoint),
        df = df,
        extraOptions = Map[String, String](),
        sink = sink,
        outputMode = outputMode,
        recoverFromCheckpointLocation = false,
        triggerClock = clock
      )
  }

  def getClock(time: Long): ManualClock = {
    new ManualClock(time)
  }
}
