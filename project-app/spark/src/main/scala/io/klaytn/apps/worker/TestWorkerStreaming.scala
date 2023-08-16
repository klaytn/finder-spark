package io.klaytn.apps.worker

import io.klaytn.apps.common.TestDataAggregator
import io.klaytn.utils.s3.S3Util
import io.klaytn.utils.spark.KafkaStreamingHelper
import org.apache.spark.TaskContext

import java.nio.ByteBuffer

object TestWorkerStreaming extends KafkaStreamingHelper {
  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .foreachPartition { iter =>
            iter.foreach { r =>
              val totalSeg = ByteBuffer
                .wrap(r.headers().lastHeader("totalSegments").value())
                .getLong()
                .toInt
              val offset = r.offset()
              TestDataAggregator.set(TaskContext.getPartitionId(),
                                     r.key(),
                                     totalSeg,
                                     offset)
            }

            S3Util.writeText(
              "klaytn-dev-spark",
              s"output/test/object/${System
                .currentTimeMillis()}_TID_${Thread.currentThread().getId}_PID_${TaskContext.getPartitionId()}",
              TestDataAggregator.getAndClear()
            )
          }
      }
    }
  }
}
