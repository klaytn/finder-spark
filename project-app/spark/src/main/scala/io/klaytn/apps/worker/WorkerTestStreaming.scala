package io.klaytn.apps.worker

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.StreamingHelper
import org.apache.spark.TaskContext

object WorkerTestStreaming extends StreamingHelper {
  override def run(args: Array[String]): Unit = {
    val stream = ssc.receiverStream(new WorkerMockReceiver(3))
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.repartition(3).foreachPartition { data =>
          TaskContext.getPartitionId() match {
            case 0 => SlackUtil.sendMessage(s"test # 0: ${data.mkString(",")}")
            case 1 => SlackUtil.sendMessage(s"test # 1: ${data.mkString(",")}")
            case 2 => SlackUtil.sendMessage(s"test # 2: ${data.mkString(",")}")
          }
        }
      }
    }
  }
}
