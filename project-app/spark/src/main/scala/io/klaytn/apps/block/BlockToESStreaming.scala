package io.klaytn.apps.block

import io.klaytn.apps.common.LastProcessedBlockNumber
import io.klaytn.apps.common.LoaderHelper.getBlock
import io.klaytn.service.CaverService
import io.klaytn.utils.spark.KafkaStreamingHelper

object BlockToESStreaming extends KafkaStreamingHelper {
  import BlockToESStreamingDeps._

  private val caverService = CaverService.of()

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .foreachPartition { iter =>
            val logSeq = iter
              .filter(
                _.key().toLong >= LastProcessedBlockNumber
                  .getLastProcessedBlockNumber())
              .toSeq
            if (logSeq.nonEmpty) {
              getBlock(logSeq, "BlockToES", caverService)
                .foreach(processor.process)
              LastProcessedBlockNumber.setLastProcessedBlockNumber(
                logSeq.map(_.key().toLong).max)
            }
          }
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
