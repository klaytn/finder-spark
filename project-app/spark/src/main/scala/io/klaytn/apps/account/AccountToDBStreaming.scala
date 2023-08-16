package io.klaytn.apps.account

import io.klaytn.apps.common.LastProcessedBlockNumber
import io.klaytn.apps.common.LoaderHelper.getBlock
import io.klaytn.model.Block
import io.klaytn.service.CaverService
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}

object AccountToDBStreaming extends KafkaStreamingHelper {
  import AccountToDBStreamingDeps.service

  private val caverService = CaverService.of()

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val logRDD = rdd.mapPartitions { iter =>
          val logSeq = iter.toSeq.filter(
            _.key().toLong >= LastProcessedBlockNumber
              .getLastProcessedBlockNumber())

          if (logSeq.nonEmpty) {
            LastProcessedBlockNumber.setLastProcessedBlockNumber(
              logSeq.map(_.key().toLong).max)
            getBlock(logSeq, "AccountDB", caverService).iterator
          } else Seq.empty[Block].iterator
        }

        if (!logRDD.isEmpty()) {
          val numPartitions = UserConfig.numInstances * UserConfig.numCores
          service.process(logRDD, numPartitions)
        }
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
