package io.klaytn.apps.transfer

import io.klaytn.apps.common.LastProcessedBlockNumber
import io.klaytn.apps.common.LoaderHelper.getBlock
import io.klaytn.model.Block
import io.klaytn.service.CaverService
import io.klaytn.utils.spark.KafkaStreamingHelper

object TransferToDBStreaming extends KafkaStreamingHelper {
  import TransferToDBStreamingDeps.service

  private val caverService = CaverService.of()

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .mapPartitions { iter =>
            val logSeq = iter.toSeq.filter(
              _.key().toLong >= LastProcessedBlockNumber
                .getLastProcessedBlockNumber())

            if (logSeq.nonEmpty) {
              LastProcessedBlockNumber.setLastProcessedBlockNumber(
                logSeq.map(_.key().toLong).max)
              getBlock(logSeq, "TransferToDB", caverService).iterator
            } else Seq.empty[Block].iterator
          }
          .foreachPartition { blocks =>
            blocks.foreach { block =>
              val eventLogs = block.toRefined._3
              if (eventLogs.nonEmpty) {
                service.insertTransferToMysql(eventLogs)
                service.procBurn(eventLogs)
                service.procWithdrawOrDeposit(eventLogs)
                service.procApprove(eventLogs)
                service.procUpdateTokenUri(eventLogs)
              }
            }
          }
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
