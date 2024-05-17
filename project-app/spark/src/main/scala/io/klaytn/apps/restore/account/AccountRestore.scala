package io.klaytn.apps.restore.account

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper}

object AccountRestore extends KafkaStreamingHelper {
  import AccountRestoreDeps._

  def restoreAccount(id: Long, limit: Long): Unit = {

    val refinedBlocks = blockPersistentAPI.getBlocksByRange(id, id + limit)
    val refinedTransactionReceipts = transactionPersistentAPI
      .getTransactionReceiptsByBlockRange(id, id + limit)
    val txByBlock = refinedTransactionReceipts.groupBy(_.blockNumber)
    val refinedEventLogs =
      eventLogPersistentAPI.getEventLogsByBlockRange(id, id + limit)
    val eventsByBlock = refinedEventLogs.groupBy(_.blockNumber)

    // Seq[(RefinedBlock, List[RefinedTransactionReceipt], List[RefinedEventLog])]
    val blocks = refinedBlocks.map { block =>
      val txs = txByBlock.getOrElse(block.number, List.empty)
      val events = eventsByBlock.getOrElse(block.number, List.empty)
      (block, txs.toList, events.toList)
    }.toSeq
    val blocksRDD = sc.parallelize(blocks, 50)
    accountService.processRefined(blocksRDD, 50)
  }
  override def run(args: Array[String]): Unit = {
    val cnt = 100000
    0L to 1500L foreach { x =>
      val t1 = System.currentTimeMillis()
      restoreAccount(x * cnt, cnt)
      try {
        SlackUtil.sendMessage(
          s"RestoreAccount: ${x * cnt} ~ ${x * cnt + cnt} is done. ${System
            .currentTimeMillis() - t1} ms, ${chainPhase}"
        )
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    }
  }
}
