package io.klaytn.apps.restore.transaction

import io.klaytn.apps.restore.bulkload.BulkLoadHelper
import io.klaytn.dsl.db.withDB
import io.klaytn.utils.spark.SparkHelper
import io.klaytn.utils.{SlackUtil}
import io.klaytn.persistent.impl.rdb.RDBTransactionPersistentAPI
import io.klaytn.persistent.impl.rdb.RDBEventLogPersistentAPI
import io.klaytn.persistent.impl.rdb.RDBBlockPersistentAPI
import io.klaytn.model.RefinedEventLog
import io.klaytn.service.CaverService
/*
--driver-memory 10g
--num-executors 40
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.transaction.TransactionRestoreBatch
 */
object TransactionRestoreBatch extends SparkHelper with BulkLoadHelper {
  import TransactionRestoreBatchDeps._

  def transactionRestoreBatchByDB(bnp: Int): Unit = {
    val txRepository = new RDBTransactionPersistentAPI()
    val eventLogRepository = new RDBEventLogPersistentAPI()
    val caverService = CaverService.of()
    val start = bnp * 100000L
    val end = (bnp + 1) * 100000L
    val missingBlocks = txRepository.getMissingBlocks(start, end)

    if (missingBlocks.isEmpty) {
      val mismatchingBlocks = txRepository.getMismatchingBlocks(start, end)
      SlackUtil.sendMessage(
        s"TransactionRestoreBatch Start: $start, End: $end, MismatchingBlocks: ${mismatchingBlocks.size}")
      if (start == 0) {
        return
      } else if (mismatchingBlocks.isEmpty) {
        return
      }
      mismatchingBlocks.foreach { blockNumber =>
        val block = caverService.getBlock(blockNumber)
        txRepository.insertTransactionReceipts(block.toRefined._2)
        eventLogRepository.insertEventLogs(block.toRefined._3)
        SlackUtil.sendMessage(s"TransactionRestoreBatch Inserted: $blockNumber")
      }
    } else {
      val blockRepository = new RDBBlockPersistentAPI()
      SlackUtil.sendMessage(
        s"TransactionRestoreBatch Start: $start, End: $end, MissingBlocks: ${missingBlocks.size}")
      missingBlocks.foreach { blockNumber =>
        val block = caverService.getBlock(blockNumber)
        blockRepository.insertBlocks(List(block.toRefined._1))
        txRepository.insertTransactionReceipts(block.toRefined._2)
        eventLogRepository.insertEventLogs(block.toRefined._3)
        SlackUtil.sendMessage(s"TransactionRestoreBatch Inserted: $blockNumber")
      }

    }

  }

  def transactionRefineBatch(bnp: Int): Unit = {
    val txRepository = new RDBTransactionPersistentAPI()
    val start = bnp * 100000L
    val end = (bnp + 1) * 100000L

    val sql =
      s"""UPDATE transactions t
      SET
          t.code_format = CASE WHEN t.code_format = '' THEN NULL ELSE t.code_format END,
          t.contract_address = CASE WHEN t.contract_address = '' THEN NULL ELSE t.contract_address END,
          t.fee_payer_signatures = CASE WHEN t.fee_payer_signatures = '' THEN NULL ELSE t.fee_payer_signatures END,
          t.fee_payer = CASE WHEN t.fee_payer = '' THEN NULL ELSE t.fee_payer END,
          t.fee_ratio = CASE WHEN t.fee_ratio = '' THEN NULL ELSE t.fee_ratio END,
          t.max_fee_per_gas = CASE WHEN t.max_fee_per_gas = '' THEN NULL ELSE t.max_fee_per_gas END,
          t.max_priority_fee_per_gas = CASE WHEN t.max_priority_fee_per_gas = '' THEN NULL ELSE t.max_priority_fee_per_gas END,
          t.access_list = CASE WHEN t.access_list = '' THEN '[]' ELSE t.access_list END,
          t.chain_id = CASE WHEN t.chain_id = '' THEN '0x2019' ELSE t.chain_id END,
          t.value = CASE WHEN t.value = '' THEN NULL ELSE t.value END,
          t.to = CASE WHEN t.to = '' THEN NULL ELSE t.to END,
          t.`key` = CASE WHEN t.`key` = '' THEN NULL ELSE t.`key` END
      WHERE
          t.block_number BETWEEN ? and ?"""

    withDB("finder0101") { c =>
      val pstmt = c.prepareStatement(sql)
      pstmt.setLong(1, start)
      pstmt.setLong(2, end)
      val result = pstmt.executeUpdate()
      pstmt.close()
      SlackUtil.sendMessage(
        s"TransactionRefineBatch Start: $start, End: $end, Updated: $result")
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    start to end foreach { bnp =>
      transactionRefineBatch(bnp)
    }
  }
}
