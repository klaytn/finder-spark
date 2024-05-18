package io.klaytn.persistent.impl.rdb

import io.klaytn.model.{Block, RefinedTransactionReceipt}
import io.klaytn.persistent.TransactionPersistentAPI
import io.klaytn.repository.TransactionRepository

class RDBTransactionPersistentAPI
    extends TransactionRepository
    with TransactionPersistentAPI {
  override def getTransactionHashAndTsAndTxErrorAndFrom(
      blockNumber: Long,
      transactionIndex: Int): Option[(String, Int, Int, String)] = {
    super.getTransactionHashAndTsAndTxErrorFrom(blockNumber, transactionIndex)
  }

  override def insertTransactionReceipts(
      transactionReceipts: List[RefinedTransactionReceipt]): Unit = {
    super.insertTransactionReceipts(transactionReceipts)
  }

  override def insertTransactionReceipts(block: Block): Unit = {
    this.insertTransactionReceipts(block.toRefined._2)
  }

  override def getMismatchingBlocks(startNum: Long,
                                    endNum: Long): List[Long] = {
    super.getMismatchingBlocks(startNum, endNum)
  }

  override def getMissingBlocks(startNum: Long, endNum: Long): List[Long] = {
    super.getMissingBlocks(startNum, endNum)
  }

  override def getTransactionReceiptsByBlockRange(
      startNum: Long,
      endNum: Long): Seq[RefinedTransactionReceipt] = {
    super.getTransactionReceiptsByBlockRange(startNum, endNum)
  }
}
