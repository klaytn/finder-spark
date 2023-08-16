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
}
