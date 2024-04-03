package io.klaytn.persistent

import io.klaytn.model.{Block, ChainPhase, RefinedTransactionReceipt}
import io.klaytn.persistent.impl.rdb.RDBTransactionPersistentAPI

object TransactionPersistentAPI {
  def of(chainPhase: ChainPhase): TransactionPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBTransactionPersistentAPI()
    }
  }
}

trait TransactionPersistentAPI extends Serializable {
  def getTransactionHashAndTsAndTxErrorAndFrom(
      blockNumber: Long,
      transactionIndex: Int): Option[(String, Int, Int, String)]
  def insertTransactionReceipts(
      transactionReceipts: List[RefinedTransactionReceipt]): Unit
  def insertTransactionReceipts(block: Block): Unit
  def getMismatchingBlocks(startNum: Long, endNum: Long): List[Long]
  def getMissingBlocks(startNum: Long, endNum: Long): List[Long]
}
