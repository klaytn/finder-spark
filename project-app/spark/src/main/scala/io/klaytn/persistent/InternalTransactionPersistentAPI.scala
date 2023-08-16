package io.klaytn.persistent

import io.klaytn.model.{ChainPhase, RefinedInternalTransactions}
import io.klaytn.persistent.impl.rdb.RDBInternalTransactionPersistentAPI
import io.klaytn.repository.InternalTransactionForRegContract

object InternalTransactionPersistentAPI {
  def of(chainPhase: ChainPhase): InternalTransactionPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBInternalTransactionPersistentAPI()
    }
  }
}

trait InternalTransactionPersistentAPI extends Serializable {
  def getInternalTransactionDB(blockNumber: Long): String
  def getInternalTransactionIndexDB(account: String): String
  def getMaxId(dbName: String): Option[Long]
  def getITXs(dbName: String,
              tableId: Long,
              cnt: Int): Seq[InternalTransactionForRegContract]
  def insert(internalTransactions: Seq[RefinedInternalTransactions],
             dbName: String): Unit
  def insert(internalTransactions: List[RefinedInternalTransactions]): Unit
  def insertIndex(internalTransactions: Seq[RefinedInternalTransactions],
                  dbName: String): Unit
  def insertIndex(internalTransactions: List[RefinedInternalTransactions]): Unit
}
