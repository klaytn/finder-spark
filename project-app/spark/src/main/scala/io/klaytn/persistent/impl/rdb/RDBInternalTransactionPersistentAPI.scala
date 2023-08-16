package io.klaytn.persistent.impl.rdb

import io.klaytn.model.RefinedInternalTransactions
import io.klaytn.persistent.InternalTransactionPersistentAPI
import io.klaytn.repository.{
  InternalTransactionForRegContract,
  InternalTransactionRepository
}

class RDBInternalTransactionPersistentAPI
    extends InternalTransactionRepository
    with InternalTransactionPersistentAPI {
  override def getInternalTransactionDB(blockNumber: Long): String = {
    super.getInternalTransactionDB(blockNumber)
  }

  override def getInternalTransactionIndexDB(account: String): String = {
    super.getInternalTransactionIndexDB(account)
  }

  override def getMaxId(dbName: String): Option[Long] = {
    super.getMaxId(dbName)
  }

  override def getITXs(dbName: String,
                       tableId: Long,
                       cnt: Int): Seq[InternalTransactionForRegContract] = {
    super.getITXs(dbName, tableId, cnt)
  }

  override def insert(internalTransactions: Seq[RefinedInternalTransactions],
                      dbName: String): Unit = {
    super.insert(internalTransactions, dbName)
  }

  override def insert(
      internalTransactions: List[RefinedInternalTransactions]): Unit = {
    super.insert(internalTransactions)
  }

  override def insertIndex(
      internalTransactions: Seq[RefinedInternalTransactions],
      dbName: String): Unit = {
    super.insertIndex(internalTransactions, dbName)
  }

  override def insertIndex(
      internalTransactions: List[RefinedInternalTransactions]): Unit = {
    super.insertIndex(internalTransactions)
  }
}
