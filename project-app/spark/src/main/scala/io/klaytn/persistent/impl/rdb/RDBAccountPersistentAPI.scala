package io.klaytn.persistent.impl.rdb

import io.klaytn.model.finder.{AccountType, ContractType}
import io.klaytn.persistent.AccountPersistentAPI
import io.klaytn.repository.AccountRepository

class RDBAccountPersistentAPI()
    extends AccountRepository
    with AccountPersistentAPI {
  override def getConsensusNodes(): Seq[String] = super.getConsensusNodes()

  override def getAccountType(address: String): AccountType.Value =
    super.getAccountType(address)

  override def updateContractInfos(contractAddress: String,
                                   contractType: ContractType.Value,
                                   contractCreator: String,
                                   createTxHash: String,
                                   createdTimestamp: Long,
                                   from: String): Unit =
    super.updateContractInfos(contractAddress,
                              contractType,
                              contractCreator,
                              createTxHash,
                              createdTimestamp,
                              from)

  override def updateContractType(contractAddress: String,
                                  contractType: ContractType.Value): Unit =
    super.updateContractType(contractAddress, contractType)

  override def updateTotalTXCountAndType(transactionCount: Int,
                                         contractType: ContractType.Value,
                                         from: String,
                                         txHash: String,
                                         address: String): Unit =
    super.updateTotalTXCountAndType(transactionCount,
                                    contractType,
                                    from,
                                    txHash,
                                    address)

  override def updateTotalTXCount(transactionCount: Int,
                                  address: String): Unit =
    super.updateTotalTXCount(transactionCount, address)

  override def updateTotalTXCountBatch(data: Seq[(String, Long)]): Unit =
    super.updateTotalTXCountBatch(data)

  override def insert(address: String,
                      accountType: AccountType.Value,
                      transactionCount: Int,
                      contractType: ContractType.Value,
                      contractCreatorAddress: Option[String],
                      contractCreatorTransactionHash: Option[String],
                      createdTimestamp: Long,
                      from: Option[String]): Unit =
    super.insert(address,
                 accountType,
                 transactionCount,
                 contractType,
                 contractCreatorAddress,
                 contractCreatorTransactionHash,
                 createdTimestamp,
                 from)

  override def updatePrimaryKNS(address: String, name: String): Unit = {
    super.findAddressByKNS(name) match {
      case Some(a) =>
        super.updateKNS(a, null) // Reset the existing values in the DB
      case _ =>
    }
    super.updateKNS(address, name)
  }
}
