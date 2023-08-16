package io.klaytn.persistent

import io.klaytn._
import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.model.finder.{AccountType, ContractType}
import io.klaytn.model.{Chain, ChainPhase}
import io.klaytn.persistent.impl.rdb.RDBAccountPersistentAPI
import io.klaytn.persistent.impl.rdb_and_es.RDBAndESAccountPersistentAPI
import io.klaytn.utils.http.HttpClient

object AccountPersistentAPI {
  def of(chainPhase: ChainPhase): AccountPersistentAPI = {
    chainPhase.chain match {
      case Chain.baobab | Chain.cypress =>
        val accountHttpClient = HttpClient.createPooled()
        val accountESClient =
          new ESClientImpl(ESConfig.account(), accountHttpClient)
        new RDBAndESAccountPersistentAPI(accountESClient)
      case _ =>
        new RDBAccountPersistentAPI()
    }
  }
}

trait AccountPersistentAPI extends Serializable {
  def getConsensusNodes(): Seq[String]
  def getAccountType(address: String): AccountType.Value
  def updateContractInfos(contractAddress: String,
                          contractType: ContractType.Value,
                          contractCreator: String,
                          createTxHash: String,
                          createdTimestamp: Long,
                          from: String): Unit
  def updateContractType(contractAddress: String,
                         contractType: ContractType.Value): Unit
  def updateTotalTXCountAndType(transactionCount: Int,
                                contractType: ContractType.Value,
                                from: String,
                                txHash: String,
                                address: String): Unit
  def updateTotalTXCount(transactionCount: Int, address: String): Unit
  def updateTotalTXCountBatch(data: Seq[(String, Long)]): Unit
  def insert(address: String,
             accountType: AccountType.Value,
             transactionCount: Int,
             contractType: ContractType.Value,
             contractCreatorAddress: Option[String],
             contractCreatorTransactionHash: Option[String],
             createdTimestamp: Long,
             from: Option[String]): Unit
  def updatePrimaryKNS(address: String, name: String): Unit
}
