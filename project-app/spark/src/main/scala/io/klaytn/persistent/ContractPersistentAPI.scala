package io.klaytn.persistent

import io.klaytn._
import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.model.finder.Contract
import io.klaytn.model.{Chain, ChainPhase}
import io.klaytn.persistent.impl.rdb.RDBContractPersistentAPI
import io.klaytn.utils.http.HttpClient
import io.klaytn.model.finder.ContractType
import io.klaytn.persistent.impl.rdb_and_es.RDBAndESContractPersistentAPI

object ContractPersistentAPI {
  def of(chainPhase: ChainPhase): ContractPersistentAPI = {
    chainPhase.chain match {
      case Chain.baobab | Chain.cypress =>
        val contractHttpClient = HttpClient.createPooled()
        val contractESClient =
          new ESClientImpl(ESConfig.contract(), contractHttpClient)
        new RDBAndESContractPersistentAPI(contractESClient)
      case _ =>
        new RDBContractPersistentAPI()
    }
  }
}

case class ContractDTO(contractAddress: String,
                       contractType: ContractType.Value,
                       name: Option[String],
                       symbol: Option[String],
                       decimal: Option[Int],
                       totalSupply: Option[BigInt],
                       verified: Boolean)

trait ContractPersistentAPI {
  def insert(contract: Contract,
             transferCount: Int,
             createdTimestamp: Long,
             txError: Int): Option[Contract]
  def updateTotalTransfer(data: Seq[(String, Int)]): Seq[((String, Int), Int)]
  def updateHolderCount(data: Seq[(String, Long)]): Unit
  def replaceHolderCount(data: Seq[(String, Long)]): Unit
  def findContract(address: String): Option[Contract]
  def findAllContractByAddress(addressList: Seq[String]): Seq[ContractDTO]
  def updateTotalSupply(contractAddress: String,
                        totalSupply: BigInt,
                        decimal0: Option[Int]): Unit
  def updateContract(contract: Contract): Unit
  def selectBurnInfo(contractAddress: String): (String, Int)
  def updateBurnInfo(contractAddress: String, amount: String, count: Int): Unit
  def updateImplementationAddress(contractAddress: String,
                                  implementationAddress: String): Unit
}
