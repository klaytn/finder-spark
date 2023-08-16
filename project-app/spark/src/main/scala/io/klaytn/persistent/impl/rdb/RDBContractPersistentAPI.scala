package io.klaytn.persistent.impl.rdb

import io.klaytn.model.finder.Contract
import io.klaytn.persistent.ContractPersistentAPI
import io.klaytn.repository.ContractRepository
import io.klaytn.utils.Utils

class RDBContractPersistentAPI()
    extends ContractRepository
    with ContractPersistentAPI {

  override def insert(contract: Contract,
                      transferCount: Int,
                      createdTimestamp: Long,
                      txError: Int): Option[Contract] = {
    super.insert(contract, transferCount, createdTimestamp, txError)
  }

  override def updateTotalTransfer(
      data: Seq[(String, Int)]): Seq[((String, Int), Int)] = {
    super.updateTotalTransfer(data)
  }

  override def updateHolderCount(data: Seq[(String, Long)]): Unit = {
    super.updateHolderCount(data, false)
  }

  override def replaceHolderCount(data: Seq[(String, Long)]): Unit = {
    super.updateHolderCount(data, true)
  }

  override def findContract(address: String): Option[Contract] = {
    super.findContract(address)
  }

  override def updateTotalSupply(contractAddress: String,
                                 totalSupply: BigInt,
                                 decimal0: Option[Int]): Unit = {
    val decimal = decimal0.getOrElse(0)
    val totalSupplyOrder = Utils.getTotalSupplyOrder(decimal, totalSupply)

    super.updateTotalSupply(contractAddress,
                            totalSupply,
                            decimal,
                            totalSupplyOrder)
  }

  override def selectBurnInfo(contractAddress: String): (String, Int) =
    super.selectBurnInfo(contractAddress)

  override def updateBurnInfo(contractAddress: String,
                              amount: String,
                              count: Int): Unit =
    super.updateBurnInfo(contractAddress, amount, count)

  override def updateImplementationAddress(
      contractAddress: String,
      implementationAddress: String): Unit =
    super.updateImplementationAddress(contractAddress, implementationAddress)
}
