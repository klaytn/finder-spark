package io.klaytn.persistent.impl.rdb_and_es

import io.klaytn._
import io.klaytn.client.es.ESClient
import io.klaytn.client.es.request.impl.{
  AssignScriptValue,
  ESScriptRequest,
  ESUpdateRequestImpl,
  IncrementScriptValue
}
import io.klaytn.model.finder.Contract
import io.klaytn.model.finder.es.ESContract
import io.klaytn.persistent.{
  ContractDTO,
  ContractPersistentAPI,
  PersistentAPIErrorReporter
}
import io.klaytn.repository.ContractRepository
import io.klaytn.utils.Utils

private[this] object ContractRepositoryImpl extends ContractRepository

class RDBAndESContractPersistentAPI(
    esClient: LazyEval[ESClient],
    errorReporter: LazyEval[PersistentAPIErrorReporter] =
      PersistentAPIErrorReporter.default(
        classOf[RDBAndESContractPersistentAPI]))
    extends ContractPersistentAPI {
  private val esRetryCount = 3
  private val esRetrySleepMS = 500
  private val repository = ContractRepositoryImpl

  override def insert(contract: Contract,
                      transferCount: Int,
                      createdTimestamp: Long,
                      txError: Int): Option[Contract] = {
    val returnValues =
      repository.insert(contract, transferCount, createdTimestamp, txError)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val decimal = contract.decimal.getOrElse(0)
      val bTotalSupply = contract.totalSupply.getOrElse(BigInt(0))

      val docId = contract.contractAddress
      val dto = ESContract(
        contract_address = contract.contractAddress,
        contract_type = contract.contractType.id,
        name = contract.name.getOrElse(""),
        symbol = contract.symbol.getOrElse(""),
        verified = None,
        created_at = createdTimestamp,
        updated_at = createdTimestamp,
        total_supply_order = Utils.getTotalSupplyOrder(decimal, bTotalSupply),
        total_transfer = transferCount,
      )

      val response = esClient.insert(docId, dto.asJson)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }

      }
    }
    returnValues
  }

  override def updateTotalTransfer(
      data: Seq[(String, Int)]): Seq[((String, Int), Int)] = {
    val returnValues = repository.updateTotalTransfer(data)
    data.foreach {
      case (contractAddress, totalTransfer) =>
        Utils.retryAndReportOnFail(esRetryCount,
                                   esRetrySleepMS,
                                   errorReporter.report) {
          val docId = contractAddress
          val updateRequest = ESScriptRequest(
            Seq(
              IncrementScriptValue("total_transfer", totalTransfer),
              AssignScriptValue("updated_at", System.currentTimeMillis())
            ),
          )
          val response = esClient.update(docId, updateRequest)
          if (!response.is2xx) {
            if (response.body.contains("document_missing_exception")) {
              // TODO- Ignore Un
              // ignore
            } else {
              throw new IllegalStateException(response.body)
            }
          }
        }
    }
    returnValues
  }

  override def updateHolderCount(data: Seq[(String, Long)]): Unit = {
    repository.updateHolderCount(data, false)
    // todo foreach to bulkUpdate
    data.map {
      case (contractAddress, _) =>
        Utils.retryAndReportOnFail(esRetryCount,
                                   esRetrySleepMS,
                                   errorReporter.report) {
          val docId = contractAddress
          val updateRequest = ESUpdateRequestImpl(
            Map(
              "updated_at" -> System.currentTimeMillis()
            )
          )
          val response = esClient.update(docId, updateRequest)
          if (!response.is2xx) {
            if (response.body.contains("document_missing_exception")) {
              // TODO- Ignore Un
              // ignore
            } else {
              throw new IllegalStateException(response.body)
            }
          }
        }
    }
  }

  override def replaceHolderCount(data: Seq[(String, Long)]): Unit = {
    repository.updateHolderCount(data, true)
    // todo foreach to bulkUpdate
    data.map {
      case (contractAddress, _) =>
        Utils.retryAndReportOnFail(esRetryCount,
                                   esRetrySleepMS,
                                   errorReporter.report) {
          val docId = contractAddress
          val updateRequest = ESUpdateRequestImpl(
            Map(
              "updated_at" -> System.currentTimeMillis()
            )
          )
          val response = esClient.update(docId, updateRequest)
          if (!response.is2xx) {
            if (response.body.contains("document_missing_exception")) {
              // TODO- Ignore Un
              // ignore
            } else {
              throw new IllegalStateException(response.body)
            }
          }
        }
    }

  }

  override def findContract(address: String): Option[Contract] = {
    repository.findContract(address)
  }

  override def updateTotalSupply(contractAddress: String,
                                 totalSupply: BigInt,
                                 decimal0: Option[Int]): Unit = {
    val decimal = decimal0.getOrElse(0)
    val totalSupplyOrder = Utils.getTotalSupplyOrder(decimal, totalSupply)

    repository.updateTotalSupply(contractAddress,
                                 totalSupply,
                                 decimal,
                                 totalSupplyOrder)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = contractAddress
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "total_supply" -> totalSupply.toString(),
          "total_supply_order" -> totalSupplyOrder,
          "decimal" -> decimal,
          "updated_at" -> System.currentTimeMillis()
        ))
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Un
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def findAllContractByAddress(
      addressList: Seq[String]): Seq[ContractDTO] = {
    repository.findAllContractByAddress(addressList)
  }

  override def updateContract(contract: Contract): Unit = {
    repository.updateContract(contract)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = contract.contractAddress
      val decimal = contract.decimal.getOrElse(0)
      val totalSupply = contract.totalSupply.getOrElse(BigInt(0))
      val totalSupplyOrder = Utils.getTotalSupplyOrder(decimal, totalSupply)
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "contract_type" -> contract.contractType.id,
          "name" -> contract.name.getOrElse(""),
          "symbol" -> contract.symbol.getOrElse(""),
          "total_supply" -> totalSupply.toString(),
          "total_supply_order" -> totalSupplyOrder,
          "decimal" -> decimal
        ))
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Un
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def selectBurnInfo(contractAddress: String): (String, Int) =
    repository.selectBurnInfo(contractAddress)

  override def updateBurnInfo(contractAddress: String,
                              amount: String,
                              count: Int): Unit =
    repository.updateBurnInfo(contractAddress, amount, count)

  override def updateImplementationAddress(
      contractAddress: String,
      implementationAddress: String): Unit =
    repository.updateImplementationAddress(contractAddress,
                                           implementationAddress)
}
