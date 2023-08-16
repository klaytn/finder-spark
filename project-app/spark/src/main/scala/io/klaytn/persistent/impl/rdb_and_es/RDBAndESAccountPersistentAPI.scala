package io.klaytn.persistent.impl.rdb_and_es

import io.klaytn._
import io.klaytn.client.es.ESClient
import io.klaytn.client.es.request.impl.ESUpdateRequestImpl
import io.klaytn.model.finder.es.ESAccount
import io.klaytn.model.finder.{AccountType, ContractType}
import io.klaytn.persistent.{AccountPersistentAPI, PersistentAPIErrorReporter}
import io.klaytn.repository.AccountRepository
import io.klaytn.utils.Utils

private[this] object AccountRepositoryImpl extends AccountRepository

class RDBAndESAccountPersistentAPI(
    esClient: LazyEval[ESClient],
    errorReporter: LazyEval[PersistentAPIErrorReporter] =
      PersistentAPIErrorReporter.default(classOf[RDBAndESAccountPersistentAPI])
) extends AccountPersistentAPI {
  private val esRetryCount = 3
  private val esRetrySleepMS = 500
  private val repository = AccountRepositoryImpl

  override def getConsensusNodes(): Seq[String] = repository.getConsensusNodes()

  override def getAccountType(address: String): AccountType.Value = {
    repository.getAccountType(address)
  }

  override def updateContractInfos(contractAddress: String,
                                   contractType: ContractType.Value,
                                   contractCreator: String,
                                   createTxHash: String,
                                   createdTimestamp: Long,
                                   from: String): Unit = {
    repository
      .updateContractInfos(contractAddress,
                           contractType,
                           contractCreator,
                           createTxHash,
                           createdTimestamp,
                           from)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = contractAddress
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "contract_type" -> contractType.id,
          "contract_creator_address" -> contractCreator,
          "contract_creator_tx_hash" -> createTxHash,
          "updated_at" -> System.currentTimeMillis()
        ))
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def updateContractType(contractAddress: String,
                                  contractType: ContractType.Value): Unit = {
    repository.updateContractType(contractAddress, contractType)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = contractAddress
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "contract_type" -> contractType.id,
          "updated_at" -> System.currentTimeMillis()
        )
      )
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def updateTotalTXCountAndType(transactionCount: Int,
                                         contractType: ContractType.Value,
                                         from: String,
                                         txHash: String,
                                         address: String): Unit = {
    repository.updateTotalTXCountAndType(transactionCount,
                                         contractType,
                                         from,
                                         txHash,
                                         address)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = address
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "contract_type" -> contractType.id,
          "contract_creator_address" -> from,
          "contract_creator_tx_hash" -> txHash,
          "updated_at" -> System.currentTimeMillis()
        )
      )
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def updateTotalTXCount(transactionCount: Int,
                                  address: String): Unit = {
    repository.updateTotalTXCount(transactionCount, address)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = address
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "updated_at" -> System.currentTimeMillis()
        )
      )
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def updateTotalTXCountBatch(data: Seq[(String, Long)]): Unit = {
    repository.updateTotalTXCountBatch(data)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      data.foreach {
        case (address, _) =>
          val docId = address
          val updateRequest = ESUpdateRequestImpl(
            Map(
              "updated_at" -> System.currentTimeMillis()
            )
          )
          val response = esClient.update(docId, updateRequest)
          if (!response.is2xx) {
            if (response.body.contains("document_missing_exception")) {
              // TODO- Ignore Until migration
              // ignore
            } else {
              throw new IllegalStateException(response.body)
            }
          }
      }
    }
  }

  override def insert(address: String,
                      accountType: AccountType.Value,
                      transactionCount: Int,
                      contractType: ContractType.Value,
                      contractCreatorAddress: Option[String],
                      contractCreatorTransactionHash: Option[String],
                      createdTimestamp: Long,
                      from: Option[String]): Unit = {
    repository.insert(address,
                      accountType,
                      transactionCount,
                      contractType,
                      contractCreatorAddress,
                      contractCreatorTransactionHash,
                      createdTimestamp,
                      from)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val account = ESAccount(
        address = address,
        `type` = accountType.id,
        balance = None,
        contract_type = contractType.id,
        contract_creator_address = contractCreatorAddress,
        contract_creator_tx_hash = contractCreatorTransactionHash,
        kns_domain = None,
        address_label = None,
        tags = None,
        updated_at = System.currentTimeMillis()
      )

      val response = esClient.insert(address, account.asJson)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }

  override def updatePrimaryKNS(address: String, name0: String): Unit = {
    val name = if (name0 != null && name0.isEmpty) null else name0

    if (name != null) {
      repository.findAddressByKNS(name) match {
        case Some(a) =>
          repository.updateKNS(a, null) // Reset the existing values in the DB
          Utils.retryAndReportOnFail(esRetryCount,
                                     esRetrySleepMS,
                                     errorReporter.report) {
            val docId = address
            val updateRequest = ESUpdateRequestImpl(
              Map(
                "updated_at" -> System.currentTimeMillis(),
                "kns_domain" -> ""
              )
            )
            val response = esClient.update(docId, updateRequest)
            if (!response.is2xx) {
              if (response.body.contains("document_missing_exception")) {
                // TODO- Ignore Until migration
                // ignore
              } else {
                throw new IllegalStateException(response.body)
              }
            }
          }
        case _ =>
      }
    }

    repository.updateKNS(address, name)
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      val docId = address
      val knsDomain = if (name == null) "" else name
      val updateRequest = ESUpdateRequestImpl(
        Map(
          "updated_at" -> System.currentTimeMillis(),
          "kns_domain" -> knsDomain
        )
      )
      val response = esClient.update(docId, updateRequest)
      if (!response.is2xx) {
        if (response.body.contains("document_missing_exception")) {
          // TODO- Ignore Until migration
          // ignore
        } else {
          throw new IllegalStateException(response.body)
        }
      }
    }
  }
}
