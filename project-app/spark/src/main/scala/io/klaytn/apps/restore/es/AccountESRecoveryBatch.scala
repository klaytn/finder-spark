package io.klaytn.apps.restore.es

import com.typesafe.config.ConfigFactory
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESIndexRequest}
import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.model.finder.es.ESAccount
import io.klaytn.persistent.PersistentAPIErrorReporter
import io.klaytn.utils.{JsonUtil, Utils}
import io.klaytn.utils.http.HttpClient
import io.klaytn.utils.spark.SparkHelper

object AccountESRecoveryBatch extends SparkHelper {
  import spark.implicits._

  private val dao = new AccountDao()
  private val config = ConfigFactory.load()
  private val httpClient = HttpClient.createPooled()
  private val esClient = new ESClientImpl(ESConfig.account(), httpClient)

  private val retryCount = 3
  private val retrySleepMS = 3000
  private val errorReporter = PersistentAPIErrorReporter.default(getClass)

  private val optConfigKeyStart = "spark.app.es_recovery.start_id"
  private val optConfigKeyEnd = "spark.app.es_recovery.end_id"
  private val configKeyRepartition = "spark.app.es_recovery.repartition"
  private val configKeyBulkSize = "spark.app.es_recovery.index_bulk_size"

  override def run(args: Array[String]): Unit = {
    val minId = if (config.hasPath(optConfigKeyStart)) {
      config.getLong(optConfigKeyStart)
    } else {
      dao.getMinId()
    }

    val maxId = if (config.hasPath(optConfigKeyEnd)) {
      config.getLong(optConfigKeyEnd)
    } else {
      dao.getMaxId()
    }

    val bulkSize = config.getInt(configKeyBulkSize)
    Range
      .Long(minId, maxId, bulkSize)
      .toDS()
      .repartition(config.getInt(configKeyRepartition))
      .foreachPartition { (iterator: Iterator[Long]) =>
        iterator.foreach { step =>
          val fromInclusive = step
          val endExclusive = Math.min(maxId, step + bulkSize)
          val list = dao.findByIdRange(fromInclusive, endExclusive)
          esBulkIndex(list)
        }
      }
  }

  private def esBulkIndex(list: Seq[AccountDTO]): Unit = {
    val dtoList = list.map { dto =>
      ESAccount(
        address = dto.address,
        `type` = dto.accountType,
        balance = dto.balance,
        contract_type = dto.contract_type,
        contract_creator_address = dto.contract_creator_address,
        contract_creator_tx_hash = dto.contract_creator_tx_hash,
        kns_domain = dto.kns_domain,
        address_label = dto.address_label,
        tags = dto.tags
          .flatMap(x => JsonUtil.fromJsonArray(x))
          .map(seq => seq.map(_.toString)),
        updated_at = dto.updated_at,
      )
    }
    val indexRequestList = dtoList.map(
      dto =>
        ESIndexRequest(
          dto.address,
          dto.asJson
      ))
    val bulkRequest = ESBulkIndexRequest(esClient.indexName, indexRequestList)
    Utils.retryAndReportOnFail(retryCount, retrySleepMS, errorReporter.report) {
      esClient.bulkInsert(bulkRequest)
    }
  }

}
