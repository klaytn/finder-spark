package io.klaytn.apps.restore.es

import com.typesafe.config.ConfigFactory
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESIndexRequest}
import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.model.finder.es.ESContract
import io.klaytn.persistent.PersistentAPIErrorReporter
import io.klaytn.utils.{SlackUtil, Utils}
import io.klaytn.utils.http.HttpClient
import io.klaytn.utils.spark.SparkHelper

object ContractESRecoveryBatch extends SparkHelper {
  import spark.implicits._

  private val dao = new ContractDao()
  private val config = ConfigFactory.load()
  private val httpClient = HttpClient.createPooled()
  private val esClient = new ESClientImpl(ESConfig.contract(), httpClient)

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

  private def esBulkIndex(list: Seq[ContractDTO]): Unit = {
    val dtoList = list.filter(_.contract_address != null).map { dto =>
      ESContract(
        contract_address = dto.contract_address,
        contract_type = dto.contract_type,
        name = dto.name,
        symbol = dto.symbol,
        verified = dto.verified,
        created_at = dto.created_at,
        updated_at = dto.updated_at,
        total_supply_order = dto.total_supply_order,
        total_transfer = dto.total_transfer,
      )
    }
    val indexRequestList = dtoList.flatMap { dto =>
      try {
        val request = ESIndexRequest(
          dto.contract_address,
          dto.asJson
        )
        Some(request)
      } catch {
        case e: Throwable =>
          val msg =
            s"""${this.getClass.getSimpleName.stripSuffix("$")}
               |e: ${e.getLocalizedMessage}
               |address: ${dto.contract_address}
               |type: ${dto.contract_type}
               |name: ${dto.name}
               |symbol: ${dto.symbol}
               |verified: ${dto.verified}
               |total_supply_order: ${dto.total_supply_order}
               |total_transfer: ${dto.total_transfer}
               |""".stripMargin
          SlackUtil.sendMessage(msg)
          None
      }
    }
    val bulkRequest = ESBulkIndexRequest(esClient.indexName, indexRequestList)
    Utils.retryAndReportOnFail(retryCount, retrySleepMS, errorReporter.report) {
      esClient.bulkInsert(bulkRequest)
    }
  }

}
