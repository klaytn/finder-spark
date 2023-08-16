package io.klaytn.apps.restore.es

import com.typesafe.config.ConfigFactory
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESIndexRequest}
import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.model.finder.es.ESTransaction
import io.klaytn.persistent.PersistentAPIErrorReporter
import io.klaytn.utils.{SlackUtil, Utils}
import io.klaytn.utils.http.HttpClient
import io.klaytn.utils.spark.SparkHelper

import scala.collection.JavaConverters._

object TransactionESRecoveryBatch extends SparkHelper {
  import spark.implicits._
  private val esRetryCount = 3
  private val esRetrySleepMS = 500

  private val config = ConfigFactory.load()

  private val httpClient = HttpClient.createPooled()
  private val esClient = new ESClientImpl(ESConfig.transaction(), httpClient)
  private val errorReporter = PersistentAPIErrorReporter.default(getClass)

  private val optConfigKeyStartId = "spark.app.es_recovery.start_id"
  private val optConfigKeyEndId = "spark.app.es_recovery.end_id"
  private val minBlockNumber =
    config.getLong("spark.app.es_recovery.min_block_number")
  private val bulkSize = config.getInt("spark.app.es_recovery.index_bulk_size")
  private val dbNames = config.getStringList("spark.app.es_recovery.db_names")
  private val tableName = config.getString("spark.app.es_recovery.table_name")
  private val repartitionSize =
    config.getInt("spark.app.es_recovery.repartition")

  override def run(args: Array[String]): Unit = {
    dbNames.iterator().asScala.foreach { dbName =>
      _run(dbName, tableName)
    }
  }

  private def _run(dbName: String, tableName: String): Unit = {
    val dao = new TransactionDao(
      dbName = dbName,
      tableName = tableName,
      SlackUtil.sendMessage
    )

    val minId = if (config.hasPath(optConfigKeyStartId)) {
      config.getLong(optConfigKeyStartId)
    } else {
      dao.getMinId()
    }

    val maxId = if (config.hasPath(optConfigKeyEndId)) {
      config.getLong(optConfigKeyEndId)
    } else {
      dao.getMaxId()
    }

    val esIndexCountAccumulator =
      spark.sparkContext.longAccumulator("es_index_count")

    Range
      .Long(minId, maxId, bulkSize)
      .toDS()
      .repartition(repartitionSize)
      .foreachPartition { (iterator: Iterator[Long]) =>
        iterator.grouped(100).foreach { grouped =>
          grouped.foreach { step =>
            val fromInclusive = step
            val endExclusive = Math.min(maxId, step + bulkSize)
            val dtoList = dao.findRecoveryTargets(
              fromIdInclusive = fromInclusive,
              toIdExclusive = endExclusive,
              minBlockNumber = minBlockNumber
            )
            if (dtoList.nonEmpty) {
              esIndexCountAccumulator.add(dtoList.size)
              val indexRequestList = dtoList
                .map { dto =>
                  ESTransaction(
                    block_hash = dto.block_hash,
                    block_number = dto.block_number,
                    transaction_hash = dto.transaction_hash,
                    transaction_index = dto.transaction_index,
                    status = dto.status,
                    type_int = dto.type_int,
                    chain_id = dto.chain_id,
                    contract_address = dto.contract_address,
                    fee_payer = dto.fee_payer,
                    from = dto.from,
                    to = dto.to,
                    sender_tx_hash = dto.sender_tx_hash,
                    timestamp = dto.timestamp,
                  )
                }
                .map { x =>
                  val docId = x.transaction_hash
                  val doc = x.asJson
                  ESIndexRequest(docId, doc)
                }

              val bulkRequest =
                ESBulkIndexRequest(esClient.indexName, indexRequestList)
              Utils.retryAndReportOnFail(esRetryCount,
                                         esRetrySleepMS,
                                         errorReporter.report) {
                val response = esClient.bulkInsert(bulkRequest)
                if (response.isFailed()) {
                  val message =
                    s"""request: ${bulkRequest.toRequestBodyString()}
                       |response: ${response.body}
                       |""".stripMargin
                  throw new IllegalStateException(message)
                }
              }
            }
          }
        }
      }

    val msg =
      s"""${this.getClass.getSimpleName}
         |minBlockId        : $minBlockNumber
         |id_range          : $minId ~ $maxId
         |total index count : ${esIndexCountAccumulator.sum}
         |bulkSize          : $bulkSize
         |repartition       : $repartitionSize
         |""".stripMargin
    SlackUtil.sendMessage(msg)
  }
}
