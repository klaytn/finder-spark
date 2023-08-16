package io.klaytn.apps.adhoc.restore

import com.typesafe.config.ConfigFactory
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper
import scala.collection.JavaConverters._

object GasPriceRecoveryBatch extends SparkHelper {
  import spark.implicits._

  private val config = ConfigFactory.load()
  private val optConfigKeyStartId = "spark.app.start_id"
  private val optConfigKeyEndId = "spark.app.end_id"
  private val configKeyRepartition = "spark.app.repartition"

  private val beforeGasPrice =
    config.getString("spark.app.gas_price_conditions_not")
  private val scanBulkSize = config.getInt("spark.app.scan_bulk_size")

  override def run(args: Array[String]): Unit = {
    config.getStringList("spark.app.db_names").iterator().asScala.foreach {
      dbName =>
        _run(dbName, "transactions")
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

    val updateAccumulator = spark.sparkContext.longAccumulator("update_count")

    Range
      .Long(minId, maxId, scanBulkSize)
      .toDS()
      .repartition(config.getInt(configKeyRepartition))
      .foreachPartition { (iterator: Iterator[Long]) =>
        iterator.foreach { step =>
          val fromInclusive = step
          val endExclusive = Math.min(maxId, step + scanBulkSize)
          val ids = dao.findRecoveryTargetIdsGasPriceNotEquals(
            fromIdInclusive = fromInclusive,
            toIdExclusive = endExclusive,
            gasPrice = beforeGasPrice,
          )
          if (ids.nonEmpty) {
            updateAccumulator.add(ids.size)
//            dao.updateGasPrice(ids = ids, gasPrice = currentGasPrice, bulkSize = updateBulkSize)
          }
        }
      }

    SlackUtil.sendMessage(s"""${this.getClass.getSimpleName}
                             |id range: $minId ~ $maxId
                             |total target count: ${updateAccumulator.count}
                             |beforeGasPrice    : $beforeGasPrice
                             |scanBulkSize      : $scanBulkSize
                             |target_size       : ${updateAccumulator.count}
                             |""".stripMargin)
  }
}
