package io.klaytn.apps.adhoc.block

import io.klaytn.model.ChainPhase
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.service.{BlockService, CaverService, LoadDataInfileService}
import io.klaytn.utils.spark.SparkHelper

object LoadBlockRewardBatch extends SparkHelper {
  private val chainPhase = ChainPhase.get()
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
  private val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
  private val caverService = CaverService.of()
  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  private val blockService = new BlockService(
    blockPersistentAPI,
    transactionPersistentAPI,
    eventLogPersistentAPI,
    caverService,
    loadDataInfileService
  )

  override def run(args: Array[String]): Unit = {
    // Cypress
    sc.parallelize(0 until 126000000)
      .repartition(8)
      .foreachPartition { numbers =>
        numbers.foreach(number =>
          blockService.saveBlockRewardToMysql(number.toLong))
      }
  }
}
