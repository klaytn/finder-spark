package io.klaytn.apps.adhoc.restore

import io.klaytn.model.ChainPhase
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.service.{BlockService, CaverService, LoadDataInfileService}
import io.klaytn.utils.Utils
import io.klaytn.utils.spark.SparkHelper

object RestoreBlockBatch extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    val chainPhase = ChainPhase.get()
    val caverService =
      CaverService.of("http://en-cypress.klaytnfinder.io:8551")

    val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
    val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
    val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
    val loadDataInfileService = LoadDataInfileService.of(chainPhase)

    val blockService = new BlockService(
      blockPersistentAPI,
      transactionPersistentAPI,
      eventLogPersistentAPI,
      caverService,
      loadDataInfileService
    )

    140760000 to 140860000 foreach { blockNumber =>
      val block = caverService.getBlock(blockNumber)
      Utils.retry(5, 1000) {
        val (_, loadFile) = blockService.process(block, jobBasePath)
        loadFile.foreach {
          case (k, v) =>
            if (k != "blockNumber") {
              loadDataInfileService.loadDataAndDeleteFile(v, None)
            }
        }
      }
    }
  }
}
