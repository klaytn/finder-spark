package io.klaytn.apps.block

import io.klaytn.apps.common.DumpKafka
import io.klaytn.model.{Block, ChainPhase, DumpKafkaLog}
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.service.{BlockService, CaverService, LoadDataInfileService}
import io.klaytn.utils.spark.UserConfig
import org.apache.spark.rdd.RDD

object DumpKafkaBlock extends DumpKafka {
  private val chainPhase = ChainPhase.get()
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
  private val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  private val caverService = CaverService.of()
  private val blockService = new BlockService(
    blockPersistentAPI,
    transactionPersistentAPI,
    eventLogPersistentAPI,
    caverService,
    loadDataInfileService
  )

  override def postProcess(dumpKafkaLogRDD: RDD[DumpKafkaLog]): Unit = {
    val blockRDD =
      dumpKafkaLogRDD.flatMap(data => Block.parse(data.log)).map(_.toRefined)
    blockService.saveBlockToS3(blockRDD,
                               UserConfig.logStorageS3Path,
                               UserConfig.refinedLogCount,
                               true)
  }
}
