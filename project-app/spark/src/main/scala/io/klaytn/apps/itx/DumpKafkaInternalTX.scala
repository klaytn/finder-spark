package io.klaytn.apps.itx

import io.klaytn.apps.common.DumpKafka
import io.klaytn.model.{ChainPhase, DumpKafkaLog, InternalTransaction}
import io.klaytn.persistent.InternalTransactionPersistentAPI
import io.klaytn.service.InternalTransactionService
import io.klaytn.utils.spark.UserConfig
import org.apache.spark.rdd.RDD

object DumpKafkaInternalTX extends DumpKafka {
  private val chainPhase = ChainPhase.get()
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)

  private val iTXService = new InternalTransactionService(
    internalTransactionPersistentAPI)

  override def postProcess(dumpKafkaLogRDD: RDD[DumpKafkaLog]): Unit = {
    val traceRDD = dumpKafkaLogRDD
      .flatMap(data => InternalTransaction.parse(data.log))
      .flatMap(_.toRefined())
    iTXService.saveInternalTransactionToS3(traceRDD,
                                           UserConfig.logStorageS3Path,
                                           UserConfig.refinedLogCount)
  }
}
