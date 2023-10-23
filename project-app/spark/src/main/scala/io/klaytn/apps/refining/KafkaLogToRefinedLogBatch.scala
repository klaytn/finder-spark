package io.klaytn.apps.refining

import io.klaytn.model.{Block, ChainPhase, InternalTransaction}
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  InternalTransactionPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.service.{
  BlockService,
  CaverService,
  InternalTransactionService,
  LoadDataInfileService
}
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.{SparkHelper, UserConfig}

object KafkaLogToRefinedLogBatch extends SparkHelper {
  private val chainPhase = ChainPhase.get()
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
  private val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  private val caverService = CaverService.of()
  private val blockService = new BlockService(
    blockPersistentAPI,
    transactionPersistentAPI,
    eventLogPersistentAPI,
    caverService,
    loadDataInfileService
  )
  private val internalTransactionService = new InternalTransactionService(
    internalTransactionPersistentAPI)

  override def run(args: Array[String]): Unit = {
    0 to 1253 foreach { bnp =>
      Seq("blocks", "event_logs", "transaction_receipts").foreach { label =>
        GCSUtil.delete(
          "klaytn-prod-lake",
          s"klaytn/${UserConfig.chainPhase.chain}/label=$label/bnp=$bnp",
          true)
      }
      val input =
        s"s3a://klaytn-prod-lake/klaytn/${UserConfig.chainPhase.chain}/label=kafka_log/topic=block/bnp=$bnp/*.gz"
      val blockRDD = sc.textFile(input).flatMap(Block.parse).map(_.toRefined)
      blockService.saveBlockToS3(blockRDD, UserConfig.logStorageS3Path, 8, true)
    }

    0 to 1253 foreach { bnp =>
      Seq("internal_transactions").foreach { label =>
        GCSUtil.delete(
          "klaytn-prod-lake",
          s"klaytn/${UserConfig.chainPhase.chain}/label=$label/bnp=$bnp",
          true)
      }
      val traceRDD = sc
        .textFile(
          s"s3a://klaytn-prod-lake/klaytn/${UserConfig.chainPhase.chain}/label=kafka_log/topic=trace/bnp=$bnp/*.gz")
        .flatMap { line =>
          InternalTransaction.parse(line) match {
            case Some(internalTransaction: InternalTransaction) =>
              internalTransaction.toRefined()
            case _ => None
          }
        }

      internalTransactionService.saveInternalTransactionToS3(
        traceRDD,
        UserConfig.logStorageS3Path,
        10)
    }
  }
}
