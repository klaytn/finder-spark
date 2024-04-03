package io.klaytn.apps.block

import io.klaytn.apps.common.LastProcessedBlockNumber
import io.klaytn.apps.common.LoaderHelper.getBlock
import io.klaytn.client.FinderRedis
import io.klaytn.model.ChainPhase
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.service.{BlockService, CaverService, LoadDataInfileService}
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.KafkaStreamingHelper

import scala.collection.mutable.ArrayBuffer

object BlockToDBStreaming extends KafkaStreamingHelper {
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
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .mapPartitions { iter =>
            val start0 = System.currentTimeMillis()
            var start = start0

            val longTime = ArrayBuffer.empty[String]

            val logSeq = iter
              .filter(_.key().toLong >= LastProcessedBlockNumber
                .getLastProcessedBlockNumber())
              .toSeq
            val result = getBlock(logSeq, "BlockToDB", caverService)
              .flatMap { block =>
                longTime.append(
                  s"time: ${System.currentTimeMillis() - start}; block number: ${block.blockNumber}")
                start = System.currentTimeMillis()

                val (longTimeLogs, result) =
                  blockService.processParallel(block, jobBasePath)
                longTime.appendAll(longTimeLogs)

                result
              }

            if (System
                  .currentTimeMillis() - start0 > 1000 && longTime.nonEmpty) {
              SlackUtil.sendMessage(longTime.mkString("\n"))
            }

            var txCount = 0
            var eventLogCount = 0
            /*
            result: [("blockNumber", number), ("tx", s3file), ("eventLog", s3file)]
             */
            result.map { x =>
              x._1 match {
                case "blockNumber" => (0, x)
                case "tx" =>
                  txCount += 1
                  txCount % 4 match {
                    case 0 => (0, x)
                    case 1 => (1, x)
                    case 2 => (4, x)
                    case _ => (5, x)
                  }
                case "eventLog" =>
                  eventLogCount += 1
                  eventLogCount % 4 match {
                    case 0 => (2, x)
                    case 1 => (3, x)
                    case 2 => (6, x)
                    case _ => (7, x)
                  }
                case _ => (0, x)
              }
            }.iterator
          }
          .groupByKey(8)
          .foreach { iter =>
            val blockNumbers = ArrayBuffer.empty[Long]
            iter._2.foreach {
              case (key, value) =>
                if (key != "blockNumber") {
                  loadDataInfileService.loadDataAndDeleteFile(value, None)
                } else {
                  blockNumbers.append(value.toLong)
                }
            }

            if (blockNumbers.nonEmpty) {
              val currentBlockNumber = blockNumbers.max
              val blockNumber = FinderRedis.get("latest:block") match {
                case Some(blockNumber) => blockNumber.toLong
                case _                 => 0L
              }
              if (blockNumber < currentBlockNumber) {
                LastProcessedBlockNumber.setLastProcessedBlockNumber(
                  currentBlockNumber)
                FinderRedis.publish("channel:block",
                                    currentBlockNumber.toString)
                FinderRedis.set("latest:block", currentBlockNumber.toString)
              }
            }
          }
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
