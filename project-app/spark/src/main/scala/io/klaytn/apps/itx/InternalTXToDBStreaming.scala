package io.klaytn.apps.itx

import io.klaytn.apps.common.LastProcessedBlockNumber
import io.klaytn.apps.common.LoaderHelper.getInternalTransaction
import io.klaytn.client.FinderRedis
import io.klaytn.model.ChainPhase
import io.klaytn.persistent.InternalTransactionPersistentAPI
import io.klaytn.service.{InternalTransactionService, LoadDataInfileService}
import io.klaytn.utils.spark.KafkaStreamingHelper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object InternalTXToDBStreaming extends KafkaStreamingHelper {
  private val chainPhase = ChainPhase.get()
  private val persistentAPI = InternalTransactionPersistentAPI.of(chainPhase)
  private val service = new InternalTransactionService(persistentAPI)
  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .mapPartitions { iter =>
            val result = ArrayBuffer.empty[(Int, (String, String))]

            val logSeq = iter
              .filter(_.key().toLong >= LastProcessedBlockNumber
                .getLastProcessedBlockNumber())
              .toSeq
            getInternalTransaction(logSeq).foreach { x =>
              val refinedInternalTransactions = x.toRefined()
              if (refinedInternalTransactions.length < 1000) {
                service.saveInternalTransactionToMysql(
                  refinedInternalTransactions)
                service.saveInternalTransactionIndexToMysql(
                  refinedInternalTransactions)
              } else {
                val (itxLines, itxIndexLines) =
                  loadDataInfileService.internalTransactionLines(
                    refinedInternalTransactions)

                val itxLinesByDB =
                  mutable.Map.empty[String, ArrayBuffer[String]]
                val itxIndexLinesByDB =
                  mutable.Map.empty[String, ArrayBuffer[String]]

                itxLines.foreach { itxLine =>
                  val dbName =
                    persistentAPI.getInternalTransactionDB(x.blockNumber)
                  itxLinesByDB
                    .getOrElseUpdate(dbName, ArrayBuffer.empty[String])
                    .append(itxLine)
                }

                itxIndexLines.foreach {
                  case (address, itxIndexLine) =>
                    val dbName =
                      persistentAPI.getInternalTransactionIndexDB(address)
                    itxIndexLinesByDB
                      .getOrElseUpdate(dbName, ArrayBuffer.empty[String])
                      .append(itxIndexLine)
                }

                itxLinesByDB.foreach {
                  case (dbName, lines) =>
                    val file =
                      loadDataInfileService.writeLoadData(jobBasePath,
                                                          "itx",
                                                          x.blockNumber,
                                                          lines)
                    if (file.isDefined) {
                      val shardNo = dbName.replaceFirst("finder02", "").toInt
                      result.append((shardNo, (dbName, file.get)))
                    }
                }

                itxIndexLinesByDB.foreach {
                  case (dbName, lines) =>
                    val file =
                      loadDataInfileService.writeLoadData(jobBasePath,
                                                          "itxindex",
                                                          x.blockNumber,
                                                          lines)
                    if (file.isDefined) {
                      val rand = Random.nextInt(2)
                      val pno = dbName match {
                        case "finder0201" => if (rand == 0) 0 else 4
                        case "finder0202" => if (rand == 0) 1 else 5
                        case "finder0203" => if (rand == 0) 2 else 6
                        case "finder0204" => if (rand == 0) 3 else 7
                        case "finder0205" => if (rand == 0) 3 else 7
                        case _            => if (rand == 0) 2 else 6
                      }
                      result.append((pno, (dbName, file.get)))
                    }
                }
              }

              result.append((2, ("blockNumber", x.blockNumber.toString)))
            }

            result.iterator
          }
          .groupByKey(8)
          .foreach { iter =>
            val blockNumbers = ArrayBuffer.empty[Long]
            iter._2.foreach {
              case (dbName, file) =>
                if (dbName != "blockNumber") {
                  loadDataInfileService.loadDataAndDeleteFile(file,
                                                              Some(dbName))
                } else {
                  blockNumbers.append(file.toLong)
                }
            }

            if (blockNumbers.nonEmpty) {
              val currentBlockNumber = blockNumbers.max
              val blockNumber = FinderRedis.get("latest:internal-tx") match {
                case Some(blockNumber) => blockNumber.toLong
                case _                 => 0L
              }
              if (blockNumber < currentBlockNumber) {
                LastProcessedBlockNumber.setLastProcessedBlockNumber(
                  currentBlockNumber)
                FinderRedis.publish("channel:internal-tx",
                                    currentBlockNumber.toString)
                FinderRedis.set("latest:internal-tx",
                                currentBlockNumber.toString)
              }
            }
          }
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
