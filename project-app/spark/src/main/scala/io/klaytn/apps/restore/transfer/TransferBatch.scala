package io.klaytn.apps.restore.transfer

import io.klaytn.apps.restore.bulkload.BulkLoadHelper
import io.klaytn.dsl.db.withDB
import io.klaytn.model.Block
import io.klaytn.model.finder.TransferType
import io.klaytn.repository.AccountTransferContracts
import io.klaytn.utils.spark.SparkHelper
import io.klaytn.utils.{SlackUtil, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
--driver-memory 10g
--num-executors 40
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.transfer.TransferBatch
 */
object TransferBatch extends SparkHelper with BulkLoadHelper {
  import TransferBatchDeps._

  def tokenTransfer(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .repartition(noPartition)
        .foreachPartition { iter =>
          iter
            .flatMap { line =>
              Block.parse(line) match {
                case Some(block) =>
                  //                  if (block.blockNumber >= 86637000L) {
                  //                    Seq.empty
                  //                  } else {
                  val refinedData = block.toRefined
                  val refinedEventLogs = refinedData._3
                  refinedEventLogs.filter(log => log.isTokenTransferEvent)
                //                  }
                case _ =>
                  Seq.empty
              }
            }
            .grouped(500)
            .foreach { x =>
              val m = ArrayBuffer.empty[Int]
              transferPersistentAPI.insertTokenTransferEvent(x, m)
            }
        }
    }
  }

  def tokenTransferSaveAsFile(rdd: RDD[String], bnp: Int): Unit = {
    rdd
      .flatMap { line =>
        Block.parse(line) match {
          case Some(block) =>
            val refinedData = block.toRefined
            val tokenTransferEventLogs =
              refinedData._3.filter(_.isTokenTransferEvent)

            tokenTransferEventLogs.map { eventLog =>
              val (address, from, to, _, _) = eventLog.extractTransferInfo()
              val amount = eventLog.data

              s"$address\t$from\t$to\t$amount\t${eventLog.timestamp}\t${eventLog.blockNumber}\t${eventLog.transactionHash}\t" +
                s"${Utils.getDisplayOrder(eventLog.blockNumber, eventLog.transactionIndex, eventLog.logIndex)}"
            }
          case _ =>
            Seq.empty
        }
      }
      .saveAsTextFile(s"gs://${outputDirPrefix()}/token_transfers2/$bnp")
  }

  def tokenTransferSaveAsFile(bnp: Int, noPartition: Int): Unit = {
    val rdd = sc
      .textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
      .repartition(noPartition)
    tokenTransferSaveAsFile(rdd, bnp)
  }

  def nftTransfer(bnp: Int, noPartition: Int): Unit = {
    withDB("finder03") { c =>
      val startBlock = bnp * 100000
      val endBlock = startBlock + 100000
      val pstmt =
        c.prepareStatement(
          s"delete from nft_transfers where block_number >= $startBlock and block_number < $endBlock")
      pstmt.execute()
      pstmt.close()

    }

    val rdd = sc
      .textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
      .repartition(noPartition)
    // nft transfer
    rdd.repartition(noPartition).foreachPartition { iter =>
      iter
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
              //                if (block.blockNumber >= 86637000L) {
              //                  Seq.empty
              //                } else {
              val refinedData = block.toRefined
              val refinedEventLogs = refinedData._3
              refinedEventLogs.filter(log => log.isNFTTransferEvent)
            //                }
            case _ =>
              Seq.empty
          }
        }
        .grouped(500)
        .foreach(transferService.insertNFTTransferToMysql)
    }
  }

  def nftTransferSaveAsFile(rdd: RDD[String], bnp: Int): Unit = {
    rdd
      .flatMap { line =>
        Block.parse(line) match {
          case Some(block) =>
            val refinedData = block.toRefined
            val nftEventLogs = refinedData._3.filter(_.isNFTTransferEvent)

            nftEventLogs.flatMap { eventLog =>
              val (address, from, to, tokenIds, amounts) =
                eventLog.extractTransferInfo()
              tokenIds.zipWithIndex.map {
                case (tokenId, idx) =>
                  s"$address\t$from\t$to\t${amounts(idx)}\t$tokenId\t${eventLog.timestamp}\t${eventLog.blockNumber}\t" +
                    s"${eventLog.transactionHash}\t${Utils.getDisplayOrder(eventLog.blockNumber, eventLog.transactionIndex, eventLog.logIndex)}"

              }
            }
          case _ =>
            Seq.empty
        }
      }
      .saveAsTextFile(s"gs://${outputDirPrefix()}/nft_transfers2/$bnp")
  }

  def nftTransferSaveAsFile(bnp: Int, noPartition: Int): Unit = {
    val rdd = sc
      .textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
      .repartition(noPartition)
    // nft transfer
    nftTransferSaveAsFile(rdd, bnp)
  }

  def tokenTransferAndNFTTransferSaveAsFile(bnp: Int,
                                            noPartition: Int): Unit = {
    val rdd = sc
      .textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
      .repartition(noPartition)

    rdd.cache()

    // nft transfer
    tokenTransferSaveAsFile(rdd, bnp)
    // nft transfer
    nftTransferSaveAsFile(rdd, bnp)

    spark.catalog.clearCache()
    rdd.unpersist()
  }

  def accountTransferContracts(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .repartition(noPartition)
        .mapPartitions { iter =>
          iter
            .flatMap { line =>
              Block.parse(line) match {
                case Some(block) =>
                  //                  if (block.blockNumber >= 86637000L) {
                  //                    Seq.empty
                  //                  } else {
                  val refinedData = block.toRefined

                  val refinedEventLogs = refinedData._3

                  val m = mutable.Map.empty[String, Int]
                  refinedEventLogs
                    .filter(log =>
                      log.isTokenTransferEvent || log.isNFTTransferEvent)
                    .foreach { eventLog =>
                      try {
                        val (address, from, to, _, _) =
                          eventLog.extractTransferInfo()
                        val isTokenTransfer = eventLog.isTokenTransferEvent

                        if (m.getOrElse(s"${from}_$address", 0) < eventLog.timestamp) {
                          m.put(s"${from}_${address}_$isTokenTransfer",
                                eventLog.timestamp)
                        }

                        if (m.getOrElse(s"${to}_$address", 0) < eventLog.timestamp) {
                          m.put(s"${to}_${address}_$isTokenTransfer",
                                eventLog.timestamp)
                        }
                      } catch {
                        case e: Throwable =>
                          val msg =
                            s"""batch: ${this.getClass.getName.stripSuffix("$")}
                               |bnp: $bnp
                               |block: ${block.blockNumber}
                               |eventLogIndex: ${eventLog.logIndex}
                               |txIndex: ${eventLog.transactionIndex}
                               |topics: ${eventLog.topics.mkString(",")}
                               |error: ${e.getLocalizedMessage}
                               |stackTrace: ${StringUtils.abbreviate(
                                 ExceptionUtils.getStackTrace(e),
                                 500)}
                               |""".stripMargin
                          SlackUtil.sendMessage(msg)
                      }
                    }
                  m.toSeq
                //                  }
                case _ =>
                  Seq.empty
              }
            }
        }
        .groupByKey(noPartition)
        .foreachPartition { iter =>
          iter
            .map {
              case (k, v) =>
                val s = k.split("_")
                val (accountAddress, contractAddress, isTokenTransfer) =
                  (s(0), s(1), s(2).toBoolean)
                val ts = v.max

                val transferType =
                  if (isTokenTransfer) TransferType.TOKEN else TransferType.NFT
                AccountTransferContracts(accountAddress,
                                         contractAddress,
                                         ts,
                                         transferType)
            }
            .grouped(500)
            .foreach { accountTransferContracts =>
              transferPersistentAPI.insertAccountTransferContracts(
                accountTransferContracts)
            }
        }
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    val noPartition = 32

//    start to end foreach { bnp =>
//      tokenTransferAndNFTTransferSaveAsFile(bnp, noPartition)
//    }

//    start to end foreach { bnp =>
//      nftTransferSaveAsFile(bnp, noPartition)
//    }
//    start to end foreach { bnp =>
//      tokenTransferSaveAsFile(bnp, noPartition)
//    }

    start to end foreach { bnp =>
      tokenTransfer(bnp, noPartition)
    }

    start to end foreach { bnp =>
      nftTransfer(bnp, noPartition)
    }

    start to end foreach { bnp =>
      accountTransferContracts(bnp, noPartition)
    }
  }
}
