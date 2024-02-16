package io.klaytn.apps.restore.holder

import io.klaytn.apps.restore.bulkload.BulkLoadHelper
import io.klaytn.dsl.db.withDB
import io.klaytn.model.Block
import io.klaytn.model.finder.ContractType.{KIP17, KIP37}
import io.klaytn.model.finder.{ContractType, TokenTransfer}
import io.klaytn.repository._
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.config.Constants
import io.klaytn.utils.klaytn.NumberConverter.StringConverter
import io.klaytn.utils.spark.SparkHelper
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.TaskContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
--driver-memory 10g
--num-executors 20
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.holder.HolderBatch
 */
object HolderBatch extends SparkHelper with BulkLoadHelper {
  import HolderBatchDeps._

  def tokenHolder(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
              //              if (block.blockNumber >= 86643000L) {
              //                Seq.empty
              //              } else {
              val result = mutable.ArrayBuffer
                .empty[(String, (Boolean, String, Int, Long))]
              val refinedEventLogs = block.toRefined._3

              refinedEventLogs.filter(log => log.isTokenTransferEvent).foreach {
                eventLog =>
                  val (address, from, to, _, _) = eventLog.extractTransferInfo()
                  if (from != Constants.ZeroAddress) {
                    result.append(
                      (s"${address}_$from",
                       (false,
                        eventLog.data,
                        eventLog.timestamp,
                        eventLog.blockNumber)))
                  }
                  result.append(
                    (s"${address}_$to",
                     (true,
                      eventLog.data,
                      eventLog.timestamp,
                      eventLog.blockNumber)))
              }
              result
            //              }
            case _ =>
              Seq.empty
          }
        }
        .groupByKey(noPartition)
        .foreachPartition { iter =>
          iter
            .map {
              case (k, v) =>
                val s = k.split("_")
                val (contractAddress, holderAddress) = (s(0), s(1))

                val summary = v.toSeq.map {
                  case (add, amount0, ts, blockNumber) =>
                    import io.klaytn.utils.klaytn.NumberConverter._
                    val amount = amount0.hexToBigInt()
                    if (add) (amount, ts, blockNumber)
                    else (-amount, ts, blockNumber)
                }
                val amount = summary.map(_._1).sum
                val timestamp = summary.map(_._2).max
                val blockNumber = summary.map(_._3).max
                TokenHolders(contractAddress,
                             holderAddress,
                             amount,
                             timestamp,
                             blockNumber)
            }
            .grouped(100)
            .foreach(tokenHolders =>
              holderPersistentAPI.insertTokenHolders(
                tokenHolders.filter(_.holderAddress != Constants.ZeroAddress)))
        }
    }
  }

  def nftHolder(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
              //              if (block.blockNumber >= 86643000L) {
              //                Seq.empty
              //              } else {
              val result = mutable.ArrayBuffer
                .empty[(String, (Boolean, BigInt, String, Int, Long))]
              val refinedEventLogs = block.toRefined._3

              refinedEventLogs.filter(log => log.isNFTTransferEvent).foreach {
                eventLog =>
                  try {
                    val (address, from, to, tokenIds, amounts) =
                      eventLog.extractTransferInfo()

                    val contract = contractService.getMaybeNFTContract(address)
                    val contractType = contract.contractType.id

                    for (i <- tokenIds.indices) {
                      val tokenId = tokenIds(i)
                      val amount = amounts(i)

                      if (from != Constants.ZeroAddress) {
                        result.append(
                          (s"${address}_${from}_$contractType",
                           (false,
                            tokenId,
                            amount,
                            eventLog.timestamp,
                            eventLog.blockNumber)))
                      }
                      result.append(
                        (s"${address}_${to}_$contractType",
                         (true,
                          tokenId,
                          amount,
                          eventLog.timestamp,
                          eventLog.blockNumber)))
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
                         |stackTrace: ${StringUtils
                             .abbreviate(ExceptionUtils.getStackTrace(e), 500)}
                         |""".stripMargin
                      SlackUtil.sendMessage(msg)
                  }
              }

              result
            //              }
            case _ =>
              Seq.empty
          }
        }
        .groupByKey(noPartition)
        .foreachPartition { iter =>
          iter
            .map {
              case (k, v) =>
                val s = k.split("_")
                val (contractAddress, holderAddress, contractType) =
                  (s(0), s(1), s(2).toInt)

                val summary = mutable.Map
                  .empty[(String, String, BigInt), (BigInt, String, Int, Long)]
                v.foreach {
                  case (add, tokenId, amount0, ts, blockNumber) =>
                    import io.klaytn.utils.klaytn.NumberConverter._
                    val amount =
                      if (add) amount0.hexToBigInt() else -amount0.hexToBigInt()

                    val key =
                      if (ContractType
                            .from(contractType) == ContractType.KIP17) {
                        (contractAddress, holderAddress, BigInt(-1))
                      } else {
                        (contractAddress, holderAddress, tokenId)
                      }

                    val uri = {
                      val originUri = if (summary.contains(key)) {
                        summary(key)._2
                      } else {
                        if (ContractType.from(contractType) == ContractType.KIP17) {
                          "-"
                        } else {
                          contractService.getTokenUri(
                            ContractType.from(contractType),
                            contractAddress,
                            tokenId)
                        }
                      }

                      if (originUri.length > 255) {
                        val msg = s"""<modify to abbreviate uri>
                                     |key: $k
                                     |value: $v
                                     |tokenId: $tokenId
                                     |origin_uri: $originUri
                                     |origin_uri_length: ${originUri.length}
                                     |""".stripMargin
                        SlackUtil.sendMessage(msg)
                        val abbreviated = originUri.substring(0, 255)
                        abbreviated
                      } else {
                        originUri
                      }
                    }
                    val value = summary.getOrElse(key, (BigInt(0), "", 0, 0L))
                    summary.put(key,
                                (value._1 + amount,
                                 uri,
                                 if (value._3 > ts) value._3 else ts,
                                 if (value._4 > blockNumber) value._4
                                 else blockNumber))
                }
                val result = summary.map {
                  case ((contractAddress, holderAddress, tokenId),
                        (tokenCount, uri, timestamp, blockNumber)) =>
                    NFTHolders(contractAddress,
                               holderAddress,
                               tokenId,
                               tokenCount,
                               uri,
                               timestamp,
                               blockNumber)
                }.toSeq

                (contractType, result)
            }
            .grouped(100)
            .foreach { x =>
              val kip17Holders =
                x.filter(x => ContractType.from(x._1) == KIP17).flatMap(_._2)
              val kip37Holders =
                x.filter(x => ContractType.from(x._1) == KIP37).flatMap(_._2)
              if (kip17Holders.nonEmpty) {
                holderPersistentAPI.insertKIP17Holders(
                  kip17Holders.filter(_.holderAddress != Constants.ZeroAddress))
              }
              if (kip37Holders.nonEmpty) {
                holderPersistentAPI.insertKIP37Holders(
                  kip37Holders.filter(_.holderAddress != Constants.ZeroAddress))
              }
            }
        }
    }
  }

  def nftInventories(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"gs://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
              //              if (block.blockNumber >= 86643000L) {
              //                Seq.empty
              //              } else {
              val result = mutable.Map.empty[String, (BigInt, Int)]
              val refinedEventLogs = block.toRefined._3

              refinedEventLogs.filter(log => log.isNFTTransferEvent).foreach {
                eventLog =>
                  try {
                    val (address, from, to, tokenIds, amounts) =
                      eventLog.extractTransferInfo()

                    val contract = contractService.getMaybeNFTContract(address)
                    val contractType = contract.contractType

                    if (contractType == KIP17) {
                      for (i <- tokenIds.indices) {
                        import io.klaytn.utils.klaytn.NumberConverter._

                        val tokenId = tokenIds(i)
                        val amount = amounts(i).hexToBigInt()

                        if (from != Constants.ZeroAddress) {
                          val key = s"${address}_${from}_$tokenId"
                          val value = result.getOrElse(key, (BigInt(0), 0))
                          result.put(key,
                                     (value._1 - amount,
                                      if (value._2 > eventLog.timestamp)
                                        value._2
                                      else eventLog.timestamp))
                        }
                        val key = s"${address}_${to}_$tokenId"
                        val value = result.getOrElse(key, (BigInt(0), 0))
                        result.put(key,
                                   (value._1 + amount,
                                    if (value._2 > eventLog.timestamp) value._2
                                    else eventLog.timestamp))
                      }
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
                         |stackTrace: ${StringUtils
                             .abbreviate(ExceptionUtils.getStackTrace(e), 500)}
                         |""".stripMargin
                      SlackUtil.sendMessage(msg)
                  }
              }

              result.toSeq.filter(x => x._2._1 != BigInt(0))
            //              }
            case _ =>
              Seq.empty
          }
        }
        .groupByKey(noPartition)
        .foreachPartition { iter =>
          iter
            .flatMap {
              case (k, v) =>
                val s = k.split("_")
                val (contractAddress, holderAddress, tokenId) =
                  (s(0), s(1), BigInt(s(2)))

                //                   (contractAddress, holderAddress, tokenId), (tokenCount, timestamp)
                val summary =
                  mutable.Map.empty[(String, String, BigInt), (BigInt, Int)]
                v.foreach {
                  case (amount, ts) =>
                    val key = (contractAddress, holderAddress, tokenId)

                    val value = summary.getOrElse(key, (BigInt(0), 0))
                    summary.put(
                      key,
                      (value._1 + amount, if (value._2 > ts) value._2 else ts))
                }

                summary.flatMap {
                  case ((contractAddress, holderAddress, tokenId),
                        (tokenCount, timestamp)) =>
                    if (tokenCount > 0) { // Received NFT
                      val uri = contractService.getTokenUri(KIP17,
                                                            contractAddress,
                                                            tokenId)
                      Some(
                        NFTInventories(isSend = false,
                                       contractAddress,
                                       holderAddress,
                                       tokenId,
                                       uri,
                                       timestamp))
                    } else if (tokenCount < 0) { // Sent NFT
                      // Don't get uri since removed
                      Some(
                        NFTInventories(isSend = true,
                                       contractAddress,
                                       holderAddress,
                                       tokenId,
                                       "-",
                                       timestamp))
                    } else { // if count is 0 no need to process
                      None
                    }
                }.toSeq
            }
            .grouped(100)
            .foreach(x =>
              holderPersistentAPI.insertKIP17Inventories(
                x.filter(_.holderAddress != Constants.ZeroAddress)))
        }
    }
  }

  def nftHolderAndInventoriesByDB(): Unit = {
    val phase = "baobab"
    sc.textFile(s"gs://${kafkaLogDirPrefix()}/output/nft_transfers/$phase/*")
      .flatMap { line =>
        val s = line.split("\t")
        val (id,
             contractType,
             contractAddress,
             from,
             to,
             tokenCount,
             tokenId,
             ts,
             blockNumber,
             txHash) =
          (s(0).toLong,
           s(1).toInt,
           s(2),
           s(3),
           s(4),
           s(5),
           BigInt(s(6)),
           s(7).toInt,
           s(8).toLong,
           s(9))

        Seq(
          (s"$contractAddress\t$from\t$contractType\t$tokenId",
           s"$tokenCount\t$ts"),
          (s"$contractAddress\t$to\t$contractType\t$tokenId",
           s"$tokenCount\t$ts")
        )
      }
      .groupByKey(16)
      .map { x =>
        }
  }

  def tokenHolderByDB(): Unit = {
    9154 to 9154 foreach { idx =>
      val startBlock = idx * 10000
      val endBlock = startBlock + 10000

      if (idx % 20 == 0) {
        SlackUtil.sendMessage(s"baobab token holder: $startBlock ~ $endBlock")
      }

      val tokenTransfers = withDB("finder03") { c =>
        val tokenTransfers = ArrayBuffer.empty[TokenTransfer]
        val updateAmounts = ArrayBuffer.empty[(Long, String)]

        val pstmt = c.prepareStatement(
          s"SELECT * FROM token_transfers WHERE block_number >= $startBlock and block_number < $endBlock")
        val rs = pstmt.executeQuery()

        while (rs.next()) {
          tokenTransfers.append(
            TokenTransfer(
              rs.getLong("id"),
              rs.getString("contract_address"),
              rs.getString("from"),
              rs.getString("to"),
              rs.getString("amount"),
              rs.getInt("timestamp"),
              rs.getLong("block_number"),
              rs.getString("transaction_hash"),
              rs.getString("display_order")
            ))

          if (!rs.getString("amount").startsWith("0x")) {
            updateAmounts.append((rs.getLong("id"), rs.getString("amount")))
          }
        }

        rs.close()
        pstmt.close()

        tokenTransfers
      }.sortBy(_.timestamp)

      if (tokenTransfers.nonEmpty) {
        // key: contractAddress_[from or to]
        // value: amount_ts
        val m = mutable.Map.empty[String, (BigInt, Int, Long)]
        tokenTransfers.foreach { t =>
          val amount = t.amount.hexToBigInt()
          if (t.from != Constants.ZeroAddress) {
            holderService.calAmount(m,
                                    s"${t.contractAddress}\t${t.from}",
                                    -amount,
                                    t.timestamp,
                                    t.blockNumber)
          }
          holderService.calAmount(m,
                                  s"${t.contractAddress}\t${t.to}",
                                  amount,
                                  t.timestamp,
                                  t.blockNumber)
        }
        val holders = m.map {
          case (k, v) =>
            val s1 = k.split("\t")

            val (contractAddress, holder, amount, ts, blockNumber) =
              (s1(0), s1(1), v._1, v._2, v._3)

            TokenHolders(contractAddress, holder, amount, ts, blockNumber)
        }.toSeq

        holderPersistentAPI.insertTokenHolders(
          holders.filter(_.holderAddress != Constants.ZeroAddress))
      }
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    val noPartition = 80

    start to end foreach { bnp =>
      tokenHolder(bnp, noPartition)
    }
    start to end foreach { bnp =>
      nftHolder(bnp, noPartition)
    }
    start to end foreach { bnp =>
      nftInventories(bnp, noPartition)
    }
  }
}
