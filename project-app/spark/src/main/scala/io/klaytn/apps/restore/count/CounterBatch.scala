package io.klaytn.apps.restore.count

import io.klaytn.apps.restore.bulkload.BulkLoadHelper
import io.klaytn.dsl.db.withDB
import io.klaytn.model.Block
import io.klaytn.repository.ContractRepository
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable

/*
--driver-memory 10g
--num-executors 40
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.count.CounterBatch
 */
object CounterBatch extends SparkHelper with BulkLoadHelper {
  import CounterBatchDeps._

  def totalTransfer(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"s3a://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
//              if (block.blockNumber >= 87198602L) {
//                Seq.empty
//              } else {
              val refinedEventLogs = block.toRefined._3
              refinedEventLogs
                .filter(e => e.isTokenTransferEvent || e.isNFTTransferEvent)
                .flatMap { e =>
                  try {
                    val (address, _, _, _, _) = e.extractTransferInfo()
                    Some((address, 1))
                  } catch {
                    case ex: Throwable =>
                      val msg =
                        s"""batch: ${this.getClass.getName.stripSuffix("$")}
                         |bnp: $bnp
                         |block: ${block.blockNumber}
                         |eventLogIndex: ${e.logIndex}
                         |txIndex: ${e.transactionIndex}
                         |topics: ${e.topics.mkString(",")}
                         |error: ${ex.getLocalizedMessage}
                         |stackTrace: ${StringUtils.abbreviate(
                             ExceptionUtils.getStackTrace(ex),
                             500)}
                         |""".stripMargin
                      SlackUtil.sendMessage(msg)
                      None
                  }
                }
//              }
            case _ =>
              Seq.empty
          }
        }
        .reduceByKey(_ + _, noPartition)
        .foreachPartition { iter =>
          iter
            .grouped(500)
            .foreach(x => contractPersistentAPI.updateTotalTransfer(x))
        }
    }
  }

  def totalTransaction(bnp: Int, noPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"s3a://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    if (!rdd.isEmpty()) {
      rdd
        .flatMap { line =>
          Block.parse(line) match {
            case Some(block) =>
//              if (block.blockNumber >= 86629000L) {
//                Seq.empty
//              } else {
              val refinedTransactionReceipts = block.toRefined._2

              refinedTransactionReceipts.map { tx =>
                (tx.from, 1L)
              }
//              }
            case _ =>
              Seq.empty
          }
        }
        .reduceByKey(_ + _, noPartition)
        .repartition(8)
        .foreachPartition { iter =>
          iter
            .grouped(500)
            .foreach(x => accountPersistentAPI.updateTotalTXCountBatch(x))
        }
    }
  }

  def holderCount(): Unit = {
    holderService.updateHolderCount()
  }

  def totalTransferByDB(): Unit = {
    val transferCount = mutable.Map.empty[String, Long]

    withDB("finder03") { c =>
      val pstmt =
        c.prepareStatement(
          "select contract_address, count(*) from token_transfers group by contract_address")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        transferCount.put(rs.getString(1), rs.getLong(2))
      }
      rs.close()
      pstmt.close()

      val pstmt2 =
        c.prepareStatement(
          "select contract_address, count(*) from nft_transfers group by contract_address")
      val rs2 = pstmt2.executeQuery()
      while (rs2.next()) {
        transferCount.put(rs2.getString(1), rs2.getLong(2))
      }
      rs2.close()
      pstmt2.close()
    }

//    withDB("finder0101") { c =>
//      val pstmt =
//        c.prepareStatement("UPDATE `contracts` SET `total_transfer`=? WHERE `contract_address`=?")
//      transferCount.map {
//        case (k, v) =>
//          pstmt.setLong(1, v)
//          pstmt.setString(2, k)
//          pstmt.execute()
//      }
//    }

    transferCount.grouped(40).foreach { grouped =>
      withDB(ContractRepository.ContractDB) { c =>
        val pstmt = c.prepareStatement(
          "UPDATE `contracts` SET `total_transfer`=? WHERE `contract_address`=?")
        grouped.foreach {
          case (k, v) =>
            pstmt.setLong(1, v)
            pstmt.setString(2, k)
            pstmt.execute()
        }
        pstmt.close()
      }
      Thread.sleep(100)
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    val noPartition = 160
    start to end foreach { bnp =>
      totalTransaction(bnp, noPartition)
//      totalTransfer(bnp, noPartition)
    }
    sc.parallelize(Seq("1")).foreachPartition { x =>
      holderCount()
      totalTransferByDB()
    }
  }
}
