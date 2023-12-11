package io.klaytn.apps.adhoc.restore

import io.klaytn.model.RefinedEventLog
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper

object RestoreEventLogBatch extends SparkHelper {
  import RestoreEventLogBatchDeps._

  override def run(args: Array[String]): Unit = {

    SlackUtil.sendMessage(s"""
                             |Start Job: RestoreEventLogBatch
                             |""".stripMargin)

    try {
      import spark.implicits._
      val df =
        spark.read.parquet("gs://klaytn-prod-lake/klaytn/label=event_logs/")

      //  [address, blockHash, blockNumber, data, logIndex, timestamp, topics, transactionHash, transactionIndex, transactionStatus]
      df.map { row =>
          RefinedEventLog(
            label = "",
            bnp = "",
            blockHash = row.getAs[String]("blockHash"),
            blockNumber = row.getAs[Long]("blockNumber"),
            transactionHash = row.getAs[String]("transactionHash"),
            transactionIndex = row.getAs[Int]("transactionIndex"),
            transactionStatus = row.getAs[Boolean]("transactionStatus"),
            address = row.getAs[String]("address"),
            topics = row.getAs[Seq[String]]("topics"),
            data = row.getAs[String]("data"),
            logIndex = row.getAs[Int]("logIndex"),
            timestamp = row.getAs[Int]("timestamp"),
            None
          )
        }
        .rdd
        .foreachPartition { iter =>
          iter
            .sliding(500, 500)
            .foreach({ l =>
              eventLogPersistentAPI.insertEventLogs(l)
              transferService.insertTransferToMysql(l)
            })
        }
    } catch {
      case e: Exception =>
        SlackUtil.sendMessage(s"""
                                                    |Job: RestoreEventLogBatch
                                                    |Processing Failed. reason: ${e.getMessage}
                                                    |""".stripMargin)
    }

    SlackUtil.sendMessage(s"""
                             |End Job: RestoreEventLogBatch
                             |""".stripMargin)

  }
}
