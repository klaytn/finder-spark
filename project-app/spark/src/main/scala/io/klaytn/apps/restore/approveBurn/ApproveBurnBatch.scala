package io.klaytn.apps.restore.approveBurn

import io.klaytn.apps.restore.bulkload.BulkLoadHelper
import io.klaytn.dsl.db.withDB
import io.klaytn.model.Block
import io.klaytn.model.finder.TransferType
import io.klaytn.utils.spark.SparkHelper
import io.klaytn.utils.{SlackUtil, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import io.klaytn.persistent.impl.rdb.RDBEventLogPersistentAPI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import io.klaytn.model.RefinedEventLog

/*
--driver-memory 10g
--num-executors 40
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.transfer.TransferBatch
 */
object ApproveBurnBatch extends SparkHelper with BulkLoadHelper {
  import ApproveBurnBatchDeps._

  def approveBurn(bnp: Int, noPartition: Int): Unit = {
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
                  val refinedData = block.toRefined
                  val refinedEventLogs = refinedData._3
                  refinedEventLogs
                case _ =>
                  Seq.empty
              }
            }
            .grouped(500)
            .foreach { x =>
              transferService.procApprove(x)
              transferService.procBurn(x)
            }
        }
    }
  }

  def approveBurnByDB(bnp: Int): Unit = {
    val eventLogRepository = new RDBEventLogPersistentAPI()
    val eventLogs = eventLogRepository.getEventLogsByBlockRange(
      bnp * 100000L,
      (bnp + 1) * 100000L)
    val groupedEventLogs: Seq[(Long, Seq[RefinedEventLog])] =
      eventLogs.groupBy(_.blockNumber).toSeq.sortBy(_._1)
    groupedEventLogs.foreach { x =>
      transferService.procBurn(x._2)
      transferService.procApprove(x._2)
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    val noPartition = 32

    start to end foreach { bnp =>
      approveBurn(bnp, noPartition)
    }

  }
}
