package io.klaytn.apps.restore.eventLog

import io.klaytn.model.RefinedEventLog
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.KafkaStreamingHelper

object EventLogRestore extends KafkaStreamingHelper {

  import EventLogRestoreDeps._

  private def restoreEventLogs(id: Long, limit: Long): Int = {
    val blocks = blockPersistentAPI.getBlocksByRange(id, id + limit)
    val eventLogsByBlockNumber = eventLogPersistentAPI
      .getEventLogsByBlockRange(id, id + limit)
      .map(log => (log.blockNumber, log.transactionHash, log.logIndex))
      .groupBy(_._1)
    val totalInsertList =
      scala.collection.mutable.ListBuffer.empty[RefinedEventLog]
    sc.parallelize(blocks, 100)
      .mapPartitions(blocks => {
        val insertList =
          scala.collection.mutable.ListBuffer.empty[RefinedEventLog]
        blocks.foreach(block => {
          val logs = caverService.getEventLogsByBlock(block)
          val logsHaving = eventLogsByBlockNumber
            .getOrElse(block.number, List.empty)
            .map(l => (l._2, l._3))
            .toSet
          val logsToAdd = logs.filter(log =>
            !logsHaving.contains((log.transactionHash, log.logIndex)))
          insertList ++= logsToAdd
        })
        insertList.toIterator
      })
      .collect()
      .map(res => {
        totalInsertList += res
      })

    totalInsertList.grouped(1000).foreach { x =>
      eventLogPersistentAPI.insertEventLogs(x)
    }
    totalInsertList.size
  }

  override def run(args: Array[String]): Unit = {
    val cnt = 10000
    7000L to 15000L foreach { x =>
      val t1 = System.currentTimeMillis()
      val inserted = restoreEventLogs(x * cnt, cnt)
      try {
        SlackUtil.sendMessage(
          s"restoreEventLogs: ${x * cnt} ~ ${x * cnt + cnt} is done. ${System
            .currentTimeMillis() - t1} ms, $chainPhase, $inserted Inserted"
        )
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
