package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.RefinedEventLog
import io.klaytn.utils.JsonUtil
import io.klaytn.repository.TransactionRepository

object EventLogRepository {
  val EventLogDB = "finder03"
  val EventLogTable = "event_logs"
}

abstract class EventLogRepository extends AbstractRepository {
  import EventLogRepository._

  def insertEventLogs(eventLogs: Seq[RefinedEventLog]): Unit = {
    withDB(EventLogDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $EventLogTable (`address`,`block_hash`,`block_number`,`signature`,`data`,`log_index`,`topics`," +
          "`transaction_hash`,`transaction_index`,`removed`) VALUES (?,?,?,?,?,?,?,?,?,?)"
      )

      eventLogs.zipWithIndex.foreach {
        case (eventLog, index) =>
          pstmt.setString(1, eventLog.address)
          pstmt.setString(2, eventLog.blockHash)
          pstmt.setLong(3, eventLog.blockNumber)
          pstmt.setString(4,
                          if (eventLog.topics.isEmpty) ""
                          else eventLog.topics.head)
          pstmt.setString(5, eventLog.data)
          pstmt.setInt(6, eventLog.logIndex)
          pstmt.setString(7, JsonUtil.asJson(eventLog.topics))
          pstmt.setString(8, eventLog.transactionHash)
          pstmt.setInt(9, eventLog.transactionIndex)
          eventLog.removed match {
            case Some(removed) => pstmt.setBoolean(10, removed)
            case _             => pstmt.setNull(10, java.sql.Types.INTEGER)
          }

          pstmt.addBatch()
          pstmt.clearParameters()

          if ((index + 1) % 3000 == 0) {
            execute(pstmt)
          }
      }

      execute(pstmt)
      pstmt.close()
    }
  }

  def getEventLogsByBlockRange(startBlock: Long,
                               endBlock: Long): Seq[RefinedEventLog] = {
    val eventLogs = scala.collection.mutable.ListBuffer.empty[RefinedEventLog]
    withDB(EventLogDB) { c =>
      val pstmt = c.prepareStatement(
        s"""SELECT el.block_number, el.block_hash, el.transaction_hash, el.transaction_index, tx.status, el.address, el.topics, el.data, el.log_index, tx.timestamp, el.removed FROM
        $EventLogTable el JOIN ${TransactionRepository.TransactionTable} tx ON tx.transaction_hash = el.transaction_hash
          WHERE tx.block_number BETWEEN ? AND ?
          ORDER BY tx.block_number ASC;
          """
      )
      pstmt.setLong(1, startBlock)
      pstmt.setLong(2, endBlock)

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        val topics: Seq[String] = JsonUtil
          .fromJson[Seq[String]](rs.getString("topics"))
          .getOrElse(Seq.empty[String])

        val eventLog = RefinedEventLog(
          label = "event_logs",
          bnp = (rs.getLong("block_number") / 10000L).toString,
          blockHash = rs.getString("block_hash"),
          blockNumber = rs.getLong("block_number"),
          transactionHash = rs.getString("transaction_hash"),
          transactionIndex = rs.getInt("transaction_index"),
          transactionStatus = rs.getBoolean("status"),
          address = rs.getString("address"),
          topics = topics,
          data = rs.getString("data"),
          logIndex = rs.getInt("log_index"),
          timestamp = rs.getInt("timestamp"),
          removed = Option(rs.getBoolean("removed"))
        )
        eventLogs += eventLog
      }
    }
    return eventLogs
  }

}
