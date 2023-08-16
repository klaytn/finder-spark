package io.klaytn.apps.adhoc.restore

import io.klaytn.dsl.db.withDB

import scala.collection.mutable

class TransactionDao(dbName: String,
                     tableName: String,
                     errorReporter: String => Unit)
    extends Serializable {

  def findRecoveryTargetIdsGasPriceNotEquals(fromIdInclusive: Long,
                                             toIdExclusive: Long,
                                             gasPrice: String): Seq[Long] = {
    val targetIds = mutable.ListBuffer[Long]()
    withDB(dbName) { conn =>
      val query = s"SELECT id " +
        s"FROM $tableName " +
        s"WHERE id >= $fromIdInclusive " +
        s"AND id < $toIdExclusive " +
        s"AND gas_price != $gasPrice "
      val statement = conn.prepareStatement(query)
      val rs = statement.executeQuery()
      while (rs.next()) {
        targetIds.append(rs.getLong(1))
      }
      targetIds
    }
  }

  // TODO: Check condition
  def findRecoveryTargetIds(fromIdInclusive: Long,
                            toIdExclusive: Long,
                            beforeGasPrice: String,
                            optMinBlockId: Option[Long]): Seq[Long] = {
    val targetIds = mutable.ListBuffer[Long]()
    withDB(dbName) { conn =>
      val baseQuery = s"SELECT id " +
        s"FROM $tableName " +
        s"WHERE id >= $fromIdInclusive " +
        s"AND id < $toIdExclusive " +
        s"AND gas_price = $beforeGasPrice "
      val query = if (optMinBlockId.isDefined) {
        baseQuery + s"AND block_number >= ${optMinBlockId.get}"
      } else {
        baseQuery
      }
      val statement = conn.prepareStatement(query)
      val rs = statement.executeQuery()
      while (rs.next()) {
        targetIds.append(rs.getLong(1))
      }
      targetIds
    }
  }

  def updateGasPrice(ids: Seq[Long],
                     gasPrice: String,
                     bulkSize: Int = 10): Unit = {
    ids.grouped(bulkSize).foreach { grouped =>
      withDB("finder01") { c =>
        val failedIds = new mutable.ListBuffer[Long]()
        val failedReasons = new mutable.HashSet[String]()
        val pstmt =
          c.prepareStatement(s"UPDATE $dbName SET `gasPrice`=? WHERE id =?")
        grouped.foreach { id =>
          try {
            pstmt.setLong(1, id)
            pstmt.setString(2, gasPrice)
            pstmt.execute()
          } catch {
            case e: Throwable =>
              failedIds.append(id)
              failedReasons.add(e.getLocalizedMessage)
          }
        }
        pstmt.close()
        if (failedIds.nonEmpty) {
          val msg = s"""<gasPrice update failed>
                       |ids: 
                       |${failedIds.mkString(",")}
                       |
                       |reasons: 
                       |${failedReasons.mkString("\n")}
                       |""".stripMargin
          errorReporter(msg)
        }
      }

      Thread.sleep(100)
    }
  }

  def getMinId(): Long = {
    withDB(dbName) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $tableName ORDER BY id ASC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val minId = rs.getLong(1)
      rs.close()
      pstmt.close()
      minId
    }
  }

  def getMaxId(): Long = {
    withDB(dbName) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $tableName ORDER BY id DESC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val maxId = rs.getLong(1)
      rs.close()
      pstmt.close()
      maxId
    }
  }
}
