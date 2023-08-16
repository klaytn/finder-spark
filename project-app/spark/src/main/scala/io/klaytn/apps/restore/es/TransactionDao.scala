package io.klaytn.apps.restore.es

import io.klaytn.dsl.db.withDB
import scala.collection.mutable

private[es] class TransactionDao(dbName: String,
                                 tableName: String,
                                 errorReporter: String => Unit)
    extends Serializable {

  def findRecoveryTargets(fromIdInclusive: Long,
                          toIdExclusive: Long,
                          minBlockNumber: Long): Seq[TransactionDTO] = {
    val targets = mutable.ListBuffer[TransactionDTO]()
    withDB(dbName) { conn =>
      val query = s"SELECT " +
        s"`block_hash`, `block_number`, `transaction_hash`, " +
        s"`status`, `type_int`, `chain_id`, `contract_address`, " +
        s"`fee_payer`, `from`, `to`, `sender_tx_hash`, `timestamp`, `transaction_index` " +
        s"FROM $tableName " +
        s"WHERE id >= $fromIdInclusive " +
        s"AND id < $toIdExclusive " +
        s"AND block_number >= $minBlockNumber"
      val statement = conn.prepareStatement(query)
      val rs = statement.executeQuery()
      while (rs.next()) {
        targets.append(
          TransactionDTO(
            block_hash = rs.getString(1),
            block_number = rs.getLong(2),
            transaction_hash = rs.getString(3),
            status = if (rs.getInt(4) == 1) true else false,
            type_int = rs.getInt(5),
            chain_id = Option(rs.getString(6)),
            contract_address = Option(rs.getString(7)),
            fee_payer = Option(rs.getString(8)),
            from = rs.getString(9),
            to = Option(rs.getString(10)),
            sender_tx_hash = rs.getString(11),
            timestamp = rs.getLong(12),
            transaction_index = rs.getInt(13)
          ))
      }
      rs.close()
      statement.close()
      targets
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
