package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.RefinedInternalTransactions
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.config.Cfg
import io.klaytn.utils.{JsonUtil, ShardUtil}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class InternalTransactionForRegContract(id: Long,
                                             blockNumber: Long,
                                             from: String,
                                             to: String,
                                             transactionIndex: Int,
                                             input: String,
                                             `type`: String)

object InternalTransactionRepository {
  val InternalTXTable = "internal_transactions"
  val InternalTXIndexTable = "internal_transaction_index"
}

abstract class InternalTransactionRepository extends AbstractRepository {
  import InternalTransactionRepository._
  val ShardNo: Int = Cfg.getInt("spark.app.mysql.shardno.finder02")

  def getInternalTransactionDB(blockNumber: Long): String = {
    f"finder02${ShardUtil.blockNumberShardNumSelector(blockNumber, ShardNo)}%02d"
  }

  def getInternalTransactionIndexDB(account: String): String = {
    f"finder02${ShardUtil.accountAddressShardNumSelector(account, ShardNo)}%02d"
  }

  def getMaxId(dbName: String): Option[Long] = {
    withDB(dbName) { c =>
      val pstmt = c.prepareStatement(s"SELECT MAX(id) FROM $InternalTXTable")
      val rs = pstmt.executeQuery()
      val maxId = if (rs.next()) Some(rs.getLong(1)) else None
      rs.close()
      pstmt.close()
      maxId
    }
  }

  def getITXs(dbName: String,
              tableId: Long,
              cnt: Int): Seq[InternalTransactionForRegContract] = {
    val itxs = mutable.ArrayBuffer.empty[InternalTransactionForRegContract]
    withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `id`,`block_number`,`from`,`to`,`transaction_index`,`input`,`type` FROM $InternalTXTable WHERE id > $tableId ORDER BY id LIMIT $cnt")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        itxs.append(
          InternalTransactionForRegContract(rs.getLong(1),
                                            rs.getLong(2),
                                            rs.getString(3),
                                            rs.getString(4),
                                            rs.getInt(5),
                                            rs.getString(6),
                                            rs.getString(7)))
      }
      rs.close()
      pstmt.close()
    }
    itxs
  }

  def insert(internalTransactions: Seq[RefinedInternalTransactions],
             dbName: String): Unit = {
    withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $InternalTXTable (`block_number`,`call_id`,`error`,`from`,`gas`,`gas_used`," +
          "`transaction_index`,`input`,`output`,`parent_call_id`,`reverted`,`time`,`to`,`type`,`value`,`internal_tx_id`)" +
          " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      )

      internalTransactions.zipWithIndex.foreach {
        case (refinedInternalTransaction, idx) =>
          pstmt.setLong(1, refinedInternalTransaction.blockNumber)
          pstmt.setInt(2, refinedInternalTransaction.callId)
          pstmt.setString(3, refinedInternalTransaction.error.orNull)
          pstmt.setString(4, refinedInternalTransaction.from.get)
          refinedInternalTransaction.gas match {
            case Some(gas) => pstmt.setLong(5, gas)
            //            case _         => pstmt.setNull(5, java.sql.Types.BIGINT)
            case _ => pstmt.setLong(5, 0)
          }
          refinedInternalTransaction.gasUsed match {
            case Some(gasUsed) => pstmt.setInt(6, gasUsed)
            case _             => pstmt.setNull(6, java.sql.Types.INTEGER)
          }
          pstmt.setInt(7, refinedInternalTransaction.index)
          pstmt.setString(8, refinedInternalTransaction.input.orNull)
          pstmt.setString(9, refinedInternalTransaction.output.orNull)
          refinedInternalTransaction.parentCallId match {
            case Some(parentCallId) => pstmt.setInt(10, parentCallId)
            case _                  => pstmt.setNull(10, java.sql.Types.INTEGER)
          }
          pstmt.setString(11, refinedInternalTransaction.reverted match {
            case Some(reverted) => JsonUtil.asJson(reverted)
            case _              => null
          })
          pstmt.setString(12, refinedInternalTransaction.time.orNull)
          pstmt.setString(13, refinedInternalTransaction.to.orNull)
          pstmt.setString(14, refinedInternalTransaction.`type`)
          refinedInternalTransaction.value match {
            case Some(value) =>
              pstmt.setString(15, value)
            case _ => pstmt.setNull(15, java.sql.Types.VARCHAR)
          }
          pstmt.setString(
            16,
            s"${refinedInternalTransaction.blockNumber}_${refinedInternalTransaction.index}_${refinedInternalTransaction.callId}")

          pstmt.addBatch()
          pstmt.clearParameters()

          if ((idx + 1) % 3000 == 0) {
            execute(pstmt)
          }
      }

      execute(pstmt)
      pstmt.close()
    }
  }

  def insert(internalTransactions: List[RefinedInternalTransactions]): Unit = {
    val data =
      mutable.Map.empty[String, ArrayBuffer[RefinedInternalTransactions]]

    internalTransactions
      .filter { x =>
        val from = x.from.getOrElse("")
        from.nonEmpty && from.length > 2
      }
      .foreach { itx =>
        data
          .getOrElseUpdate(getInternalTransactionDB(itx.blockNumber),
                           ArrayBuffer.empty[RefinedInternalTransactions])
          .append(itx)
      }

    Random.shuffle(data.keySet).foreach { dbName =>
      val internalTransactions = data(dbName)
      insert(internalTransactions, dbName)
    }
  }

  def insertIndex(internalTransactions: Seq[RefinedInternalTransactions],
                  dbName: String): Unit = {
    withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $InternalTXIndexTable (`internal_tx_id`,`account_address`,`block_number`," +
          "`transaction_index`,`call_id`) VALUES (?,?,?,?,?)"
      )

      internalTransactions
        .filter { x =>
          val from = x.from.getOrElse("")
          from.nonEmpty && from.length > 2
        }
        .zipWithIndex
        .foreach {
          case (refinedInternalTransaction, idx) =>
            pstmt.setString(
              1,
              s"${refinedInternalTransaction.blockNumber}_${refinedInternalTransaction.index}_${refinedInternalTransaction.callId}")
            pstmt.setString(2, refinedInternalTransaction.from.get)
            pstmt.setLong(3, refinedInternalTransaction.blockNumber)
            pstmt.setInt(4, refinedInternalTransaction.index)
            pstmt.setInt(5, refinedInternalTransaction.callId)

            pstmt.addBatch()
            pstmt.clearParameters()

            pstmt.setString(
              1,
              s"${refinedInternalTransaction.blockNumber}_${refinedInternalTransaction.index}_${refinedInternalTransaction.callId}")
            pstmt.setString(2, refinedInternalTransaction.to.getOrElse("-"))
            pstmt.setLong(3, refinedInternalTransaction.blockNumber)
            pstmt.setInt(4, refinedInternalTransaction.index)
            pstmt.setInt(5, refinedInternalTransaction.callId)

            pstmt.addBatch()
            pstmt.clearParameters()

            if ((idx + 1) % 3000 == 0) {
              execute(pstmt)
            }
        }

      execute(pstmt)
      pstmt.close()
    }
  }

  def insertIndex(
      internalTransactions: List[RefinedInternalTransactions]): Unit = {
    val data =
      mutable.Map.empty[String, ArrayBuffer[RefinedInternalTransactions]]

    internalTransactions
      .filter { x =>
        val from = x.from.getOrElse("")
        from.nonEmpty && from.length > 2
      }
      .foreach { itx =>
        data
          .getOrElseUpdate(getInternalTransactionIndexDB(itx.from.get),
                           ArrayBuffer.empty[RefinedInternalTransactions])
          .append(itx)

        val to = itx.to.getOrElse("-")
        if (itx.from.get != to && to.length > 2) {
          data
            .getOrElseUpdate(getInternalTransactionIndexDB(to),
                             ArrayBuffer.empty[RefinedInternalTransactions])
            .append(itx)
        }
      }

    Random.shuffle(data.keySet).foreach { dbName =>
      val internalTransactions = data(dbName)
      insertIndex(internalTransactions, dbName)
    }
  }
}
