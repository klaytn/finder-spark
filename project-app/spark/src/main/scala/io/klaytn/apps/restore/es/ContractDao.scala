package io.klaytn.apps.restore.es

import io.klaytn.dsl.db.withDB
import io.klaytn.repository.ContractRepository
import org.apache.commons.lang3.BooleanUtils

import scala.collection.mutable

class ContractDao() extends Serializable {
  private val ContractDB = ContractRepository.ContractDB
  private val ContractTable = "contracts"

  def getMinId(): Long = {
    withDB(ContractDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $ContractTable ORDER BY id ASC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val minId = rs.getLong(1)
      rs.close()
      pstmt.close()
      minId
    }
  }

  def getMaxId(): Long = {
    withDB(ContractDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $ContractTable ORDER BY id DESC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val maxId = rs.getLong(1)
      rs.close()
      pstmt.close()
      maxId
    }
  }

  def findByIdRange(fromInclusiveId: Long,
                    toExclusiveId: Long): Seq[ContractDTO] = {
    if (fromInclusiveId > toExclusiveId) {
      throw new IllegalArgumentException(
        s"invalid input [from: $fromInclusiveId / to: $toExclusiveId]")
    }
    if (toExclusiveId - fromInclusiveId > 500) {
      throw new IllegalArgumentException(s"max diff is 500")
    }
    withDB(ContractDB) { conn =>
      val result = new mutable.ListBuffer[ContractDTO]()
      val pstmt = conn.prepareStatement(
        s"SELECT " +
          s"contract_address, contract_type, name, symbol, verified, " +
          s"created_at, updated_at, total_supply_order, total_transfer " +
          s"FROM $ContractTable WHERE id >= $fromInclusiveId AND id < $toExclusiveId")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        val contract_address = rs.getString(1)
        val contract_type = rs.getInt(2)
        val name = Option(rs.getString(3)).getOrElse("")
        val symbol = Option(rs.getString(4)).getOrElse("")
        val verified =
          Option(rs.getString(5)).filter(_.nonEmpty).map(BooleanUtils.toBoolean)
        val created_at = Option(rs.getTimestamp(6)).map(_.getTime).getOrElse(0L)
        val updated_at = Option(rs.getTimestamp(7)).map(_.getTime).getOrElse(0L)
        val total_supply_order = rs.getString(8)
        val total_transfer = rs.getLong(9)

        result.append(
          ContractDTO(
            contract_address = contract_address,
            contract_type = contract_type,
            name = name,
            symbol = symbol,
            verified = verified,
            created_at = created_at,
            updated_at = updated_at,
            total_supply_order = total_supply_order,
            total_transfer = total_transfer
          )
        )
      }
      rs.close()
      pstmt.close()
      result
    }
  }

}
