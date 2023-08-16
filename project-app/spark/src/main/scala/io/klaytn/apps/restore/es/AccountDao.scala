package io.klaytn.apps.restore.es

import io.klaytn.dsl.db.withDB
import io.klaytn.repository.AccountRepository

import scala.collection.mutable

private[es] class AccountDao extends Serializable {
  private val AccountDB = AccountRepository.AccountDB
  private val AccountTable = "accounts"

  def getMinId(): Long = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $AccountTable ORDER BY id ASC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val minId = rs.getLong(1)
      rs.close()
      pstmt.close()
      minId
    }
  }

  def getMaxId(): Long = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT id FROM $AccountTable ORDER BY id DESC LIMIT 1")
      val rs = pstmt.executeQuery()
      rs.next()
      val maxId = rs.getLong(1)
      rs.close()
      pstmt.close()
      maxId
    }
  }

  def findByIdRange(fromInclusiveId: Long,
                    toExclusiveId: Long): Seq[AccountDTO] = {
    if (fromInclusiveId > toExclusiveId) {
      throw new IllegalArgumentException(
        s"invalid input [from: $fromInclusiveId / to: $toExclusiveId]")
    }
    if (toExclusiveId - fromInclusiveId > 500) {
      throw new IllegalArgumentException(s"max diff is 500")
    }
    withDB(AccountDB) { conn =>
      val result = new mutable.ListBuffer[AccountDTO]()
      val pstmt = conn.prepareStatement(s"SELECT " +
        s"`address`, `account_type`, `balance`, `contract_type`, `contract_creator_address`, " +
        s"`contract_creator_transaction_hash`, `kns_domain`, `address_label`, `tags`, `updated_at` FROM $AccountTable WHERE id >= $fromInclusiveId AND id < $toExclusiveId")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        val address = rs.getString(1)
        val accountType = rs.getInt(2)
        val balance = rs.getBigDecimal(3)
        val contractType = rs.getInt(4)
        val contractCreatorAddress = rs.getString(5)
        val contractCreatorTransactionHash = rs.getString(6)
        val knsDomain = rs.getString(7)
        val addressLabel = rs.getString(8)
        val tags = rs.getString(9)
        val updatedAt = rs.getTimestamp(10).getTime

        result.append(
          AccountDTO(
            address = address,
            accountType = accountType,
            balance = Option(balance).map(BigDecimal(_)),
            contract_type = contractType,
            contract_creator_address = Option(contractCreatorAddress),
            contract_creator_tx_hash = Option(contractCreatorTransactionHash),
            kns_domain = Option(knsDomain),
            address_label = Option(addressLabel),
            tags = Option(tags),
            updated_at = updatedAt
          )
        )
      }
      rs.close()
      pstmt.close()
      result
    }
  }

}
