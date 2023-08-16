package io.klaytn.repository

import io.klaytn.client.FinderRedis
import io.klaytn.dsl.db.withDB
import io.klaytn.model.finder.{AccountType, ContractType}

import scala.collection.mutable

object AccountRepository {
  val AccountDB: String = "finder0101"
  val AccountTable = "accounts"
}

// todo di cache service
abstract class AccountRepository extends AbstractRepository {
  import AccountRepository._

  def getConsensusNodes(): Seq[String] = {
    val result = mutable.Set.empty[String]
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT address FROM $AccountTable WHERE `contract_type`=126")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.add(rs.getString(1))
      }
      rs.close()
      pstmt.close()
    }
    result.toSeq
  }

  def getAccountType(address: String): AccountType.Value = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"SELECT `account_type` FROM $AccountTable WHERE `address` = ?")
      pstmt.setString(1, address)
      val rs = pstmt.executeQuery()
      val accountType = if (rs.next()) {
        AccountType(rs.getInt(1))
      } else {
        AccountType.Unknown
      }
      rs.close()
      pstmt.close()
      accountType
    }
  }

  def updateContractInfos(contractAddress: String,
                          contractType: ContractType.Value,
                          contractCreator: String,
                          createTxHash: String,
                          createdTimestamp: Long,
                          from: String): Unit = {
    withDB(AccountDB) { c =>
      val pstmt = c.prepareStatement(
        "UPDATE accounts SET account_type=1, contract_type=?, contract_creator_address=?, " +
          "contract_creator_transaction_hash=?, created_at=?, contract_deployer_address=? where address=?")
      pstmt.setInt(1, contractType.id)
      pstmt.setString(2, contractCreator)
      pstmt.setString(3, createTxHash)
      pstmt.setString(4, sdf.format(createdTimestamp))
      pstmt.setString(5, from)
      pstmt.setString(6, contractAddress)
      pstmt.execute()
      pstmt.close()
    }

    deleteCache(contractAddress)
  }

  def updateContractType(contractAddress: String,
                         contractType: ContractType.Value): Unit = {
    val accountType =
      if (contractType == ContractType.ConsensusNode) { // ConsensusNode must be stored as an EOA
        AccountType.EOA
      } else {
        AccountType.SCA
      }

    val updateCount = withDB(AccountDB) { conn =>
      val pstmt =
        conn.prepareStatement(
          s"UPDATE $AccountTable SET `account_type` = ?,`contract_type` = ? WHERE `address` = ?")

      pstmt.setInt(1, accountType.id)
      pstmt.setInt(2, contractType.id)
      pstmt.setString(3, contractAddress)
      val updateCount = pstmt.executeUpdate()
      pstmt.close()

      updateCount
    }

    if (updateCount == 0) {
      insert(contractAddress,
             accountType,
             0,
             contractType,
             None,
             None,
             System.currentTimeMillis(),
             None)
    }

    deleteCache(contractAddress)
  }

  def updateTotalTXCountAndType(transactionCount: Int,
                                contractType: ContractType.Value,
                                from: String,
                                txHash: String,
                                address: String): Unit = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"UPDATE $AccountTable SET `total_transaction_count` = `total_transaction_count` + ?, `account_type` = ?, " +
          "`contract_type` = ?, `contract_creator_address` = ?, " +
          "`contract_creator_transaction_hash` = ? WHERE `address` = ?")

      pstmt.setInt(1, transactionCount)
      pstmt.setInt(2, AccountType.SCA.id)
      pstmt.setInt(3, contractType.id)
      pstmt.setString(4, from)
      pstmt.setString(5, txHash)
      pstmt.setString(6, address)
      pstmt.execute()

      pstmt.close()
    }
    deleteCache(address)
  }

  def updateTotalTXCount(transactionCount: Int, address: String): Unit = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"UPDATE $AccountTable SET `total_transaction_count` = `total_transaction_count` + ? WHERE `address` = ?")

      pstmt.setInt(1, transactionCount)
      pstmt.setString(2, address)
      pstmt.execute()

      pstmt.close()
    }
    deleteCache(address)
  }

  def updateTotalTXCountBatch(data: Seq[(String, Long)]): Unit = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"UPDATE $AccountTable SET `total_transaction_count` = `total_transaction_count` + ? WHERE `address` = ?")

      data.foreach {
        case (address, transactionCount) =>
          pstmt.setLong(1, transactionCount)
          pstmt.setString(2, address)
          pstmt.addBatch()
          pstmt.clearParameters()
      }

      pstmt.executeBatch()
      pstmt.clearBatch()

      pstmt.close()
    }

    data.foreach {
      case (address, _) =>
        deleteCache(address)
    }
  }

  def insert(address: String,
             accountType: AccountType.Value,
             transactionCount: Int,
             contractType: ContractType.Value,
             contractCreatorAddress: Option[String],
             contractCreatorTransactionHash: Option[String],
             createdTimestamp: Long,
             from: Option[String]): Unit = {
    withDB(AccountDB) { conn =>
      val pstmt = conn.prepareStatement(
        s"INSERT IGNORE INTO $AccountTable (`address`,`account_type`,`total_transaction_count`,`contract_type`," +
          "`contract_creator_address`,`contract_creator_transaction_hash`,`created_at`,`contract_deployer_address`) VALUES (?,?,?,?,?,?,?,?)")

      pstmt.setString(1, address)
      pstmt.setLong(3, transactionCount)
      pstmt.setInt(4, contractType.id)
      accountType match {
        case AccountType.SCA =>
          pstmt.setInt(2, AccountType.SCA.id)
          pstmt.setString(5, contractCreatorAddress.orNull)
          pstmt.setString(6, contractCreatorTransactionHash.orNull)
        case AccountType.EOA =>
          pstmt.setInt(2, AccountType.EOA.id)
          pstmt.setNull(5, java.sql.Types.VARCHAR)
          pstmt.setNull(6, java.sql.Types.VARCHAR)
      }
      pstmt.setString(7, sdf.format(createdTimestamp))
      pstmt.setString(8, from.orNull)
      pstmt.execute()
      pstmt.close()
    }

    deleteCache(address)
  }

  private def normalizeKNS(name: String): String = {
    if (name != null && name.length > 255) name.substring(0, 255)
    else name
  }

  def updateKNS(address: String, name: String): Unit = {
    withDB(AccountDB) { c =>
      val pstmt = c.prepareStatement(
        s"UPDATE $AccountTable SET `kns_domain`=? WHERE `address`=?")
      pstmt.setString(1, normalizeKNS(name))
      pstmt.setString(2, address)
      pstmt.execute()
      pstmt.close()
    }
    deleteCache(address)
  }

  def findAddressByKNS(name: String): Option[String] = {
    withDB(AccountDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `address` FROM $AccountTable WHERE `kns_domain`=?")
      pstmt.setString(1, normalizeKNS(name))
      pstmt.execute()
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        rs.getString(1)
      } else {
        null
      }
      rs.close()
      pstmt.close()

      Option(result)
    }
  }

  // todo move to external
  private def deleteCache(address: String): Unit = {
    FinderRedis.del(s"cache/account-by-address::$address")
  }

}
