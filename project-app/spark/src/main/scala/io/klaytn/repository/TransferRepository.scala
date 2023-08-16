package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.finder._
import io.klaytn.model.{Chain, RefinedEventLog}
import io.klaytn.utils.Utils
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.spark.UserConfig

import java.sql.Types
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class AccountTransferContracts(accountAddress: String,
                                    contractAddress: String,
                                    timestamp: Int,
                                    transferType: TransferType.Value)

object TransferRepository {
  val AccountTransferContractsDB = "finder0101"
  val TokenTransferDB = "finder03"
  val NFTTransferDB = "finder03"
  val AccountNFTApprovesDB = "finder0101"
  val AccountTokenApprovesDB = "finder0101"

  val AccountTransferContractsTable = "account_transfer_contracts"
  val TokenTransfersTable: String = UserConfig.chainPhase.chain match {
    case Chain.baobab | Chain.cypress =>
      "token_transfers"
    case _ =>
      "token_transfers"
  }
  val NFTTransfersTable: String = UserConfig.chainPhase.chain match {
    case Chain.baobab | Chain.cypress =>
      "nft_transfers"
    case _ =>
      "nft_transfers"
  }

  val TokenBurnsTable = "token_burns"
  val NFTBurnsTable = "nft_burns"

  val AccountNFTApprovesTable = "account_nft_approves"
  val AccountTokenApprovesTable = "account_token_approves"
}

abstract class TransferRepository extends AbstractRepository {
  import TransferRepository._

  def insertAccountTransferContracts(
      accountTransferContracts: Seq[AccountTransferContracts]): Unit = {
    //                       [(account_address, contract_address), (AccountTransferContracts, update)]
    val m =
      mutable.Map.empty[(String, String), (AccountTransferContracts, Boolean)]
    accountTransferContracts.foreach(a =>
      m((a.accountAddress, a.contractAddress)) = (a, false))
    val where = accountTransferContracts
      .map(a =>
        s"`account_address`='${a.accountAddress}' AND `contract_address`='${a.contractAddress}'")
      .mkString(" or ")

    withDB(AccountTransferContractsDB) { c =>
      val pstmtSel = c.prepareStatement(
        s"SELECT `account_address`, `contract_address` FROM $AccountTransferContractsTable WHERE $where")
      val rs = pstmtSel.executeQuery()
      while (rs.next()) {
        val data = m((rs.getString(1), rs.getString(2)))
        m((rs.getString(1), rs.getString(2))) = (data._1, true)
      }

      rs.close()
      pstmtSel.close()
    }

    val insert = m.filter(_._2._2 == false).map(_._2._1)
    val update = m.filter(_._2._2).map(_._2._1)

    // insert
    withDB(AccountTransferContractsDB) { c =>
      val pstmtIns = c.prepareStatement(
        s"INSERT IGNORE INTO $AccountTransferContractsTable (`account_address`, `contract_address`,`created_at`,`updated_at`,`transfer_type`) VALUES (?,?,?,?,?)")
      insert.foreach { accountTransferContract =>
        val dt = sdf.format(accountTransferContract.timestamp * 1000L)
        pstmtIns.setString(1, accountTransferContract.accountAddress)
        pstmtIns.setString(2, accountTransferContract.contractAddress)
        pstmtIns.setString(3, dt)
        pstmtIns.setString(4, dt)
        pstmtIns.setInt(5, accountTransferContract.transferType.id)
        pstmtIns.addBatch()
        pstmtIns.clearParameters()
      }
      pstmtIns.executeBatch()
      pstmtIns.clearBatch()
      pstmtIns.close()
    }

    // update
    withDB(AccountTransferContractsDB) { c =>
      val pstmtUp = c.prepareStatement(
        s"UPDATE $AccountTransferContractsTable SET `updated_at`=?, `transfer_type`=? WHERE `account_address`=? AND `contract_address`=?")
      update.foreach { accountTransferContract =>
        val dt = sdf.format(accountTransferContract.timestamp * 1000L)
        pstmtUp.setString(1, dt)
        pstmtUp.setInt(2, accountTransferContract.transferType.id)
        pstmtUp.setString(3, accountTransferContract.accountAddress)
        pstmtUp.setString(4, accountTransferContract.contractAddress)
        pstmtUp.addBatch()
        pstmtUp.clearParameters()
      }
      pstmtUp.executeBatch()
      pstmtUp.clearBatch()
      pstmtUp.close()
    }
  }

  private def insertTokenTransfer(eventLogs0: Seq[RefinedEventLog],
                                  insertResult: ArrayBuffer[Int],
                                  tableName: String): Unit = {
    eventLogs0.grouped(3000).foreach { eventLogs =>
      withDB(TokenTransferDB) { c =>
        val pstmt = c.prepareStatement(
          s"INSERT INTO $tableName (`contract_address`,`from`,`to`,`amount`,`timestamp`,`block_number`," +
            "`transaction_hash`,`display_order`) VALUES (?,?,?,?,?,?,?,?)"
        )

        eventLogs.foreach { eventLog =>
          val (address, from, to, _, _) = eventLog.extractTransferInfo()

          pstmt.setString(1, address)
          pstmt.setString(2, from)
          pstmt.setString(3, to)
          pstmt.setString(4, eventLog.data)
          pstmt.setInt(5, eventLog.timestamp)
          pstmt.setLong(6, eventLog.blockNumber)
          pstmt.setString(7, eventLog.transactionHash)
          pstmt.setString(8,
                          Utils.getDisplayOrder(eventLog.blockNumber,
                                                eventLog.transactionIndex,
                                                eventLog.logIndex))

          pstmt.addBatch()
          pstmt.clearParameters()
        }

        execute(pstmt).foreach(x => insertResult.append(x))
        pstmt.close()
      }
    }
  }

  def insertTokenTransferEvent(eventLogs: Seq[RefinedEventLog],
                               insertResult: ArrayBuffer[Int]): Unit = {
    insertTokenTransfer(eventLogs, insertResult, TokenTransfersTable)
  }

  def insertTokenBurnEvent(eventLogs: Seq[RefinedEventLog]): Unit = {
    insertTokenTransfer(eventLogs, ArrayBuffer.empty[Int], TokenBurnsTable)
  }

  private def insertNFTTransfer(transfers0: Seq[NFTTransfer],
                                tableName: String): Unit = {
    transfers0.grouped(3000).foreach { transfers =>
      withDB(NFTTransferDB) { c =>
        val pstmt = c.prepareStatement(
          s"INSERT INTO $tableName (`contract_type`,`contract_address`,`from`,`to`,`token_count`," +
            "`token_id`, `timestamp`,`block_number`,`transaction_hash`,`display_order`) VALUES (?,?,?,?,?,?,?,?,?,?)"
        )

        transfers.foreach { transfer =>
          pstmt.setInt(1, transfer.contractType.id)
          pstmt.setString(2, transfer.contractAddress)
          pstmt.setString(3, transfer.from)
          pstmt.setString(4, transfer.to)
          pstmt.setString(5, transfer.tokenCount)
          pstmt.setString(6, transfer.tokenId.toString())
          pstmt.setInt(7, transfer.timestamp)
          pstmt.setLong(8, transfer.blockNumber)
          pstmt.setString(9, transfer.transactionHash)
          pstmt.setString(10, transfer.displayOrder)

          pstmt.addBatch()
          pstmt.clearParameters()
        }

        execute(pstmt)
        pstmt.close()
      }
    }
  }

  def insertNFTTransferEvent(transfers: Seq[NFTTransfer]): Unit = {
    insertNFTTransfer(transfers, NFTTransfersTable)
  }

  def insertNFTBurnEvent(transfers: Seq[NFTTransfer]): Unit = {
    insertNFTTransfer(transfers, NFTBurnsTable)
  }

  def getNFTTransferMinBlockNumber(): Long = {
    withDB(NFTTransferDB) { c =>
      val pstmt =
        c.prepareStatement(s"SELECT MIN(block_number) FROM $NFTTransfersTable")

      val rs = pstmt.executeQuery()
      val minBlockNumber = if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }

      rs.close()
      pstmt.close()

      minBlockNumber
    }
  }

  private def getNFTTransfers(tableId: Long,
                              cnt: Int,
                              tableName: String): Seq[NFTTransfer] = {
    withDB(NFTTransferDB) { c =>
      val nftTransfers = ArrayBuffer.empty[NFTTransfer]
      val pstmt =
        c.prepareStatement(s"SELECT * FROM $tableName WHERE id > ? AND id <= ?")
      pstmt.setLong(1, tableId)
      pstmt.setLong(2, tableId + cnt)

      val rs = pstmt.executeQuery()

      while (rs.next()) {
        nftTransfers.append(
          NFTTransfer(
            Some(rs.getLong("id")),
            ContractType.from(rs.getInt("contract_type")),
            rs.getString("contract_address"),
            rs.getString("from"),
            rs.getString("to"),
            rs.getString("token_count"),
            BigInt(rs.getString("token_id")),
            rs.getInt("timestamp"),
            rs.getLong("block_number"),
            rs.getString("transaction_hash"),
            rs.getString("display_order")
          ))
      }

      rs.close()
      pstmt.close()
      nftTransfers
    }
  }

  def getNFTTransfers(tableId: Long, cnt: Int): Seq[NFTTransfer] = {
    getNFTTransfers(tableId, cnt, NFTTransfersTable)
  }

  def getNFTBurnTransfers(tableId: Long, cnt: Int): Seq[NFTTransfer] = {
    getNFTTransfers(tableId, cnt, NFTBurnsTable)
  }

  def getTokenTransferMinBlockNumber(): Long = {
    withDB(TokenTransferDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT MIN(block_number) FROM $TokenTransfersTable")

      val rs = pstmt.executeQuery()
      val minBlockNumber = if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }

      rs.close()
      pstmt.close()

      minBlockNumber
    }
  }

  private def getTokenTransfers(tableId: Long,
                                cnt: Int,
                                tableName: String): Seq[TokenTransfer] = {
    val tokenTransfers = ArrayBuffer.empty[TokenTransfer]

    withDB(TokenTransferDB) { c =>
      val pstmt =
        c.prepareStatement(s"SELECT * FROM $tableName WHERE id > ? AND id <= ?")
      pstmt.setLong(1, tableId)
      pstmt.setLong(2, tableId + cnt)

      val rs = pstmt.executeQuery()

      while (rs.next()) {
        tokenTransfers.append(
          TokenTransfer(
            rs.getLong("id"),
            rs.getString("contract_address"),
            rs.getString("from"),
            rs.getString("to"),
            rs.getString("amount"),
            rs.getInt("timestamp"),
            rs.getLong("block_number"),
            rs.getString("transaction_hash"),
            rs.getString("display_order")
          ))
      }

      rs.close()
      pstmt.close()
    }

    tokenTransfers
  }

  def getTokenTransfers(tableId: Long, cnt: Int): Seq[TokenTransfer] = {
    getTokenTransfers(tableId, cnt, TokenTransfersTable)
  }

  def getTokenBurnTransfers(tableId: Long, cnt: Int): Seq[TokenTransfer] = {
    getTokenTransfers(tableId, cnt, TokenBurnsTable)
  }

  def findTokenTransfersByBlockNumbers(
      blockNumbers0: Seq[Long]): Seq[TokenTransfer] = {
    val GroupedCount = 10
    val tokenTransfers = ArrayBuffer.empty[TokenTransfer]

    blockNumbers0.grouped(GroupedCount).foreach { blockNumbers =>
      withDB(TokenTransferDB) { c =>
        val pstmt =
          c.prepareStatement(
            s"SELECT * FROM $TokenTransfersTable WHERE block_number in (?,?,?,?,?,?,?,?,?,?)")

        blockNumbers.zipWithIndex.foreach {
          case (blockNumber, index) =>
            pstmt.setLong(index + 1, blockNumber)
        }
        blockNumbers.length + 1 to GroupedCount foreach (idx =>
          pstmt.setNull(idx, Types.INTEGER))

        val rs = pstmt.executeQuery()
        while (rs.next()) {
          tokenTransfers.append(
            TokenTransfer(
              rs.getLong("id"),
              rs.getString("contract_address"),
              rs.getString("from"),
              rs.getString("to"),
              rs.getString("amount"),
              rs.getInt("timestamp"),
              rs.getLong("block_number"),
              rs.getString("transaction_hash"),
              rs.getString("display_order")
            ))
        }

        rs.close()
        pstmt.close()
      }
    }

    tokenTransfers
  }

  def findNFTTransfersByBlockNumbers(
      blockNumbers0: Seq[Long]): Seq[NFTTransfer] = {
    val GroupedCount = 10
    val nftTransfers = ArrayBuffer.empty[NFTTransfer]

    blockNumbers0.grouped(GroupedCount).foreach { blockNumbers =>
      withDB(TokenTransferDB) { c =>
        val pstmt =
          c.prepareStatement(
            s"SELECT * FROM $NFTTransfersTable WHERE block_number in (?,?,?,?,?,?,?,?,?,?)")

        blockNumbers.zipWithIndex.foreach {
          case (blockNumber, index) =>
            pstmt.setLong(index + 1, blockNumber)
        }
        blockNumbers.length + 1 to GroupedCount foreach (idx =>
          pstmt.setNull(idx, Types.INTEGER))

        val rs = pstmt.executeQuery()
        while (rs.next()) {
          nftTransfers.append(
            NFTTransfer(
              Some(rs.getLong("id")),
              ContractType.from(rs.getInt("contract_type")),
              rs.getString("contract_address"),
              rs.getString("from"),
              rs.getString("to"),
              rs.getString("token_count"),
              BigInt(rs.getString("token_id")),
              rs.getInt("timestamp"),
              rs.getLong("block_number"),
              rs.getString("transaction_hash"),
              rs.getString("display_order")
            ))
        }

        rs.close()
        pstmt.close()
      }
    }

    nftTransfers
  }

  private def findTokenTransferDisplayOrdersByBlockNumbers(
      blockNumbers0: Seq[Long],
      tableName: String): Set[String] = {
    val GroupedCount = 10
    val displayOrders = mutable.Set.empty[String]

    blockNumbers0.grouped(GroupedCount).foreach { blockNumbers =>
      withDB(TokenTransferDB) { c =>
        val pstmt =
          c.prepareStatement(
            s"SELECT DISTINCT `display_order` FROM $tableName WHERE block_number in (?,?,?,?,?,?,?,?,?,?)")

        blockNumbers.zipWithIndex.foreach {
          case (blockNumber, index) =>
            pstmt.setLong(index + 1, blockNumber)
        }
        blockNumbers.length + 1 to GroupedCount foreach (idx =>
          pstmt.setNull(idx, Types.INTEGER))

        val rs = pstmt.executeQuery()
        while (rs.next()) {
          displayOrders.add(rs.getString(1))
        }

        rs.close()
        pstmt.close()
      }
    }

    displayOrders.toSet
  }

  def findTokenTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    findTokenTransferDisplayOrdersByBlockNumbers(blockNumbers,
                                                 TokenTransfersTable)
  }

  private def findNFTTransferDisplayOrdersByBlockNumbers(
      blockNumbers0: Seq[Long],
      tableName: String): Set[String] = {
    val GroupedCount = 10
    val displayOrders = mutable.Set.empty[String]

    blockNumbers0.grouped(GroupedCount).foreach { blockNumbers =>
      withDB(TokenTransferDB) { c =>
        val pstmt =
          c.prepareStatement(
            s"SELECT DISTINCT `display_order` FROM $tableName WHERE block_number in (?,?,?,?,?,?,?,?,?,?)")

        blockNumbers.zipWithIndex.foreach {
          case (blockNumber, index) =>
            pstmt.setLong(index + 1, blockNumber)
        }
        blockNumbers.length + 1 to GroupedCount foreach (idx =>
          pstmt.setNull(idx, Types.INTEGER))

        val rs = pstmt.executeQuery()
        while (rs.next()) {
          displayOrders.add(rs.getString(1))
        }

        rs.close()
        pstmt.close()
      }
    }

    displayOrders.toSet
  }

  def findTokenBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    findTokenTransferDisplayOrdersByBlockNumbers(blockNumbers, TokenBurnsTable)
  }

  def findNFTTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    findNFTTransferDisplayOrdersByBlockNumbers(blockNumbers, NFTTransfersTable)
  }

  def findNFTBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    findNFTTransferDisplayOrdersByBlockNumbers(blockNumbers, NFTBurnsTable)
  }

  def insertTokenApprove(tokenApproves: Seq[TokenApprove]): Unit = {
    withDB(AccountTokenApprovesDB) { c =>
      val pstmt = c.prepareStatement(
        s"""INSERT INTO $AccountTokenApprovesTable (`block_number`,`transaction_hash`,`account_address`,`spender_address`,
          `contract_type`,`contract_address`,`approved_amount`,`timestamp`) values (?,?,?,?,?,?,?,?)
            ON DUPLICATE KEY UPDATE 
            `block_number` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`block_number`), `block_number`),
            `transaction_hash` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`transaction_hash`), `transaction_hash`),
            `account_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`account_address`), `account_address`),
            `spender_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`spender_address`), `spender_address`),
            `contract_type` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`contract_type`), `contract_type`),
            `contract_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`contract_address`), `contract_address`),
            `approved_amount` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`approved_amount`), `approved_amount`),
            `timestamp` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`timestamp`), `timestamp`)
          """)

      tokenApproves.foreach { tokenApprove =>
        pstmt.setLong(1, tokenApprove.blockNumber)
        pstmt.setString(2, tokenApprove.transactionHash)
        pstmt.setString(3, tokenApprove.accountAddress)
        pstmt.setString(4, tokenApprove.spenderAddress)
        pstmt.setInt(5, tokenApprove.contractType.id)
        pstmt.setString(6, tokenApprove.contractAddress)
        pstmt.setString(7, tokenApprove.approvedAmount.to64BitsHex())
        pstmt.setInt(8, tokenApprove.timestamp)
        pstmt.addBatch()
        pstmt.clearParameters()
      }

      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }
  }

  def findTokenApproveByAccountAndContractAndSpender(
      accountAddress: String,
      contractAddress: String,
      spenderAddress: String): Option[TokenApprove] = {
    withDB(AccountTokenApprovesDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT * FROM $AccountTokenApprovesTable WHERE account_address=? AND contract_address=? AND spender_address=?")

      pstmt.setString(1, accountAddress)
      pstmt.setString(2, contractAddress)
      pstmt.setString(3, spenderAddress)

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        TokenApprove(
          Option(rs.getLong("id")),
          rs.getLong("block_number"),
          rs.getString("transaction_hash"),
          rs.getString("account_address"),
          rs.getString("spender_address"),
          ContractType.from(rs.getInt("contract_type")),
          rs.getString("contract_address"),
          BigInt(rs.getString("approved_amount")),
          rs.getInt("timestamp")
        )
      } else {
        null
      }

      rs.close()
      pstmt.close()

      Option(result)
    }
  }

  def deleteTokenApprove(tokenApproves: Seq[TokenApprove]): Unit = {
    withDB(AccountTokenApprovesDB) { c =>
      val pstmt = c.prepareStatement(
        s"DELETE FROM $AccountTokenApprovesTable WHERE account_address=? AND contract_address=? AND spender_address=?")
      tokenApproves.foreach { tokenApprove =>
        pstmt.setString(1, tokenApprove.accountAddress)
        pstmt.setString(2, tokenApprove.contractAddress)
        pstmt.setString(3, tokenApprove.spenderAddress)
        pstmt.addBatch()
        pstmt.clearParameters()
      }
      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }
  }

  def insertNFTApprove(nftApproves: Seq[NFTApprove]): Unit = {
    withDB(AccountNFTApprovesDB) { c =>
      val pstmt = c.prepareStatement(
        s"""INSERT INTO $AccountNFTApprovesTable (`block_number`,`transaction_hash`,`account_address`,`spender_address`,
          `contract_type`,`contract_address`,`approved_token_id`,`approved_all`,`timestamp`) values (?,?,?,?,?,?,?,?,?)
          ON DUPLICATE KEY UPDATE 
            `block_number` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`block_number`), `block_number`),
            `transaction_hash` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`transaction_hash`), `transaction_hash`),
            `account_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`account_address`), `account_address`),
            `spender_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`spender_address`), `spender_address`),
            `contract_type` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`contract_type`), `contract_type`),
            `contract_address` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`contract_address`), `contract_address`),
            `approved_token_id` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`approved_token_id`), `approved_token_id`),
            `approved_all` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`approved_all`), `approved_all`),
            `timestamp` = IF(`timestamp` < VALUES(`timestamp`), VALUES(`timestamp`), `timestamp`)
          """)

      nftApproves.foreach { nftApprove =>
        pstmt.setLong(1, nftApprove.blockNumber)
        pstmt.setString(2, nftApprove.transactionHash)
        pstmt.setString(3, nftApprove.accountAddress)
        pstmt.setString(4, nftApprove.spenderAddress)
        pstmt.setInt(5, nftApprove.contractType.id)
        pstmt.setString(6, nftApprove.contractAddress)

        if (nftApprove.approvedTokenId.isDefined)
          pstmt.setString(7, nftApprove.approvedTokenId.get)
        else pstmt.setString(7, null)

        pstmt.setBoolean(8, nftApprove.approvedAll)
        pstmt.setInt(9, nftApprove.timestamp)
        pstmt.addBatch()
        pstmt.clearParameters()
      }

      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }
  }

  def findNFTApproveByAccountAndContract(
      accountAddress: String,
      contractAddress: String): Seq[NFTApprove] = {
    val result = mutable.ArrayBuffer.empty[NFTApprove]
    withDB(AccountNFTApprovesDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT * FROM $AccountNFTApprovesTable WHERE account_address=? AND contract_address=?")

      pstmt.setString(1, accountAddress)
      pstmt.setString(2, contractAddress)

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        NFTApprove(
          Option(rs.getLong("id")),
          rs.getLong("block_number"),
          rs.getString("transaction_hash"),
          rs.getString("account_address"),
          rs.getString("spender_address"),
          ContractType.from(rs.getInt("contract_type")),
          rs.getString("contract_address"),
          Option(rs.getString("approved_token_id")),
          rs.getBoolean("approved_all"),
          rs.getInt("timestamp")
        )
      }

      rs.close()
      pstmt.close()

      Option(result)
    }
    result
  }

  def deleteNFTApprove(nftApproves: Seq[NFTApprove]): Unit = {
    withDB(AccountNFTApprovesDB) { c =>
      val pstmt = c.prepareStatement(s"DELETE FROM $AccountNFTApprovesTable " +
        "WHERE account_address=? AND contract_address=? AND approved_all=0 AND approved_token_id=?")
      nftApproves.filter(_.approvedTokenId.isDefined).foreach { nftApprove =>
        pstmt.setString(1, nftApprove.accountAddress)
        pstmt.setString(2, nftApprove.contractAddress)
        pstmt.setString(3, nftApprove.approvedTokenId.get)
        pstmt.addBatch()
        pstmt.clearParameters()
      }
      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }

    withDB(AccountNFTApprovesDB) { c =>
      val pstmt = c.prepareStatement(s"DELETE FROM $AccountNFTApprovesTable " +
        "WHERE account_address=? AND contract_address=? AND approved_all=1 AND spender_address=?")
      nftApproves.filter(_.approvedTokenId.isEmpty).foreach { nftApprove =>
        pstmt.setString(1, nftApprove.accountAddress)
        pstmt.setString(2, nftApprove.contractAddress)
        pstmt.setString(3, nftApprove.spenderAddress)
        pstmt.addBatch()
        pstmt.clearParameters()
      }
      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }
  }
}
