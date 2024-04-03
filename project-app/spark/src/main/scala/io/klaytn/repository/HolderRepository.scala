package io.klaytn.repository

import io.klaytn._
import io.klaytn.client.FinderRedis
import io.klaytn.contract.lib.{
  KIP17MetadataReader,
  KIP37MetadataReader,
  KIP7MetadataReader
}
import io.klaytn.dsl.db.withDB
import io.klaytn.persistent.ContractPersistentAPI
import io.klaytn.service.CaverFactory
import io.klaytn.utils.klaytn.NumberConverter._

import java.sql.PreparedStatement
import scala.collection.mutable

case class TokenHolders(contractAddress: String,
                        holderAddress: String,
                        amount: BigInt,
                        timestamp: Int,
                        blockNumber: Long)
case class NFTHolders(contractAddress: String,
                      holderAddress: String,
                      tokenId: BigInt,
                      tokenCount: BigInt,
                      uri: String,
                      timestamp: Int,
                      blockNumber: Long)
case class NFTInventories(isSend: Boolean,
                          contractAddress: String,
                          holderAddress: String,
                          tokenId: BigInt,
                          uri: String,
                          timestamp: Int)
object HolderRepository {
  val TokenHolderDB = "finder03"
  val NFTHolderDB = "finder03"
  val TokenHoldersTable = "token_holders"
  val NFTHoldersTable = "nft_holders"
  val NFTInventoriesTable = "nft_inventories"
  val NFTPatternedUriTable = "nft_patterned_uri"
}
abstract class HolderRepository(
    contractPersistentAPI: LazyEval[ContractPersistentAPI])
    extends AbstractRepository {
  import HolderRepository._

  def insertNFTPatternedUri(contractAddress: String, tokenUri: String): Unit = {
    withDB(NFTHolderDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $NFTPatternedUriTable (`contract_address`,`token_uri`) VALUES (?,?)")

      pstmt.setString(1, contractAddress)
      pstmt.setString(2, tokenUri)

      try {
        pstmt.execute()
      } catch {
        case _: Throwable =>
      }

      pstmt.close()
    }
  }

  def getNFTPatternedUri(contractAddress: String): Option[String] = {
    withDB(NFTHolderDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `token_uri` FROM $NFTPatternedUriTable WHERE `contract_address`=?")

      pstmt.setString(1, contractAddress)
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        Some(rs.getString(1))
      } else {
        None
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def getNFTUri(contractAddress: String, tokenId: BigInt): Option[String] = {
    withDB(NFTHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT `token_uri` FROM $NFTInventoriesTable WHERE `contract_address`=? AND `token_id`=?")

      pstmt.setString(1, contractAddress)
      pstmt.setString(2, tokenId.toString())
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        Some(rs.getString(1))
      } else {
        None
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def updateNFTUri(contractAddress: String,
                   tokenId: BigInt,
                   uri: String): Unit = {
    withDB(NFTHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $NFTInventoriesTable SET `token_uri`=?  WHERE `contract_address`=? AND `token_id`=?")

      pstmt.setString(1, uri)
      pstmt.setString(2, contractAddress)
      pstmt.setString(3, tokenId.toString)

      pstmt.executeUpdate()

      pstmt.close()
    }
  }

  //  private def fillDeleteTokenHolders(pstmt: PreparedStatement, contractAddress: String, holderAddress: String): Unit = {
//    pstmt.setString(1, contractAddress)
//    pstmt.setString(2, holderAddress)
//
//    pstmt.addBatch()
//    pstmt.clearParameters()
//  }
//
//  private def fillUpdateTokenHolders(pstmt: PreparedStatement,
//                                     contractAddress: String,
//                                     holderAddress: String,
//                                     amount: BigInt,
//                                     timestamp: Int): Unit = {
//    pstmt.setString(1, amount.to64BitsHex())
//    pstmt.setInt(2, timestamp)
//    pstmt.setString(3, contractAddress)
//    pstmt.setString(4, holderAddress)
//
//    pstmt.addBatch()
//    pstmt.clearParameters()
//  }
//
//  private def fillInsertTokenHolders(pstmt: PreparedStatement,
//                                     contractAddress: String,
//                                     holderAddress: String,
//                                     amount: BigInt,
//                                     timestamp: Int): Unit = {
//    pstmt.setString(1, contractAddress)
//    pstmt.setString(2, holderAddress)
//    pstmt.setString(3, amount.to64BitsHex())
//    pstmt.setInt(4, timestamp)
//
//    pstmt.addBatch()
//    pstmt.clearParameters()
//  }

  private def fillInsNFTHolders(pstmt: PreparedStatement,
                                contractAddress: String,
                                holderAddress: String,
                                tokenCount: BigInt,
                                timestamp: Int): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)
    pstmt.setString(3, tokenCount.to64BitsHex())
    pstmt.setInt(4, timestamp)

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillInsNFTInventories(pstmt: PreparedStatement,
                                    contractAddress: String,
                                    holderAddress: String,
                                    tokenId: BigInt,
                                    tokenUri: String,
                                    timestamp: Int): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)
    pstmt.setString(3, tokenId.toString())
    pstmt.setString(4, tokenUri)
    pstmt.setInt(5, timestamp)

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillDelNFTHolders(pstmt: PreparedStatement,
                                contractAddress: String,
                                holderAddress: String): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillUpdateNFTHolders(pstmt: PreparedStatement,
                                   amount: BigInt,
                                   lastTransactionTime: Int,
                                   contractAddress: String,
                                   holderAddress: String): Unit = {
    pstmt.setString(1, amount.to64BitsHex())
    pstmt.setInt(2, lastTransactionTime)
    pstmt.setString(3, contractAddress)
    pstmt.setString(4, holderAddress)

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillUpdateTokenUriBulk(pstmt: PreparedStatement,
                                     tokenUri: String,
                                     contractAddress: String,
                                     tokenId: String): Unit = {
    pstmt.setString(1, tokenUri)
    pstmt.setString(2, contractAddress)
    pstmt.setString(3, tokenId)
    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillDelNFTInventories(pstmt: PreparedStatement,
                                    contractAddress: String,
                                    holderAddress: String,
                                    tokenId: BigInt): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)
    pstmt.setString(3, tokenId.toString())

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillInsKIP37Holders(pstmt: PreparedStatement,
                                  contractAddress: String,
                                  holderAddress: String,
                                  tokenId: BigInt,
                                  tokenUri: String,
                                  tokenCount: BigInt,
                                  timestamp: Int): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)
    pstmt.setString(3, tokenId.toString())
    pstmt.setString(4, tokenUri)
    pstmt.setString(5, tokenCount.to64BitsHex())
    pstmt.setInt(6, timestamp)

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillUpdateKIP37Holders(pstmt: PreparedStatement,
                                     tokenCount: BigInt,
                                     lastTransactionTime: Int,
                                     contractAddress: String,
                                     holderAddress: String,
                                     tokenId: BigInt): Unit = {
    pstmt.setString(1, tokenCount.to64BitsHex())
    pstmt.setInt(2, lastTransactionTime)
    pstmt.setString(3, contractAddress)
    pstmt.setString(4, holderAddress)
    pstmt.setString(5, tokenId.toString())

    pstmt.addBatch()
    pstmt.clearParameters()
  }

  private def fillDelKIP37Holders(pstmt: PreparedStatement,
                                  contractAddress: String,
                                  holderAddress: String,
                                  tokenId: BigInt): Unit = {
    pstmt.setString(1, contractAddress)
    pstmt.setString(2, holderAddress)
    pstmt.setString(3, tokenId.toString())

    pstmt.addBatch()
    pstmt.clearParameters()
  }

//  private def minusTokenHolderRedisKey(contractAddress: String, holderAddress: String): String = {
//    s"minusTokenHolder:${contractAddress}_$holderAddress"
//  }
//
//  private def getMinusTokenHolderInfo(contractAddress: String, holderAddress: String): Option[TokenHolders] = {
//    val key = minusTokenHolderRedisKey(contractAddress, holderAddress)
//    SparkRedis.get(key) match {
//      case Some(v) =>
//        SparkRedis.del(key)
//
//        val s                   = v.split("_")
//        val (amount, timestamp) = (BigInt(s(0)), s(1).toInt)
//
//        Some(TokenHolders(contractAddress, holderAddress, amount, timestamp))
//      case _ => None
//    }
//  }
//
//  private def setMinusTokenHolderInfo(tokenHolder: TokenHolders): Unit = {
//    val key = minusTokenHolderRedisKey(tokenHolder.contractAddress, tokenHolder.holderAddress)
//    SparkRedis.setex(key, 86400, s"${tokenHolder.amount}_${tokenHolder.timestamp}")
//  }

  def insertTokenHolders0(insert: Seq[TokenHolders]): Unit = {
    insert.foreach { t =>
      withDB(TokenHolderDB) { c =>
        val pstmtIns = c.prepareStatement(
          s"INSERT IGNORE INTO $TokenHoldersTable (`contract_address`,`holder_address`,`amount`,`last_transaction_time`)" +
            " VALUES (?,?,?,?)")
        pstmtIns.setString(1, t.contractAddress)
        pstmtIns.setString(2, t.holderAddress)
        pstmtIns.setString(3, t.amount.to64BitsHex())
        pstmtIns.setInt(4, t.timestamp)
        pstmtIns.executeUpdate()
        pstmtIns.close()
      }
    }
  }

  def updateTokenHolders(update: Seq[TokenHolders]): Unit = {
    val result = withDB(TokenHolderDB) { c =>
      val pstmtUp = c.prepareStatement(
        s"UPDATE $TokenHoldersTable SET `amount`=?,`last_transaction_time`=?" +
          " WHERE `contract_address`=? AND `holder_address`=?")
      update.foreach { t =>
        pstmtUp.setString(1, t.amount.to64BitsHex())
        pstmtUp.setInt(2, t.timestamp)
        pstmtUp.setString(3, t.contractAddress)
        pstmtUp.setString(4, t.holderAddress)
        pstmtUp.addBatch()
        pstmtUp.clearParameters()
      }
      val result = pstmtUp.executeBatch()
      pstmtUp.clearBatch()
      pstmtUp.close()

      result
    }

    insertTokenHolders0(
      update.zipWithIndex.filter(x => result(x._2) == 0).map(_._1))
  }

  def deleteTokenHolders(delete: Seq[TokenHolders]): Unit = {
    withDB(TokenHolderDB) { c =>
      val pstmtDel =
        c.prepareStatement(
          s"DELETE FROM $TokenHoldersTable WHERE `contract_address`=? AND `holder_address`=?")

      delete.foreach { t =>
        pstmtDel.setString(1, t.contractAddress)
        pstmtDel.setString(2, t.holderAddress)
        pstmtDel.addBatch()
        pstmtDel.clearParameters()
      }

      pstmtDel.executeBatch()
      pstmtDel.clearBatch()
      pstmtDel.close()
    }
  }

  def insertTokenHolders(tokenTransfers: Seq[TokenHolders]): Unit = {
    val insert = mutable.ArrayBuffer.empty[TokenHolders]
    val update = mutable.ArrayBuffer.empty[TokenHolders]
    val delete = mutable.ArrayBuffer.empty[TokenHolders]

    val updatedIds = mutable.ArrayBuffer.empty[Long]

    withDB(TokenHolderDB) { c =>
      val pstmtSelAmount =
        c.prepareStatement(
          s"SELECT `amount`,`last_transaction_time`,`id` FROM $TokenHoldersTable WHERE `contract_address`=? AND `holder_address`=?")

      tokenTransfers.foreach { t =>
        pstmtSelAmount.setString(1, t.contractAddress)
        pstmtSelAmount.setString(2, t.holderAddress)
        val rs = pstmtSelAmount.executeQuery()
        if (rs.next()) {
          var amount = rs.getString(1).hexToBigInt() + t.amount
          val lastTransactionTime = rs.getInt(2)
          updatedIds.append(rs.getLong(3))
          if (amount <= 0) {
            try {
              val kip7 = new KIP7MetadataReader(CaverFactory.caver)
              amount = kip7
                .balanceOf(t.contractAddress, t.holderAddress, t.blockNumber)
                .getOrElse(BigInt(0))
            } catch { case _: Throwable => }
          }
          if (amount <= 0) {
            delete.append(t)
          } else {
            val ts = Math.max(lastTransactionTime, t.timestamp)
            update.append(
              TokenHolders(t.contractAddress,
                           t.holderAddress,
                           amount,
                           ts,
                           t.blockNumber))
          }
        } else if (t.amount > 0) {
          insert.append(t)
//          getMinusTokenHolderInfo(t.contractAddress, t.holderAddress) match {
//            case Some(t2) =>
//              if (t.amount + t2.amount > 0) {
//                msg.append(s"insert: ${t.contractAddress} ${t.holderAddress} ${t.amount} ; ${t.timestamp}")
//                insert.append(t)
//              } else {
//                msg.append(s"delete: ${t.contractAddress} ${t.holderAddress} ${t.timestamp}")
//                delete.append(TokenHolders(t.contractAddress, t.holderAddress, t.amount + t2.amount, t.timestamp))
//              }
//            case _ =>
//              msg.append(s"insert: ${t.contractAddress} ${t.holderAddress} ${t.amount} ${t.timestamp}")
//              insert.append(t)
//          }
        }
        rs.close()
      }

      pstmtSelAmount.close()
    }

    // insert
    insertTokenHolders0(insert)

    // update
    updateTokenHolders(update)

    // delete
    deleteTokenHolders(delete)

    val addHolders = insert
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong)
      .toSeq

    val removeHolders = delete
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong * -1)
      .toSeq

    contractPersistentAPI.updateHolderCount(addHolders)
    contractPersistentAPI.updateHolderCount(removeHolders)

    updatedIds.foreach(id => FinderRedis.del(s"cache/token-holder::$id"))

//    io.klaytn.utils.gcs.GCSUtil
//      .writeText(UserConfig.baseBucket, s"output/holder/${System.currentTimeMillis()}", msg.mkString("\n"))
  }

  def insertKIP17Inventories(nftInventories: Seq[NFTInventories]): Unit = {
    // insert
    withDB(NFTHolderDB) { c =>
      val pstmtInsInventories = c.prepareStatement(
        s"INSERT IGNORE INTO $NFTInventoriesTable (`contract_type`,`contract_address`,`holder_address`," +
          "`token_id`,`token_uri`,`last_transaction_time`) VALUES (2,?,?,?,?,?)"
      )
      nftInventories
        .filter(!_.isSend)
        .foreach { t =>
          fillInsNFTInventories(pstmtInsInventories,
                                t.contractAddress,
                                t.holderAddress,
                                t.tokenId,
                                t.uri,
                                t.timestamp)
        }

      execute(pstmtInsInventories)
      pstmtInsInventories.close()
    }

    val updatedIds = mutable.ArrayBuffer.empty[Long]

    nftInventories.filter(_.isSend).foreach { t =>
      withDB(NFTHolderDB) { c =>
        val pstmt = c.prepareStatement(
          s"SELECT `id` FROM $NFTInventoriesTable WHERE `contract_address`=? AND `holder_address`=? AND `token_id`=?")

        pstmt.setString(1, t.contractAddress)
        pstmt.setString(2, t.holderAddress)
        pstmt.setString(3, t.tokenId.toString())
        val rs = pstmt.executeQuery()
        if (rs.next()) updatedIds.append(rs.getLong(1))
        rs.close()
        pstmt.close()
      }
    }

    // delete
    withDB(NFTHolderDB) { c =>
      val pstmtDelInventories = c.prepareStatement(
        s"DELETE FROM $NFTInventoriesTable" +
          " WHERE `contract_address`=? AND `holder_address`=? AND `token_id`=? and `contract_type`=2")

      nftInventories
        .filter(_.isSend)
        .foreach(
          t =>
            fillDelNFTInventories(pstmtDelInventories,
                                  t.contractAddress,
                                  t.holderAddress,
                                  t.tokenId))

      execute(pstmtDelInventories)
      pstmtDelInventories.close()
    }

    updatedIds.foreach(id => FinderRedis.del(s"cache/nft-inventory::$id"))
  }

//  private def s3logging(contract: String, holder: String, blockNumber: Long, path: String, msg: String): Unit = {
//    GCSUtil.writeText(UserConfig.baseBucket,
//                     s"log/${UserConfig.chainPhase}/${blockNumber / 10000}/$contract/$holder/$path",
//                     msg)
//  }

  def insertKIP17Holders(nftHolders0: Seq[NFTHolders]): Unit = {
    val insert = mutable.ArrayBuffer.empty[NFTHolders]
    val update = mutable.ArrayBuffer.empty[NFTHolders]
    val delete = mutable.ArrayBuffer.empty[NFTHolders]

    val updatedIds = mutable.ArrayBuffer.empty[Long]

    val data = mutable.Map.empty[(String, String), (BigInt, Int, Long)]
    nftHolders0.foreach { nftHolder =>
      val key = (nftHolder.contractAddress, nftHolder.holderAddress)
      val (amount0, ts0, blockNumber0) = data.getOrElse(key, (BigInt(0), 0, 0L))
      val amount = amount0 + nftHolder.tokenCount
      val ts = if (ts0 >= nftHolder.timestamp) ts0 else nftHolder.timestamp
      val blockNumber =
        if (blockNumber0 >= nftHolder.blockNumber) blockNumber0
        else nftHolder.blockNumber
      data(key) = (amount, ts, blockNumber)
    }

    val nftHolders = data.map {
      case ((contractAddress, holderAddress), (tokenCount, ts, blockNumber)) =>
//        s3logging(contractAddress, holderAddress, blockNumber, s"$blockNumber.step1.$tokenCount", "")
        NFTHolders(contractAddress,
                   holderAddress,
                   null,
                   tokenCount,
                   null,
                   ts,
                   blockNumber)
    }

    withDB(NFTHolderDB) { c =>
      val pstmtSelTokenCount =
        c.prepareStatement(
          s"SELECT `token_count`,`last_transaction_time`,`id` FROM $NFTHoldersTable WHERE `contract_address`=? AND `holder_address`=?")

      nftHolders.foreach { t =>
        pstmtSelTokenCount.setString(1, t.contractAddress)
        pstmtSelTokenCount.setString(2, t.holderAddress)

        val rs = pstmtSelTokenCount.executeQuery()
        if (rs.next()) {
          var amount = rs.getString(1).hexToBigInt() + t.tokenCount
          val lastTransactionTime = rs.getInt(2)
          updatedIds.append(rs.getLong(3))
          if (amount <= 0) {
            try {
              val kip17 = new KIP17MetadataReader(CaverFactory.caver)
              amount = kip17
                .balanceOf(t.contractAddress, t.holderAddress, t.blockNumber)
                .getOrElse(BigInt(0))
//              s3logging(
//                t.contractAddress,
//                t.holderAddress,
//                t.blockNumber,
//                s"${t.blockNumber}.step2-check.db.${rs.getString(1).hexToBigInt()}.log.${t.tokenCount}.chain.$amount",
//                ""
//              )
            } catch { case _: Throwable => }
          } else {
//            s3logging(t.contractAddress,
//                      t.holderAddress,
//                      t.blockNumber,
//                      s"${t.blockNumber}.step2-update.db.${rs.getString(1).hexToBigInt()}.log.${t.tokenCount}",
//                      "")
          }

          if (amount <= 0) delete.append(t)
          else {
            val ts = Math.max(lastTransactionTime, t.timestamp)
            update.append(
              NFTHolders(t.contractAddress,
                         t.holderAddress,
                         null,
                         amount,
                         null,
                         ts,
                         t.blockNumber))
          }
        } else if (t.tokenCount > 0) {
//          s3logging(t.contractAddress,
//                    t.holderAddress,
//                    t.blockNumber,
//                    s"${t.blockNumber}.step2-insert.log.${t.tokenCount}",
//                    "")
          insert.append(t)
        }

        rs.close()
      }

      pstmtSelTokenCount.close()
    }

    // insert
    if (insert.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtInsHolders = c.prepareStatement(
          s"INSERT IGNORE INTO $NFTHoldersTable (`contract_address`,`holder_address`,`token_count`,`last_transaction_time`)" +
            " VALUES (?,?,?,?)"
        )
        insert.foreach(
          t =>
            fillInsNFTHolders(pstmtInsHolders,
                              t.contractAddress,
                              t.holderAddress,
                              t.tokenCount,
                              t.timestamp))
        execute(pstmtInsHolders)
        pstmtInsHolders.close()
      }
    }

    // update
    if (update.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtUpHolders = c.prepareStatement(
          s"UPDATE $NFTHoldersTable SET `token_count`=?,`last_transaction_time`=?" +
            " WHERE `contract_address`=? AND `holder_address`=?"
        )
        update.foreach(
          t =>
            fillUpdateNFTHolders(pstmtUpHolders,
                                 t.tokenCount,
                                 t.timestamp,
                                 t.contractAddress,
                                 t.holderAddress))
        execute(pstmtUpHolders)
        pstmtUpHolders.close()
      }
    }

    // delete
    if (delete.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtDelHolders =
          c.prepareStatement(
            s"DELETE FROM $NFTHoldersTable WHERE `contract_address`=? AND `holder_address`=?")
        delete.foreach(
          t =>
            fillDelNFTHolders(pstmtDelHolders,
                              t.contractAddress,
                              t.holderAddress))
        execute(pstmtDelHolders)
        pstmtDelHolders.close()
      }
    }

    val addHolders = insert
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong)
      .toSeq

    val removeHolders = delete
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong * -1)
      .toSeq

    contractPersistentAPI.updateHolderCount(addHolders)
    contractPersistentAPI.updateHolderCount(removeHolders)

    updatedIds.foreach(id => FinderRedis.del(s"cache/nft-17-holder::$id"))
  }

  def insertKIP37Holders(nftHolders: Seq[NFTHolders]): Unit = {
    val insert = mutable.ArrayBuffer.empty[NFTHolders]
    val update = mutable.ArrayBuffer.empty[NFTHolders]
    val delete = mutable.ArrayBuffer.empty[NFTHolders]

    val updatedIds = mutable.ArrayBuffer.empty[Long]

    withDB(NFTHolderDB) { c =>
      val pstmtSelTokenCount = c.prepareStatement(
        s"SELECT `token_count`,`last_transaction_time`,`id` FROM $NFTInventoriesTable" +
          " WHERE `contract_address`=? AND `holder_address`=? AND `token_id`=? and `contract_type`=3")

      nftHolders.foreach { t =>
        pstmtSelTokenCount.setString(1, t.contractAddress)
        pstmtSelTokenCount.setString(2, t.holderAddress)
        pstmtSelTokenCount.setString(3, t.tokenId.toString)
        val rs = pstmtSelTokenCount.executeQuery()
        if (rs.next()) {
          var amount = rs.getString(1).hexToBigInt() + t.tokenCount
          val lastTransactionTime = rs.getInt(2)
          updatedIds.append(rs.getLong(3))
          if (amount <= 0) {
            try {
              val kip37 = new KIP37MetadataReader(CaverFactory.caver)
              amount = kip37
                .balanceOf(t.contractAddress,
                           t.holderAddress,
                           t.tokenId,
                           t.blockNumber)
                .getOrElse(BigInt(0))
            } catch { case _: Throwable => }
          }
          if (amount <= 0) delete.append(t)
          else {
            val ts = Math.max(lastTransactionTime, t.timestamp)
            update.append(
              NFTHolders(t.contractAddress,
                         t.holderAddress,
                         t.tokenId,
                         amount,
                         t.uri,
                         ts,
                         t.blockNumber))
          }
        } else if (t.tokenCount > 0) insert.append(t)

        rs.close()
      }

      pstmtSelTokenCount.close()
    }

    // insert
    if (insert.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtIns = c.prepareStatement(
          s"INSERT IGNORE INTO $NFTInventoriesTable (`contract_type`,`contract_address`,`holder_address`,`token_id`," +
            "`token_uri`,`token_count`,`last_transaction_time`) VALUES (3,?,?,?,?,?,?)"
        )
        insert.foreach { t =>
//          val uri = ContractUtil.getTokenUri(ContractType.KIP37, t.contractAddress, t.tokenId)
          fillInsKIP37Holders(pstmtIns,
                              t.contractAddress,
                              t.holderAddress,
                              t.tokenId,
                              t.uri,
                              t.tokenCount,
                              t.timestamp)
        }
        execute(pstmtIns)
        pstmtIns.close()
      }
    }

    // update
    if (update.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtUp = c.prepareStatement(
          s"UPDATE $NFTInventoriesTable SET `token_count`=?,`last_transaction_time`=?" +
            " WHERE `contract_address`=? AND `holder_address`=? AND `token_id`=?"
        )
        update.foreach(
          t =>
            fillUpdateKIP37Holders(pstmtUp,
                                   t.tokenCount,
                                   t.timestamp,
                                   t.contractAddress,
                                   t.holderAddress,
                                   t.tokenId))
        execute(pstmtUp)
        pstmtUp.close()
      }
    }

    // delete
    if (delete.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmtDel = c.prepareStatement(
          s"DELETE FROM $NFTInventoriesTable" +
            " WHERE `contract_address`=? AND `holder_address`=? AND `token_id`=? and `contract_type`=3"
        )
        delete.foreach(
          t =>
            fillDelKIP37Holders(pstmtDel,
                                t.contractAddress,
                                t.holderAddress,
                                t.tokenId))
        execute(pstmtDel)
        pstmtDel.close()
      }
    }

    val addHolders = insert
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong)
      .toSeq

    val removeHolders = delete
      .map(x => (x.contractAddress, x.holderAddress))
      .toSet
      .toSeq
      .groupBy((x: (String, String)) => x._1)
      .mapValues(_.map(_._2).length.toLong * -1)
      .toSeq

    contractPersistentAPI.updateHolderCount(addHolders)
    contractPersistentAPI.updateHolderCount(removeHolders)

    updatedIds.foreach(id => FinderRedis.del(s"cache/nft-inventory::$id"))
  }

  def getTokenBalanceAndId(contractAddress: String,
                           holderAddress: String): Option[(BigInt, Long)] = {
    withDB(TokenHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT `amount`,`id` FROM `$TokenHoldersTable` WHERE `contract_address`=? AND `holder_address`=?")
      pstmt.setString(1, contractAddress)
      pstmt.setString(2, holderAddress)
      val rs = pstmt.executeQuery()
      val balance =
        if (rs.next()) Some((rs.getString(1).hexToBigInt(), rs.getLong(2)))
        else None

      rs.close()
      pstmt.close()

      balance
    }
  }

  def updateTokenBalance(contractAddress: String,
                         holderAddress: String,
                         amount: BigInt): Unit = {
    val result = withDB(TokenHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE `$TokenHoldersTable` SET `amount`=? WHERE `contract_address`=? AND `holder_address`=?")
      pstmt.setString(1, amount.to64BitsHex())
      pstmt.setString(2, contractAddress)
      pstmt.setString(3, holderAddress)
      val result = pstmt.executeUpdate()
      pstmt.close()
      result
    }

    if (result == 0)
      insertTokenHolders0(
        Seq(TokenHolders(contractAddress, holderAddress, amount, 0, 0L)))
  }

  def getNFTBalanceAndId(contractAddress: String,
                         holderAddress: String): Option[(BigInt, Long)] = {
    withDB(NFTHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT `token_count`,`id` FROM `$NFTHoldersTable` WHERE `contract_address`=? AND `holder_address`=?")
      pstmt.setString(1, contractAddress)
      pstmt.setString(2, holderAddress)
      val rs = pstmt.executeQuery()
      val balance =
        if (rs.next()) Some((rs.getString(1).hexToBigInt(), rs.getLong(2)))
        else None

      rs.close()
      pstmt.close()

      balance
    }
  }

  def updateNFTBalance(contractAddress: String,
                       holderAddress: String,
                       amount: BigInt): Unit = {
    withDB(NFTHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE `$NFTHoldersTable` SET `token_count`=? WHERE `contract_address`=? AND `holder_address`=?")
      pstmt.setString(1, amount.to64BitsHex())
      pstmt.setString(2, contractAddress)
      pstmt.setString(3, holderAddress)
      val result = pstmt.executeUpdate()
      pstmt.close()
      result
    }
  }

  def insertCorrectHolderHistory(contract: String,
                                 holder: String,
                                 chainAmount: BigInt,
                                 dbAmount: BigInt): Unit = {
    withDB(TokenHolderDB) { c =>
      val pstmt = c.prepareStatement(
        "INSERT INTO `correct_holder_history` (`contract_address`,`holder_address`,`chain_balance`,`db_balance`) VALUES (?,?,?,?)")

      pstmt.setString(1, contract)
      pstmt.setString(2, holder)
      pstmt.setString(3, chainAmount.to64BitsHex())
      pstmt.setString(4, dbAmount.to64BitsHex())
      pstmt.execute()
      pstmt.close()
    }
  }

  def getTokenHolderCount(contractAddress: String): Long = {
    withDB(TokenHolderDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT COUNT(*) FROM `$TokenHoldersTable` WHERE `contract_address`=?")
      pstmt.setString(1, contractAddress)
      val rs = pstmt.executeQuery()
      val result =
        if (rs.next()) rs.getLong(1)
        else 0L

      rs.close()
      pstmt.close()

      result
    }
  }

  /**
    * Update Token URI (BULK) in nft_inventories table
    *
    * @param tokens Seq[(contractAddress, tokenId, tokenUri)]
    */
  def updateTokenUriBulk(tokens: Seq[(String, String, String)]): Unit = {
    // Bulk Update
    // update
    if (tokens.nonEmpty) {
      withDB(NFTHolderDB) { c =>
        val pstmt = c.prepareStatement(
          s"UPDATE $NFTInventoriesTable SET `token_uri`=?" +
            " WHERE `contract_address`=? AND `token_id`=?"
        )
        tokens.map(
          t => {
            val (contractAddress, tokenId, tokenUri) = t
            fillUpdateTokenUriBulk(pstmt, tokenUri, contractAddress, tokenId)
          }
        )
        val result = execute(pstmt)
        pstmt.close()
        result
      }
    }

  }

  def getNFTHolderCount(contractAddress: String): Long = {
    withDB(NFTHolderDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT COUNT(*) FROM `$NFTHoldersTable` WHERE `contract_address`=?")
      pstmt.setString(1, contractAddress)
      val rs = pstmt.executeQuery()
      val result =
        if (rs.next()) rs.getLong(1)
        else 0L

      rs.close()
      pstmt.close()

      result
    }
  }
}
