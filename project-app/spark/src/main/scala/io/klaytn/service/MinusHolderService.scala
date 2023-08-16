package io.klaytn.service

import io.klaytn._
import io.klaytn.dsl.db.withDB
import io.klaytn.persistent.HolderPersistentAPI
import io.klaytn.repository.HolderRepository
import io.klaytn.utils.klaytn.NumberConverter.BigIntConverter

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class MinusHolderService(holderPersistentAPI: LazyEval[HolderPersistentAPI])
    extends Serializable {
  def getMinusTokenHolders(): Seq[(String, String)] = {
    val result = ArrayBuffer.empty[(String, String)]
    withDB(HolderRepository.TokenHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT contract_address, holder_address FROM token_holders WHERE amount LIKE '%-%' OR amount='0x0000000000000000000000000000000000000000000000000000000000000000'")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append((rs.getString(1), rs.getString(2)))
      }
      rs.close()
      pstmt.close()
    }
    result
  }

  def modifyMinusTokenHolder(contractAddress: String,
                             holderAddress: String): Unit = {
    try {
      val kip7 = CaverFactory.caver.kct.kip7.create(contractAddress)
      val balance =
        Try(kip7.balanceOf(holderAddress)).getOrElse(BigInt(0).bigInteger)
      if (balance == BigInt(0)) {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val pstmt =
            c.prepareStatement(
              "DELETE FROM token_holders WHERE contract_address=? AND holder_address=?")
          pstmt.setString(1, contractAddress)
          pstmt.setString(2, holderAddress)
          pstmt.execute()
        }
      } else {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val amount = BigInt(balance).to64BitsHex()
          val pstmt =
            c.prepareStatement(
              "UPDATE token_holders SET amount=? WHERE contract_address=? AND holder_address=?")
          pstmt.setString(1, amount)
          pstmt.setString(2, contractAddress)
          pstmt.setString(3, holderAddress)
          pstmt.execute()
        }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }

  def getMinusKIP17Holders(): Seq[(String, String)] = {
    val result = ArrayBuffer.empty[(String, String)]
    withDB(HolderRepository.TokenHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT contract_address, holder_address FROM nft_holders WHERE token_count LIKE '%-%' OR token_count='0x0000000000000000000000000000000000000000000000000000000000000000'")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append((rs.getString(1), rs.getString(2)))
      }
      rs.close()
      pstmt.close()
    }
    result
  }

  def modifyMinusKIP17Holder(contractAddress: String,
                             holderAddress: String): Unit = {
    try {
      val kip17 = CaverFactory.caver.kct.kip17.create(contractAddress)
      val balance =
        Try(kip17.balanceOf(holderAddress)).getOrElse(BigInt(0).bigInteger)
      if (balance == BigInt(0)) {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val pstmt =
            c.prepareStatement(
              "DELETE FROM nft_holders WHERE contract_address=? AND holder_address=?")
          pstmt.setString(1, contractAddress)
          pstmt.setString(2, holderAddress)
          pstmt.execute()

          val pstmt2 =
            c.prepareStatement(
              "DELETE FROM nft_inventories WHERE contract_address=? AND holder_address=?")
          pstmt2.setString(1, contractAddress)
          pstmt2.setString(2, holderAddress)
          pstmt2.execute()
        }
      } else {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val amount = BigInt(balance).to64BitsHex()
          val pstmt =
            c.prepareStatement(
              "UPDATE nft_holders SET token_count=? WHERE contract_address=? AND holder_address=?")
          pstmt.setString(1, amount)
          pstmt.setString(2, contractAddress)
          pstmt.setString(3, holderAddress)
          pstmt.execute()
        }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }

  def getMinusKIP37Holders(): Seq[(String, String, String)] = {
    val result = ArrayBuffer.empty[(String, String, String)]
    withDB(HolderRepository.TokenHolderDB) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT contract_address, holder_address, token_id FROM nft_inventories WHERE contract_type=3 AND token_count LIKE '%-%' OR token_count='0x0000000000000000000000000000000000000000000000000000000000000000'")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append((rs.getString(1), rs.getString(2), rs.getString(3)))
      }
      rs.close()
      pstmt.close()
    }
    result
  }

  def modifyMinusKIP37Holder(contractAddress: String,
                             holderAddress: String,
                             tokenId: String): Unit = {
    try {
      val kip37 = CaverFactory.caver.kct.kip37.create(contractAddress)
      val balance = Try(kip37.balanceOf(holderAddress, tokenId))
        .getOrElse(BigInt(0).bigInteger)
      if (balance == BigInt(0)) {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val pstmt =
            c.prepareStatement(
              "DELETE FROM nft_inventories WHERE contract_address=? AND holder_address=? AND token_id=? AND contract_type=3")
          pstmt.setString(1, contractAddress)
          pstmt.setString(2, holderAddress)
          pstmt.setString(3, tokenId)
          pstmt.execute()
        }
      } else {
        withDB(HolderRepository.TokenHolderDB) { c =>
          val amount = BigInt(balance).to64BitsHex()
          val pstmt =
            c.prepareStatement(
              "UPDATE nft_inventories SET token_count=? WHERE contract_address=? AND holder_address=? AND token_id=? AND contract_type=3")
          pstmt.setString(1, amount)
          pstmt.setString(2, contractAddress)
          pstmt.setString(3, holderAddress)
          pstmt.setString(4, tokenId)
          pstmt.execute()
        }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }

}
