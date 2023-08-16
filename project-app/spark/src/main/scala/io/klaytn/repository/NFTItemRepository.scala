package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.finder.ContractType
import io.klaytn.repository.NFTItemRepository.{NFTITemTable, NFTItemDB}
import io.klaytn.utils.klaytn.NumberConverter._

object NFTItemRepository {
  val NFTItemDB: String = "finder03"
  val NFTITemTable = "nft_items"
}

case class NFTItem(contractAddress: String,
                   contractType: ContractType.Value,
                   tokenId: BigInt,
                   tokenUri: String,
                   totalSupply: Option[BigInt],
                   totalTransfer: Long,
                   burnAmount: Option[BigInt],
                   totalBurn: Long)

abstract class NFTItemRepository extends AbstractRepository {
  def insert(nftItem: NFTItem): Unit = {
    withDB(NFTItemDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $NFTITemTable (`contract_type`, `contract_address`, `token_id`, `token_uri`, " +
          "`total_supply`, `total_transfer`, `burn_amount`, `total_burn`) VALUES (?,?,?,?,?,?,?,?)")
      pstmt.setInt(1, nftItem.contractType.id)
      pstmt.setString(2, nftItem.contractAddress)
      pstmt.setString(3, nftItem.tokenId.toString())
      pstmt.setString(4, nftItem.tokenUri)

      if (nftItem.totalSupply.isDefined)
        pstmt.setString(5, nftItem.totalSupply.get.to64BitsHex())
      else pstmt.setString(5, null)

      pstmt.setLong(6, nftItem.totalTransfer)

      if (nftItem.burnAmount.isDefined)
        pstmt.setString(7, nftItem.burnAmount.get.to64BitsHex())
      else pstmt.setString(7, null)

      pstmt.setLong(8, nftItem.totalBurn)

      pstmt.execute()
      pstmt.close()
    }
  }

  def findByContractAddressAndTokenId(contractAddress: String,
                                      tokenId: BigInt): Option[NFTItem] = {
    withDB(NFTItemDB) { c =>
      val pstmt = c.prepareStatement(
        "SELECT `contract_address`, `contract_type`, `token_id`, `token_uri`, `total_supply`, " +
          s" `total_transfer`, `burn_amount`, `total_burn` FROM $NFTITemTable WHERE `contract_address`=? AND `token_id`=?")
      pstmt.setString(1, contractAddress)
      pstmt.setString(2, tokenId.toString())
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        val contractType = ContractType.from(rs.getInt(2))
        NFTItem(
          rs.getString(1),
          contractType,
          BigInt(rs.getString(3)),
          rs.getString(4),
          Option(rs.getString(5).hexToBigInt()),
          rs.getLong(6),
          Option(rs.getString(7).hexToBigInt()),
          rs.getLong(8)
        )
      } else null

      rs.close()
      pstmt.close()

      Option(result)
    }
  }

  def updateBurnAmountAndTotalBurnByContractAddressAndTokenId(
      burnAmount: BigInt,
      totalBurn: Long,
      contractAddress: String,
      tokenId: BigInt): Unit = {
    withDB(NFTItemDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $NFTITemTable SET `burn_amount`=?, `total_burn`=? WHERE `contract_address`=? AND `token_id`=?")

      pstmt.setString(1, burnAmount.to64BitsHex())
      pstmt.setLong(2, totalBurn)
      pstmt.setString(3, contractAddress)
      pstmt.setString(4, tokenId.toString())
      pstmt.executeUpdate()
      pstmt.close()
    }
  }

  def updateTotalTransferAndTokenUriByContractAddressAndTokenId(
      totalTransfer: Long,
      tokenUri: String,
      contractAddress: String,
      tokenId: BigInt): Unit = {
    withDB(NFTItemDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $NFTITemTable SET `total_transfer`=?, `token_uri`=? WHERE `contract_address`=? AND `token_id`=?")

      pstmt.setLong(1, totalTransfer)
      pstmt.setString(2, tokenUri)
      pstmt.setString(3, contractAddress)
      pstmt.setString(4, tokenId.toString())
      pstmt.executeUpdate()
      pstmt.close()
    }
  }
}
