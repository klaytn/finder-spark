package io.klaytn.persistent.impl.rdb

import io.klaytn.persistent.NFTItemPersistentAPI
import io.klaytn.repository.{NFTItem, NFTItemRepository}

class RDBNFTItemPersistentAPI
    extends NFTItemRepository
    with NFTItemPersistentAPI {
  override def insert(nftItem: NFTItem): Unit = {
//    try {
    super.insert(nftItem)
//    } catch {
//      case e: Throwable =>
//        S3Util.writeText(
//          "klaytn-prod-spark",
//          s"output/fastworker/nftitems/${System.currentTimeMillis()}.step3.${nftItem}",
//          e.getStackTraceString
//        )
//    }
  }

  override def findByContractAddressAndTokenId(
      contractAddress: String,
      tokenId: BigInt): Option[NFTItem] = {
    super.findByContractAddressAndTokenId(contractAddress, tokenId)
  }

  override def updateBurnAmountAndTotalBurnByContractAddressAndTokenId(
      burnAmount: BigInt,
      totalBurn: Long,
      contractAddress: String,
      tokenId: BigInt): Unit = {
    super.updateBurnAmountAndTotalBurnByContractAddressAndTokenId(
      burnAmount,
      totalBurn,
      contractAddress,
      tokenId)
  }

  override def updateTotalTransferAndTokenUriByContractAddressAndTokenId(
      totalTransfer: Long,
      tokenUri: String,
      contractAddress: String,
      tokenId: BigInt): Unit = {
    super.updateTotalTransferAndTokenUriByContractAddressAndTokenId(
      totalTransfer,
      tokenUri,
      contractAddress,
      tokenId)
  }
}
