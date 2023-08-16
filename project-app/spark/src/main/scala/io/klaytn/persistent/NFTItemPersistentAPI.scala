package io.klaytn.persistent

import io.klaytn.model.ChainPhase
import io.klaytn.persistent.impl.rdb.RDBNFTItemPersistentAPI
import io.klaytn.repository.NFTItem

object NFTItemPersistentAPI {
  def of(chainPhase: ChainPhase): NFTItemPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBNFTItemPersistentAPI()
    }
  }
}

trait NFTItemPersistentAPI extends Serializable {
  def insert(nftItem: NFTItem): Unit
  def findByContractAddressAndTokenId(contractAddress: String,
                                      tokenId: BigInt): Option[NFTItem]
  def updateBurnAmountAndTotalBurnByContractAddressAndTokenId(
      burnAmount: BigInt,
      totalBurn: Long,
      contractAddress: String,
      tokenId: BigInt): Unit
  def updateTotalTransferAndTokenUriByContractAddressAndTokenId(
      totalTransfer: Long,
      tokenUri: String,
      contractAddress: String,
      tokenId: BigInt): Unit
}
