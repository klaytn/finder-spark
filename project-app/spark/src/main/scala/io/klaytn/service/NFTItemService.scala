package io.klaytn.service

import io.klaytn._
import io.klaytn.contract.lib.KIP37MetadataReader
import io.klaytn.model.finder.ContractType
import io.klaytn.persistent.NFTItemPersistentAPI
import io.klaytn.repository.NFTItem
import io.klaytn.utils.gcs.GCSUtil

class NFTItemService(nftItemPersistentAPI: LazyEval[NFTItemPersistentAPI],
                     contractService: LazyEval[ContractService])
    extends Serializable {
  def updateTotalTransfer(contractAddress: String,
                          contractType: ContractType.Value,
                          tokenId: BigInt,
                          tokenUri: String,
                          totalTransfer: Int): Unit = {
    val a = nftItemPersistentAPI.findByContractAddressAndTokenId(
      contractAddress,
      tokenId)

    GCSUtil.writeText(
      "klaytn-prod-spark",
      s"output/fastworker/nftitems/${System
        .currentTimeMillis()}.step2.$contractAddress.$tokenId.$a",
      ""
    )

    nftItemPersistentAPI.findByContractAddressAndTokenId(contractAddress,
                                                         tokenId) match {
      case Some(dbNFTItem) =>
        nftItemPersistentAPI
          .updateTotalTransferAndTokenUriByContractAddressAndTokenId(
            dbNFTItem.totalTransfer + totalTransfer,
            tokenUri,
            contractAddress,
            tokenId)
      case _ =>
        try {
          val nftItem =
            if (contractType == ContractType.KIP17 || contractType == ContractType.ERC721) {
              NFTItem(contractAddress,
                      contractType,
                      tokenId,
                      tokenUri,
                      None,
                      0L,
                      None,
                      0L)
            } else {
              val kip37 = new KIP37MetadataReader(CaverFactory.caver)
              val totalSupply = kip37.totalSupply(contractAddress, tokenId)
              NFTItem(contractAddress,
                      contractType,
                      tokenId,
                      tokenUri,
                      totalSupply,
                      totalTransfer,
                      Option(BigInt(0)),
                      0L)
            }
          GCSUtil.writeText(
            "klaytn-prod-spark",
            s"output/fastworker/nftitems/${System.currentTimeMillis()}.step3.$nftItem",
            ""
          )
          nftItemPersistentAPI.insert(nftItem)
        } catch { case _: Throwable => }
    }
  }

  def updateBurn(contractAddress: String,
                 contractType: ContractType.Value,
                 tokenId: BigInt,
                 burnAmount: BigInt,
                 totalBurns: Int): Unit = {
    nftItemPersistentAPI.findByContractAddressAndTokenId(contractAddress,
                                                         tokenId) match {
      case Some(dbNFTItem) =>
        nftItemPersistentAPI
          .updateBurnAmountAndTotalBurnByContractAddressAndTokenId(
            burnAmount + dbNFTItem.burnAmount.getOrElse(BigInt(0L)),
            totalBurns + dbNFTItem.totalBurn,
            contractAddress,
            tokenId
          )
      case _ =>
        val tokenUri =
          contractService.getTokenUri(contractType, contractAddress, tokenId)

        try {
          val nftItem =
            if (contractType == ContractType.KIP17 || contractType == ContractType.ERC721) {
              NFTItem(contractAddress,
                      contractType,
                      tokenId,
                      tokenUri,
                      None,
                      0L,
                      None,
                      0L)
            } else {
              val kip37 = new KIP37MetadataReader(CaverFactory.caver)
              val totalSupply = kip37.totalSupply(contractAddress, tokenId)
              NFTItem(contractAddress,
                      contractType,
                      tokenId,
                      tokenUri,
                      totalSupply,
                      0L,
                      Option(BigInt(0)),
                      0L)
            }
          nftItemPersistentAPI.insert(nftItem)
        } catch { case _: Throwable => }
    }
  }
}
