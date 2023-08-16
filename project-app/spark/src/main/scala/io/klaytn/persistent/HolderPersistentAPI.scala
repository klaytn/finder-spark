package io.klaytn.persistent

import io.klaytn._
import io.klaytn.model.ChainPhase
import io.klaytn.persistent.impl.rdb.RDBHolderPersistentAPI
import io.klaytn.repository.{NFTHolders, NFTInventories, TokenHolders}

object HolderPersistentAPI {
  def of(chainPhase: ChainPhase,
         contractPersistentAPI: LazyEval[ContractPersistentAPI])
    : HolderPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBHolderPersistentAPI(contractPersistentAPI)
    }
  }
}

trait HolderPersistentAPI extends Serializable {
  def insertNFTPatternedUri(contractAddress: String, tokenUri: String): Unit
  def getNFTPatternedUri(contractAddress: String): Option[String]
  def getNFTUri(contractAddress: String, tokenId: BigInt): Option[String]
  def updateNFTUri(contractAddress: String, tokenId: BigInt, uri: String): Unit
  def insertTokenHolders(tokenTransfers: Seq[TokenHolders]): Unit
  def insertKIP17Inventories(nftInventories: Seq[NFTInventories]): Unit
  def insertKIP17Holders(nftHolders: Seq[NFTHolders]): Unit
  def insertKIP37Holders(nftHolders: Seq[NFTHolders]): Unit
  def updateTokenHolders(update: Seq[TokenHolders]): Unit
  def deleteTokenHolders(delete: Seq[TokenHolders]): Unit
  def getTokenBalanceAndId(contractAddress: String,
                           holderAddress: String): Option[(BigInt, Long)]
  def updateTokenBalance(contractAddress: String,
                         holderAddress: String,
                         amount: BigInt): Unit
  def getNFTBalanceAndId(contractAddress: String,
                         holderAddress: String): Option[(BigInt, Long)]
  def updateNFTBalance(contractAddress: String,
                       holderAddress: String,
                       amount: BigInt): Unit
  def insertCorrectHolderHistory(contract: String,
                                 holder: String,
                                 chainAmount: BigInt,
                                 dbAmount: BigInt): Unit
  def getTokenHolderCount(contractAddress: String): Long
  def getNFTHolderCount(contractAddress: String): Long

  def updateTokenUriBulk(tokens: Seq[(String, String, String)]): Unit
}
