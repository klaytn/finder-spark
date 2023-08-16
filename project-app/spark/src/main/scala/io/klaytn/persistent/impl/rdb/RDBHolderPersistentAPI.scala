package io.klaytn.persistent.impl.rdb

import io.klaytn._
import io.klaytn.persistent.{ContractPersistentAPI, HolderPersistentAPI}
import io.klaytn.repository.{
  HolderRepository,
  NFTHolders,
  NFTInventories,
  TokenHolders
}

class RDBHolderPersistentAPI(
    contractPersistentAPI: LazyEval[ContractPersistentAPI])
    extends HolderRepository(contractPersistentAPI)
    with HolderPersistentAPI {
  override def insertNFTPatternedUri(contractAddress: String,
                                     tokenUri: String): Unit = {
    super.insertNFTPatternedUri(contractAddress, tokenUri)
  }

  override def getNFTPatternedUri(contractAddress: String): Option[String] = {
    super.getNFTPatternedUri(contractAddress)
  }

  override def getNFTUri(contractAddress: String,
                         tokenId: BigInt): Option[String] = {
    super.getNFTUri(contractAddress, tokenId)
  }

  override def updateNFTUri(contractAddress: String,
                            tokenId: BigInt,
                            uri: String): Unit = {
    super.updateNFTUri(contractAddress, tokenId, uri)
  }

  override def insertTokenHolders(tokenTransfers: Seq[TokenHolders]): Unit = {
    super.insertTokenHolders(tokenTransfers)
  }

  override def insertKIP17Inventories(
      nftInventories: Seq[NFTInventories]): Unit = {
    super.insertKIP17Inventories(nftInventories)
  }

  override def insertKIP17Holders(nftHolders: Seq[NFTHolders]): Unit = {
    super.insertKIP17Holders(nftHolders)
  }

  override def insertKIP37Holders(nftHolders: Seq[NFTHolders]): Unit = {
    super.insertKIP37Holders(nftHolders)
  }

  override def updateTokenHolders(update: Seq[TokenHolders]): Unit = {
    super.updateTokenHolders(update)
  }

  override def deleteTokenHolders(delete: Seq[TokenHolders]): Unit = {
    super.deleteTokenHolders(delete)
  }

  override def getTokenBalanceAndId(
      contractAddress: String,
      holderAddress: String): Option[(BigInt, Long)] = {
    super.getTokenBalanceAndId(contractAddress, holderAddress)
  }

  override def updateTokenBalance(contractAddress: String,
                                  holderAddress: String,
                                  amount: BigInt): Unit = {
    super.updateTokenBalance(contractAddress, holderAddress, amount)
  }

  override def getNFTBalanceAndId(
      contractAddress: String,
      holderAddress: String): Option[(BigInt, Long)] = {
    super.getNFTBalanceAndId(contractAddress, holderAddress)
  }

  override def updateNFTBalance(contractAddress: String,
                                holderAddress: String,
                                amount: BigInt): Unit = {
    super.updateNFTBalance(contractAddress, holderAddress, amount)
  }

  override def insertCorrectHolderHistory(contract: String,
                                          holder: String,
                                          chainAmount: BigInt,
                                          dbAmount: BigInt): Unit =
    super.insertCorrectHolderHistory(contract, holder, chainAmount, dbAmount)

  override def getTokenHolderCount(contractAddress: String): Long =
    super.getTokenHolderCount(contractAddress)

  override def getNFTHolderCount(contractAddress: String): Long =
    super.getNFTHolderCount(contractAddress)

  override def updateTokenUriBulk(tokens: Seq[(String, String, String)]): Unit =
    super.updateTokenUriBulk(tokens)
}
