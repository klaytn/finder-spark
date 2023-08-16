package io.klaytn.persistent

import io.klaytn.model.finder.{
  NFTApprove,
  NFTTransfer,
  TokenApprove,
  TokenTransfer
}
import io.klaytn.model.{ChainPhase, RefinedEventLog}
import io.klaytn.persistent.impl.rdb.RDBTransferPersistentAPI
import io.klaytn.repository.AccountTransferContracts

import scala.collection.mutable.ArrayBuffer

object TransferPersistentAPI {
  def of(chainPhase: ChainPhase): RDBTransferPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBTransferPersistentAPI()
    }
  }
}

trait TransferPersistentAPI extends Serializable {
  def insertAccountTransferContracts(
      accountTransferContracts: Seq[AccountTransferContracts]): Unit
  def insertTokenTransferEvent(eventLogs: Seq[RefinedEventLog],
                               insertResult: ArrayBuffer[Int]): Unit
  def insertNFTTransferEvent(transfers: Seq[NFTTransfer]): Unit
  def getNFTTransferMinBlockNumber(): Long
  def getNFTTransfers(tableId: Long, cnt: Int): Seq[NFTTransfer]
  def getNFTTransfersWithoutRetry(tableId: Long, cnt: Int): Seq[NFTTransfer]
  def getTokenTransferMinBlockNumber(): Long
  def getTokenTransfers(tableId: Long, cnt: Int): Seq[TokenTransfer]
  def findTokenTransfersByBlockNumbers(
      blockNumbers: Seq[Long]): Seq[TokenTransfer]
  def findNFTTransfersByBlockNumbers(blockNumbers: Seq[Long]): Seq[NFTTransfer]
  def findTokenTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String]
  def findNFTTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String]
  def findTokenBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String]
  def findNFTBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String]
  def insertTokenBurnEvent(eventLogs: Seq[RefinedEventLog]): Unit
  def insertNFTBurnEvent(transfers: Seq[NFTTransfer]): Unit
  def getTokenBurns(tableId: Long, cnt: Int): Seq[TokenTransfer]
  def getNFTBurns(tableId: Long, cnt: Int): Seq[NFTTransfer]

  def insertTokenApprove(tokenApproves: Seq[TokenApprove]): Unit

  def findTokenApproveByAccountAndContractAndSpender(
      accountAddress: String,
      contractAddress: String,
      spenderAddress: String): Option[TokenApprove]
  def deleteTokenApprove(tokenApproves: Seq[TokenApprove]): Unit

  def insertNFTApprove(nftApproves: Seq[NFTApprove]): Unit

  def findNFTApproveByAccountAndContract(
      accountAddress: String,
      contractAddress: String): Seq[NFTApprove]

  def deleteNFTApprove(nftApproves: Seq[NFTApprove]): Unit
}
