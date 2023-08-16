package io.klaytn.persistent.impl.rdb

import io.klaytn.model.RefinedEventLog
import io.klaytn.model.finder._
import io.klaytn.persistent.TransferPersistentAPI
import io.klaytn.repository.{AccountTransferContracts, TransferRepository}
import io.klaytn.utils.{SlackUtil, Utils}

import scala.collection.mutable.ArrayBuffer

class RDBTransferPersistentAPI
    extends TransferRepository
    with TransferPersistentAPI {
  override def insertAccountTransferContracts(
      accountTransferContracts: Seq[AccountTransferContracts]): Unit = {
    super.insertAccountTransferContracts(accountTransferContracts)
  }

  override def insertTokenTransferEvent(
      eventLogs: Seq[RefinedEventLog],
      insertResult: ArrayBuffer[Int]): Unit = {
    super.insertTokenTransferEvent(eventLogs, insertResult)
  }

  override def insertNFTTransferEvent(transfers: Seq[NFTTransfer]): Unit = {
    super.insertNFTTransferEvent(transfers)
  }

  override def getNFTTransferMinBlockNumber(): Long = {
    super.getNFTTransferMinBlockNumber()
  }

  private def _getNFTTransfers(tableId: Long, cnt: Int)(
      fn: => Seq[NFTTransfer]): Seq[NFTTransfer] = {
    try {
      Utils.retry(10, 100) {
        val result = fn
        if (result.nonEmpty) {
          val minId = result.map(_.id.getOrElse(0L)).min
          val maxId = result.map(_.id.getOrElse(0L)).max
          val expectedCount = maxId - minId + 1

          if (minId != tableId + 1) {
            throw new RuntimeException(
              s"nft holder] id issue: expected start id: ${tableId + 1}, but $minId")
          }
          if (expectedCount != result.length) {
            throw new RuntimeException(
              s"nft holder] count issue: expected count: $expectedCount, but ${result.length}, input: $cnt, tableId: $tableId")
          }
        }
        result
      }
    } catch {
      case e: Throwable =>
        SlackUtil.sendMessage(e.getMessage)
        super.getNFTTransfers(tableId, cnt)
    }
  }

  override def getTokenTransferMinBlockNumber(): Long = {
    super.getTokenTransferMinBlockNumber()
  }

  private def _getTokenTransfers(tableId: Long, cnt: Int)(
      fn: => Seq[TokenTransfer]): Seq[TokenTransfer] = {
    try {
      Utils.retry(10, 100) {
        val result = fn
        if (result.nonEmpty) {
          val minId = result.map(_.id).min
          val maxId = result.map(_.id).max
          val expectedCount = maxId - minId + 1

          if (minId != tableId + 1) {
            throw new RuntimeException(
              s"id issue: expected start id: ${tableId + 1}, but $minId")
          }
          if (expectedCount != result.length) {
            throw new RuntimeException(
              s"count issue: expected count: $expectedCount, but ${result.length}, input: $cnt, tableId: $tableId")
          }
        }
        result
      }
    } catch {
      case e: Throwable =>
        SlackUtil.sendMessage(e.getMessage)
        fn
    }
  }

  override def getTokenTransfers(tableId: Long,
                                 cnt: Int): Seq[TokenTransfer] = {
    _getTokenTransfers(tableId, cnt)(super.getTokenTransfers(tableId, cnt))
  }

  override def insertTokenBurnEvent(eventLogs: Seq[RefinedEventLog]): Unit = {
    super.insertTokenBurnEvent(eventLogs)
  }

  override def getTokenBurns(tableId: Long, cnt: Int): Seq[TokenTransfer] = {
    _getTokenTransfers(tableId, cnt)(super.getTokenBurnTransfers(tableId, cnt))
  }

  override def findTokenTransfersByBlockNumbers(
      blockNumbers: Seq[Long]): Seq[TokenTransfer] = {
    super.findTokenTransfersByBlockNumbers(blockNumbers)
  }

  override def findNFTTransfersByBlockNumbers(
      blockNumbers: Seq[Long]): Seq[NFTTransfer] = {
    super.findNFTTransfersByBlockNumbers(blockNumbers)
  }

  override def findTokenTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    super.findTokenTransferDisplayOrdersByBlockNumbers(blockNumbers)
  }

  override def findNFTTransferDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    super.findNFTTransferDisplayOrdersByBlockNumbers(blockNumbers)
  }

  override def findTokenBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    super.findTokenBurnDisplayOrdersByBlockNumbers(blockNumbers)
  }

  override def findNFTBurnDisplayOrdersByBlockNumbers(
      blockNumbers: Seq[Long]): Set[String] = {
    super.findNFTBurnDisplayOrdersByBlockNumbers(blockNumbers: Seq[Long])
  }

  override def insertNFTBurnEvent(transfers: Seq[NFTTransfer]): Unit = {
    super.insertNFTBurnEvent(transfers)
  }

  override def getNFTTransfers(tableId: Long, cnt: Int): Seq[NFTTransfer] = {
    _getNFTTransfers(tableId, cnt)(super.getNFTTransfers(tableId, cnt))
  }

  def getNFTTransfersWithoutRetry(tableId: Long, cnt: Int): Seq[NFTTransfer] = {
    super.getNFTTransfers(tableId, cnt)
  }

  override def getNFTBurns(tableId: Long, cnt: Int): Seq[NFTTransfer] = {
    _getNFTTransfers(tableId, cnt)(super.getNFTBurnTransfers(tableId, cnt))
  }

  override def insertTokenApprove(tokenApproves: Seq[TokenApprove]): Unit = {
    super.insertTokenApprove(tokenApproves)
  }

  override def findTokenApproveByAccountAndContractAndSpender(
      accountAddress: String,
      contractAddress: String,
      spenderAddress: String): Option[TokenApprove] = {
    super.findTokenApproveByAccountAndContractAndSpender(accountAddress,
                                                         contractAddress,
                                                         spenderAddress)
  }

  override def deleteTokenApprove(tokenApproves: Seq[TokenApprove]): Unit = {
    super.deleteTokenApprove(tokenApproves)
  }

  override def insertNFTApprove(nftApproves: Seq[NFTApprove]): Unit = {
    super.insertNFTApprove(nftApproves)
  }

  override def findNFTApproveByAccountAndContract(
      accountAddress: String,
      contractAddress: String): Seq[NFTApprove] = {
    super.findNFTApproveByAccountAndContract(accountAddress, contractAddress)
  }

  override def deleteNFTApprove(nftApproves: Seq[NFTApprove]): Unit = {
    super.deleteNFTApprove(nftApproves)
  }
}
