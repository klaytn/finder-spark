package io.klaytn.service

import com.klaytn.caver.abi.ABI
import io.klaytn._
import io.klaytn.model.finder.{
  ContractType,
  NFTApprove,
  NFTTransfer,
  TokenApprove
}
import io.klaytn.model.{EventLog, EventLogType, RefinedEventLog}
import io.klaytn.persistent.{
  ContractPersistentAPI,
  HolderPersistentAPI,
  TransferPersistentAPI
}
import io.klaytn.repository.{AccountTransferContracts, TokenHolders}
import io.klaytn.utils.config.{Constants, FunctionSupport}
import io.klaytn.utils.klaytn.NumberConverter.StringConverter
import io.klaytn.utils.klaytn.WithdrawDataReadUtil
import io.klaytn.utils.spark.UserConfig
import io.klaytn.utils.{SlackUtil, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TransferService(transferPersistentAPI: LazyEval[TransferPersistentAPI],
                      contractService: LazyEval[ContractService],
                      contractPersistentAPI: LazyEval[ContractPersistentAPI],
                      caverContractService: LazyEval[CaverContractService],
                      holderPersistentAPI: LazyEval[HolderPersistentAPI])
    extends Serializable {
  def setTokenContract(dbResult: Seq[((String, Int), Int)]): Unit = {
    dbResult.filter(_._2 == 0).foreach {
      case ((contractAddress, transferCount), _) =>
        val contract = caverContractService.getERC20(contractAddress)
        contractService.register(contract,
                                 System.currentTimeMillis(),
                                 0,
                                 transferCount,
                                 updateAccount = true)
    }
  }

  def setContract(dbResult: Seq[((String, Int), Int)]): Unit = {
    dbResult.filter(_._2 == 0).foreach {
      case ((contractAddress, transferCount), _) =>
        val contract = caverContractService.getContract(contractAddress, "")
        contractService.register(contract,
                                 System.currentTimeMillis(),
                                 0,
                                 transferCount,
                                 updateAccount = true)
    }
  }

  def getTransferCount(eventLogs: Seq[RefinedEventLog]): Seq[(String, Int)] = {
    eventLogs
      .map { eventLog =>
        val (address, _, _, _, _) = eventLog.extractTransferInfo()
        (address, 1)
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
      .toSeq
      .filter(_._2 > 0)
  }

  def toNFTTransfer(eventLogs: Seq[RefinedEventLog]): Seq[NFTTransfer] = {
    eventLogs.flatMap { eventLog =>
      val (address, from, to, tokenIds, amounts) =
        eventLog.extractTransferInfo()

      val contract = contractService.getMaybeNFTContract(address)
      val contractType =
        if (contract != null) contract.contractType else ContractType.Custom

      tokenIds.zipWithIndex.map {
        case (tokenId, idx) =>
          NFTTransfer(
            None,
            contractType,
            address,
            from,
            to,
            amounts(idx),
            tokenId,
            eventLog.timestamp,
            eventLog.blockNumber,
            eventLog.transactionHash,
            Utils.getDisplayOrder(eventLog.blockNumber,
                                  eventLog.transactionIndex,
                                  eventLog.logIndex)
          )
      }
    }
  }

  def insertAccountTransferContracts(
      accountTransferContracts0: Seq[AccountTransferContracts]): Unit = {
    if (accountTransferContracts0.nonEmpty) {
      val m = mutable.Map.empty[(String, String), AccountTransferContracts]
      accountTransferContracts0
        .filter(_.accountAddress != Constants.ZeroAddress)
        .foreach { data =>
          val key = (data.contractAddress, data.accountAddress)
          val d = m.getOrElseUpdate(key, data)
          if (d.timestamp < data.timestamp) m(key) = data
        }

      val accountTransferContracts = m.values.toSeq

      try {
        transferPersistentAPI.insertAccountTransferContracts(
          accountTransferContracts)
      } catch {
        case e: Throwable =>
          val rand = Random.nextInt(100000)
          val msg =
            s"""${e.getLocalizedMessage} ; $rand
               |filteredDataSize: ${accountTransferContracts.size}
               |msg: ${StringUtils.abbreviate(ExceptionUtils.getMessage(e), 500)}
               |trace: ${StringUtils.abbreviate(ExceptionUtils.getStackTrace(e),
                                                500)}
               |""".stripMargin
          SlackUtil.sendMessage(msg)

          Utils.retry(20, 10)(
            transferPersistentAPI.insertAccountTransferContracts(
              accountTransferContracts))

          SlackUtil.sendMessage(s"success retry: $rand")
      }
    }
  }

  def insertTokenTransferToMysql(eventLogs: Seq[RefinedEventLog]): Unit = {
    val tokenTransferEventLogs0 =
      eventLogs.filter(log => log.isTokenTransferEvent)

    // To prevent duplicates - check for duplicates by display order already entered because data is entered in blocks.
    val displayOrderInDB = transferPersistentAPI
      .findTokenTransferDisplayOrdersByBlockNumbers(
        tokenTransferEventLogs0.map(_.blockNumber))

    val tokenTransferEventLogs = tokenTransferEventLogs0.filter(log =>
      !displayOrderInDB.contains(Utils
        .getDisplayOrder(log.blockNumber, log.transactionIndex, log.logIndex)))

    val insertResult = ArrayBuffer.empty[Int]

    try {
      transferPersistentAPI.insertTokenTransferEvent(tokenTransferEventLogs,
                                                     insertResult)
    } catch {
      case e: Throwable =>
        val insertResult2 = ArrayBuffer.empty[Int]
        tokenTransferEventLogs.foreach { r =>
          transferPersistentAPI.insertTokenTransferEvent(List(r), insertResult2)
//          insertAccountTransferContracts(at)
          Thread.sleep(10)
        }

        SlackUtil.sendMessage(
          s"""exception: insert token transfer
                                 |${e.getLocalizedMessage}
                                 |blockNumber: ${eventLogs.head.blockNumber}
                                 |tokenTransferEventLogSize: ${eventLogs.size}
                                 |insertResult: 1 => ${insertResult.count(x =>
               x == 1)}, 0 => ${insertResult.count(x => x == 0)}
                                 |insertResult2: 1 => ${insertResult2.count(x =>
               x == 1)}, 0 => ${insertResult2.count(x => x == 0)}
                                 |stacktrace: ${StringUtils
               .abbreviate(ExceptionUtils.getMessage(e), 500)}
                                 |""".stripMargin)
    }

//    insertAccountTransferContracts(accountTransferContracts)

    val tokenTransferResult = contractPersistentAPI.updateTotalTransfer(
      getTransferCount(tokenTransferEventLogs))
    setTokenContract(tokenTransferResult)
  }

  def insertNFTTransferToMysql(eventLogs: Seq[RefinedEventLog]): Unit = {
    val nftTransferEventLogs0 = eventLogs.filter(log => log.isNFTTransferEvent)

    // To avoid duplicates - since data is entered in blocks, blocks that have already been entered will be duplicated.
    val displayOrdersInDB = transferPersistentAPI
      .findNFTTransferDisplayOrdersByBlockNumbers(
        nftTransferEventLogs0.map(_.blockNumber))

    val nftTransferEventLogs = nftTransferEventLogs0.filter(log =>
      !displayOrdersInDB.contains(Utils
        .getDisplayOrder(log.blockNumber, log.transactionIndex, log.logIndex)))

    val nftTransfers = toNFTTransfer(nftTransferEventLogs)

    try {
      transferPersistentAPI.insertNFTTransferEvent(nftTransfers)
    } catch {
      case e: Throwable =>
        val msg =
          s"""${e.getLocalizedMessage}
             |eventLogSize: ${eventLogs.size}
             |nftTransferEventLogSize: ${nftTransferEventLogs.size}
             |stacktrace: ${StringUtils.abbreviate(ExceptionUtils.getMessage(e),
                                                   500)}
             |""".stripMargin
        SlackUtil.sendMessage(msg)
        nftTransfers.foreach { r =>
          transferPersistentAPI.insertNFTTransferEvent(List(r))
//          insertAccountTransferContracts(at)
          Thread.sleep(10)
        }
    }

//    insertAccountTransferContracts(accountTransferContracts)

    val nftTransferResult = contractPersistentAPI.updateTotalTransfer(
      getTransferCount(nftTransferEventLogs))
    setContract(nftTransferResult)
  }

  def insertTransferToMysql(eventLogs: Seq[RefinedEventLog]): Unit = {
    insertTokenTransferToMysql(eventLogs)
    insertNFTTransferToMysql(eventLogs)
  }

  private def procTokenBurns(tokenBurns: Seq[RefinedEventLog]): Unit = {
    val displayOrderInDB = transferPersistentAPI
      .findTokenBurnDisplayOrdersByBlockNumbers(tokenBurns.map(_.blockNumber))

    val burnEvents = tokenBurns.filter(log =>
      !displayOrderInDB.contains(Utils
        .getDisplayOrder(log.blockNumber, log.transactionIndex, log.logIndex)))

    transferPersistentAPI.insertTokenBurnEvent(burnEvents)
  }

  private def procNFTBurns(nftBurns: Seq[RefinedEventLog]): Unit = {
    val displayOrderInDB = transferPersistentAPI
      .findNFTBurnDisplayOrdersByBlockNumbers(nftBurns.map(_.blockNumber))

    val burnEvents = nftBurns.filter(log =>
      !displayOrderInDB.contains(Utils
        .getDisplayOrder(log.blockNumber, log.transactionIndex, log.logIndex)))

    transferPersistentAPI.insertNFTBurnEvent(toNFTTransfer(burnEvents))
  }

  def procBurn(eventLogs: Seq[RefinedEventLog]): Unit = {
    if (!FunctionSupport.burn(UserConfig.chainPhase)) return

    val burnEvents0 = eventLogs.filter { eventLog =>
      if (eventLog.isTokenTransferEvent || eventLog.isNFTTransferEvent) {
        val (_, _, to, _, _) = eventLog.extractTransferInfo()
        Utils.isBurnByTransfer(to)
      } else false
    }

    procTokenBurns(burnEvents0.filter(_.isTokenTransferEvent))
    procNFTBurns(burnEvents0.filter(_.isNFTTransferEvent))
  }

  def procWithdrawOrDeposit(eventLogs: Seq[RefinedEventLog]): Unit = {
    eventLogs.filter(e => e.isWithdrawType1 || e.isDepositType1).foreach {
      log =>
        contractService.getContractType(log.address) match {
          case Some(typ) =>
            if (typ == ContractType.ERC20 || typ == ContractType.KIP7) {
              val holderAddress0 = log.getSignature match {
                case EventLog.Withdraw1 =>
                  WithdrawDataReadUtil
                    .getAddressWithAddressUint256Type(log.data, log.topics)
                case EventLog.Deposit1 =>
                  WithdrawDataReadUtil
                    .getAddressWithAddressUint256Type(log.data, log.topics)
                case _ => None
              }

              holderAddress0 match {
                case Some(holderAddress) =>
                  try {
                    caverContractService.getERC20MetadataReader
                      .balanceOf(log.address, holderAddress) match {
                      case Some(balance) =>
                        val data = Seq(
                          TokenHolders(log.address,
                                       holderAddress,
                                       balance,
                                       log.timestamp,
                                       log.blockNumber))
                        if (balance == BigInt(0))
                          holderPersistentAPI.deleteTokenHolders(data)
                        else holderPersistentAPI.updateTokenHolders(data)
                      case _ =>
                    }
                  } catch {
                    case _: Throwable =>
                  }
                case _ =>
              }
            }
          case _ =>
        }
    }
  }

  def procApprove(eventLogs: Seq[RefinedEventLog]): Unit = {
    if (!FunctionSupport.approve(UserConfig.chainPhase)) return
//    GCSUtil.writeText("klaytn-prod-spark", s"output/transfer/${eventLogs.map(_.blockNumber).min}", "")

    // Map[(owner, spender, contractType, contractAddress), (blockNumber, txHash, amount, timestamp)]
    val resultToken =
      mutable.Map.empty[(String, String, ContractType.Value, String),
                        (Long, String, BigInt, Int)]

    // Map[(owner, contractType, contractAddress, tokenId), (blockNumber, txHash, spender, timestamp)]
    val resultNFT =
      mutable.Map.empty[(String, ContractType.Value, String, String),
                        (Long, String, String, Int)]

    // Map[(owner, contractType, contractAddress, operator), (blockNumber, txHash, timestamp, isApprove)]
    val resultNFTAll =
      mutable.Map.empty[(String, ContractType.Value, String, String),
                        (Long, String, Int, Boolean)]

    eventLogs
      .filter(_.isApproval)
      .sortBy(log => (log.transactionIndex, log.logIndex))
      .foreach { log =>
        val contractType = contractService
          .getContractType(log.address)
          .getOrElse(ContractType.Unknown)
        log.getType match {
          case EventLogType.Approval =>
            log.topics.length match {
              case 1 =>
                val result = ABI.decodeParameters(
                  util.Arrays.asList("address", "address", "uint256"),
                  log.data)

                val owner = result.get(0).getValue.toString
                val spender = result.get(1).getValue.toString
                val amount = BigInt(result.get(2).getValue.toString)

                val key = (owner, spender, contractType, log.address)
                val newValue =
                  (log.blockNumber, log.transactionHash, amount, log.timestamp)
                if (resultToken
                      .getOrElseUpdate(key, newValue)
                      ._1 < log.blockNumber) {
                  resultToken(key) = newValue
                }
              // token(kip7,erc20) ; event Approval(address indexed owner, address indexed spender, uint256 value)
              case 3 =>
                val owner = log.topics(1).normalizeToAddress()
                val spender = log.topics(2).normalizeToAddress()
                val amount = log.data.hexToBigInt()

                val key = (owner, spender, contractType, log.address)
                val newValue =
                  (log.blockNumber, log.transactionHash, amount, log.timestamp)
                if (resultToken
                      .getOrElseUpdate(key, newValue)
                      ._1 < log.blockNumber) {
                  resultToken(key) = newValue
                }
              // kip17,erc721 ; event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)
              case 4 =>
                val owner = log.topics(1).normalizeToAddress()
                val spender = log.topics(2).normalizeToAddress()
                val tokenId = log.topics.last.hexToBigInt()

                val key = (owner, contractType, log.address, tokenId.toString)
                val newValue =
                  (log.blockNumber, log.transactionHash, spender, log.timestamp)
                if (resultNFT
                      .getOrElseUpdate(key, newValue)
                      ._1 < log.blockNumber) {
                  resultNFT(key) = newValue
                }
              case _ =>
            }
          // kip17,kip37,erc721,erc1155 ; event ApprovalForAll(address indexed owner, address indexed operator, bool approved)
          case EventLogType.ApprovalForAll =>
            val owner = log.topics(1).normalizeToAddress()
            val operator = log.topics(2).normalizeToAddress()
            val isApprove = log.data.hexToBigInt() == BigInt(1)

            val key = (owner, contractType, log.address, operator)
            val newValue =
              (log.blockNumber, log.transactionHash, log.timestamp, isApprove)
            if (resultNFTAll
                  .getOrElseUpdate(key, newValue)
                  ._1 < log.blockNumber) {
              resultNFTAll(key) = newValue
            }
          case _ =>
        }
      }

    val deleteTokenApproves = mutable.ArrayBuffer.empty[TokenApprove]
    val insertTokenApproves = mutable.ArrayBuffer.empty[TokenApprove]
    resultToken.foreach {
      case ((owner, spender, contractType, contractAddr),
            (blockNo, txHash, amount, timestamp)) =>
        val tokenApprove =
          TokenApprove(None,
                       blockNo,
                       txHash,
                       owner,
                       spender,
                       contractType,
                       contractAddr,
                       amount,
                       timestamp)
        deleteTokenApproves.append(tokenApprove)
        if (amount != Constants.BigIntZero)
          insertTokenApproves.append(tokenApprove)

    }
    // transferPersistentAPI.deleteTokenApprove(deleteTokenApproves)
    transferPersistentAPI.insertTokenApprove(insertTokenApproves)

    val deleteNFTApproves = mutable.ArrayBuffer.empty[NFTApprove]
    val insertNFTApproves = mutable.ArrayBuffer.empty[NFTApprove]
    resultNFT.foreach {
      case ((owner, contractType, contractAddr, tokenId),
            (blockNo, txHash, spender, ts)) =>
        val nftApprove =
          NFTApprove(None,
                     blockNo,
                     txHash,
                     owner,
                     spender,
                     contractType,
                     contractAddr,
                     Option(tokenId),
                     false,
                     ts)
        deleteNFTApproves.append(nftApprove)
        if (spender != Constants.ZeroAddress && spender != Constants.DeadAddress)
          insertNFTApproves.append(nftApprove)
    }

    resultNFTAll.foreach {
      case ((owner, contractType, contractAddr, operator),
            (blockNo, txHash, ts, isApprove)) =>
        val nftApprove =
          NFTApprove(None,
                     blockNo,
                     txHash,
                     owner,
                     operator,
                     contractType,
                     contractAddr,
                     None,
                     isApprove,
                     ts)
        deleteNFTApproves.append(nftApprove)
        if (isApprove) insertNFTApproves.append(nftApprove)
    }

    transferPersistentAPI.deleteNFTApprove(deleteNFTApproves)
    transferPersistentAPI.insertNFTApprove(insertNFTApproves)
  }

  def procUpdateTokenUri(eventLogs: Seq[RefinedEventLog]): Unit = {
    eventLogs.filter(e => e.isURI || e.isSetTokenURI).foreach { log =>
      contractService.getContractType(log.address) match {
        case Some(typ) =>
          val tokenId = log.topics(1).hexToBigInt()

          val uri =
            if (typ == ContractType.ERC721 || typ == ContractType.KIP17) {
              caverContractService.getKIP17MetadataReader
                .tokenURI(log.address, tokenId.bigInteger)
                .getOrElse("-")
            } else if (typ == ContractType.ERC721 || typ == ContractType.KIP17) {
              caverContractService.getKIP37MetadataReader
                .uri(log.address, tokenId.bigInteger)
                .getOrElse("-")
            } else "-"

          holderPersistentAPI.updateNFTUri(log.address, tokenId, uri)
        case _ =>
      }
    }
  }
}
