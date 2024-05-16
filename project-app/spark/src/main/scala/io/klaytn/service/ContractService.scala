package io.klaytn.service

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import io.klaytn._
import io.klaytn.client.SparkRedis
import io.klaytn.model.RefinedEventLog
import io.klaytn.model.finder.{AccountType, Contract, ContractType}
import io.klaytn.persistent._
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.UserConfig
import io.klaytn.utils.{SlackUtil, Utils}

import java.util.concurrent.TimeUnit
import scala.util.Try
import scala.util.control.Breaks.{break, breakable}

class ContractService(
    contractPersistentAPI: LazyEval[ContractPersistentAPI],
    accountPersistentAPI: LazyEval[AccountPersistentAPI],
    transactionPersistentAPI: LazyEval[TransactionPersistentAPI],
    holderPersistentAPI: LazyEval[HolderPersistentAPI],
    internalTransactionPersistentAPI: LazyEval[
      InternalTransactionPersistentAPI],
    caverContractService: LazyEval[CaverContractService],
    caverService: LazyEval[CaverService])
    extends Serializable {
  @transient private lazy val contractCache: Cache[String, Contract] = Caffeine
    .newBuilder()
    .expireAfterWrite(600, TimeUnit.SECONDS)
    .maximumSize(10000)
    .build()

  @transient private lazy val contractTypeCache
    : Cache[String, ContractType.Value] = Caffeine
    .newBuilder()
    .maximumSize(100000)
    .build()

  private val Ten = BigDecimal(10)

  def checkContractAndUpdate(contractAddress: String,
                             typeString: String): Unit = {
    var contractType = contractTypeCache.getIfPresent(contractAddress)
    if (contractType == null) {
      contractPersistentAPI.findContract(contractAddress) match {
        case Some(contract) =>
          contractTypeCache.put(contractAddress, contract.contractType)
          contractType = contract.contractType
        case _ =>
      }
    }

    try {
      val contract = typeString match {
        case "token" =>
          if (contractType == null || !(contractType == ContractType.ERC20 || contractType == ContractType.KIP7)) {
            Some(caverContractService.getToken(contractAddress))
          } else None
        case "nft" =>
          if (contractType == null || !(contractType == ContractType.ERC721 || contractType == ContractType.KIP17)) {
            Some(caverContractService.getNFT(contractAddress))
          } else None
        case "multiToken" =>
          if (contractType == null || !(contractType == ContractType.ERC1155 || contractType == ContractType.KIP37)) {
            Some(caverContractService.getMultiToken(contractAddress))
          } else None
        case _ => None
      }

      contract match {
        case Some(newContract) =>
          updateContract(newContract)
          contractTypeCache.put(contractAddress, newContract.contractType)
        case _ =>
      }
    } catch {
      case _: Throwable =>
    }
  }

  def getContractType(contractAddress: String): Option[ContractType.Value] = {
    val contractType = contractTypeCache.getIfPresent(contractAddress)
    if (contractType == null) {
      contractPersistentAPI.findContract(contractAddress) match {
        case Some(contract) =>
          contractTypeCache.put(contractAddress, contract.contractType)
          Some(contract.contractType)
        case _ => None
      }
    } else {
      Some(contractType)
    }
  }

  // TODO: Fix Method Name
  def getTokenUri(contractType: ContractType.Value,
                  contractAddress: String,
                  tokenId: BigInt): String = {
    holderPersistentAPI.getNFTUri(contractAddress, tokenId).getOrElse {
      val nftPatternedUriRedisKey = s"NFTPatternedUri:$contractAddress"
      val uri = SparkRedis.get(nftPatternedUriRedisKey).getOrElse {
        holderPersistentAPI.getNFTPatternedUri(contractAddress) match {
          case Some(uri) =>
            SparkRedis.setex(nftPatternedUriRedisKey, 3600, uri)
            uri
          case _ =>
            val uri = contractType match {
              case ContractType.KIP17 =>
                Try(caverContractService.getKIP17URI(contractAddress, tokenId))
                  .getOrElse("-")
              case ContractType.KIP37 =>
                Try(caverContractService.getKIP37URI(contractAddress, tokenId))
                  .getOrElse("-")
              case _ => "-"
            }
            if (uri.contains("{id}")) {
              holderPersistentAPI.insertNFTPatternedUri(contractAddress, uri)
              SparkRedis.setex(nftPatternedUriRedisKey, 3600, uri)
            }
            uri
        }
      }

      uri.replace("{id}", tokenId.toString())
    }
  }

  def getFreshTokenURI(contractType: ContractType.Value,
                       contractAddress: String,
                       tokenId: BigInt): String = {
    val uri = contractType match {
      case ContractType.KIP17 =>
        Try(caverContractService.getKIP17URI(contractAddress, tokenId))
          .getOrElse("-")
      case ContractType.KIP37 =>
        Try(caverContractService.getKIP37URI(contractAddress, tokenId))
          .getOrElse("-")
      case _ => "-"
    }
    if (uri.contains("{id}")) {
      holderPersistentAPI.insertNFTPatternedUri(contractAddress, uri)
    }
    uri.replace("{id}", tokenId.toString())

    if (uri != null && uri.length <= 255) {
      uri
    } else {
      "-"
    }
  }

  def applyDecimal(raw: BigDecimal, decimal: Int): BigDecimal = {
    (raw / Ten.pow(decimal)).setScale(decimal)
  }

  def checkContractAndUpdateNFT(address: String,
                                contract: Contract): Contract = {
    if (contract.contractType == ContractType.Custom) {
      val contract0 = caverContractService.getContract(address, "")
      if (contract0.contractType == ContractType.KIP17 ||
          contract0.contractType == ContractType.KIP37 ||
          contract0.contractType == ContractType.ERC721 ||
          contract0.contractType == ContractType.ERC1155) {
        register(contract0, System.currentTimeMillis(), 0, 0, true)
          .getOrElse(contract)
      } else {
        contract
      }
    } else {
      contract
    }
  }

  def getMaybeNFTContract(address: String): Contract = {
    val contract0 = contractCache.getIfPresent(address)
    if (contract0 != null) {
      return checkContractAndUpdateNFT(address, contract0)
    }

    val contract1 = contractPersistentAPI
      .findContract(address)
      .getOrElse(Contract(address, ContractType.Custom, None, None, None, None))

    val contract2 = checkContractAndUpdateNFT(address, contract1)
    contractCache.put(address, contract2)
    contract2
  }

  def procChangeTotalSupply(eventLogs: Seq[RefinedEventLog]): Unit = {
    eventLogs
      .filter { eventLog =>
        if (eventLog.isMintOrBurn) true
        else if (eventLog.isTransferEvent) {
          // In transfer
          // 1. If 'from' equals ZeroAddr it could be mint, 'to' equals ZeroAddr it could be burn
          // 2. If 'from' and 'to' are contract address, It could be change of liquidity of the lp
          val (address, from, to, _, _) = eventLog.extractTransferInfo()
          Utils.isBurnByTransfer(to) || Utils.isMintByTransfer(from) || to == address || from == address
        } else false
      }
      .map(_.address)
      .distinct
      .foreach { contractAddress =>
        contractPersistentAPI.findContract(contractAddress) match {
          case Some(contract) =>
            try {
              val (totalSupply, decimal) =
                caverContractService.getTotalSupplyAndDecimal(
                  contract.contractAddress,
                  contract.contractType)

              if (totalSupply.isDefined) {
                contractPersistentAPI.updateTotalSupply(
                  contract.contractAddress,
                  totalSupply.get,
                  decimal)
              }
            } catch {
              case _: Throwable =>
            }
          case _ =>
        }
      }
  }

  def procRegContractFromITX(dbName: String): Unit = {
    val redisKey = s"ContractService:RegContractFromITX:$dbName:LastID"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val itxs = internalTransactionPersistentAPI
      .getITXs(dbName, tableId, 1000)
      .sortBy(_.blockNumber)
    itxs
      .filter(
        x =>
          x.`type`.toLowerCase.startsWith("create") && x.to != null && x.to
            .startsWith("0x") && x.from.startsWith("0x"))
      .foreach { itx =>
        val (blockNumber, txIndex, contractCreator, contractAddress, txInput) =
          (itx.blockNumber, itx.transactionIndex, itx.from, itx.to, itx.input)

        var createTxHash: String = null
        var createdTimestamp = System.currentTimeMillis()
        var txFrom: String = null
        var txError = 0

        breakable {
          // transaction could be slower than internal tx, so retry
          1 to 100 foreach { idx =>
            transactionPersistentAPI.getTransactionHashAndTsAndTxErrorAndFrom(
              blockNumber,
              txIndex) match {
              case Some(tx) =>
                createTxHash = tx._1
                createdTimestamp = tx._2 * 1000L
                txError = tx._3
                txFrom = tx._4
                break
              case _ =>
                Thread.sleep(500)
            }
          }
        }

        val accountType = accountPersistentAPI.getAccountType(contractAddress)

        val contract =
          if (txError == 0)
            caverContractService.getContract(contractAddress, txInput)
          else Contract.empty(contractAddress)

        if (accountType == AccountType.Unknown) {

          accountPersistentAPI
            .insert(contractAddress,
                    AccountType.SCA,
                    0,
                    contract.contractType,
                    Some(contractCreator),
                    Some(createTxHash),
                    createdTimestamp,
                    Some(txFrom))
        } else {
          accountPersistentAPI
            .updateContractInfos(contractAddress,
                                 contract.contractType,
                                 contractCreator,
                                 createTxHash,
                                 createdTimestamp,
                                 txFrom)
        }

        accountPersistentAPI.updateTotalTXCount(1, contractCreator)
        contractPersistentAPI.insert(contract, 0, createdTimestamp, txError)
      }

    if (itxs.nonEmpty) {
      SparkRedis.set(redisKey, s"${itxs.map(_.id).max}")
      val s3Key =
        s"jobs/io.klaytn.apps.worker.SlowWorkerStreaming/lastId/$redisKey.${UserConfig.chainPhase.chain}"
      GCSUtil.writeText(UserConfig.baseBucket, s3Key, s"${itxs.map(_.id).max}")
    }
  }

  def register(contract: Contract,
               createdTimestamp: Long,
               txError: Int,
               transferCount: Int,
               updateAccount: Boolean): Option[Contract] = {
    if (updateAccount) {
      accountPersistentAPI.updateContractType(contract.contractAddress,
                                              contract.contractType)
    }
    val symbol =
      if (contract.symbol.isDefined && contract.symbol.get.length > 50) {
        val msg =
          s"""<modify to abbreviated contract symbol>
           |name: ${contract.name}
           |contractType: ${contract.contractType}
           |contractAddress: ${contract.contractAddress}
           |origin_symbol: ${contract.symbol.get}
           |decimal: ${contract.decimal}
           |totalSupply: ${contract.totalSupply}
           |""".stripMargin
        SlackUtil.sendMessage(msg)
        Some(contract.symbol.get.substring(0, 50))
      } else {
        contract.symbol
      }
    contractPersistentAPI.insert(contract.copy(symbol = symbol),
                                 transferCount,
                                 createdTimestamp,
                                 txError)
  }

  def updateContract(contract: Contract): Unit = {
    accountPersistentAPI.updateContractType(contract.contractAddress,
                                            contract.contractType)
    contractPersistentAPI.updateContract(contract)
  }

  def findContract(contractAddress: String): Option[Contract] =
    contractPersistentAPI.findContract(contractAddress)

  def updateBurnInfo(contractAddress: String,
                     amount0: BigInt,
                     count0: Int): Unit = {
    val (amount1, count1) =
      contractPersistentAPI.selectBurnInfo(contractAddress)

    val amount = amount0 + amount1.hexToBigInt()
    val count = count0 + count1

    contractPersistentAPI.updateBurnInfo(contractAddress,
                                         amount.to64BitsHex(),
                                         count)
  }

  def updateImplementationAddress(contractAddress: String,
                                  implementationAddress: String): Unit =
    contractPersistentAPI.updateImplementationAddress(contractAddress,
                                                      implementationAddress)

  def updateHolderCount(contractAddresses: Seq[String],
                        isToken: Boolean): Unit = {
    val redisKey = "UpdateHolderCountQueue"
    val nowScore = System.currentTimeMillis() / 1000

    // There could be multiple duplicate contracts, so it takes about 5 minutes to collect them.
    val maxScore = nowScore - 300
    val getCount = 100
    val queueData = SparkRedis.zrangebyscore(redisKey, 0, maxScore, 0, getCount)
    queueData.foreach { c =>
      val holderCount =
        if (isToken) holderPersistentAPI.getTokenHolderCount(c)
        else holderPersistentAPI.getNFTHolderCount(c)
      contractPersistentAPI.replaceHolderCount(Seq((c, holderCount)))
    }

    contractAddresses
      .filter(c => !queueData.contains(c))
      .foreach(c => SparkRedis.zaddNX(redisKey, nowScore, c))

    // If the fetched data is less than getcount, delete it as a range; if it is more than getcount, delete it one by one.
    if (queueData.size < getCount)
      SparkRedis.zremrangebyscore(redisKey, 0, maxScore)
    else SparkRedis.zrem(redisKey, queueData)
  }
}
