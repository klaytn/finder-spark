package io.klaytn.service

import com.klaytn.caver.Caver
import com.typesafe.config.{Config, ConfigFactory}
import io.klaytn._
import io.klaytn.contract.lib.KIP17MetadataReader
import io.klaytn.model._
import io.klaytn.model.finder.{Contract, ContractType}
import io.klaytn.utils.JsonUtil
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.http.HttpClient
import org.apache.http.HttpHeaders

import scala.collection.JavaConverters._
import scala.util.Try

case class InternalTransactionContentResp(jsonrpc: String,
                                          id: Int,
                                          result: InternalTransactionContent)
case class TransactionAccessList(address: String, storageKeys: List[String])

object CaverService {
  def of(): CaverService = of(ConfigFactory.load())

  def of(config: Config): CaverService = {
    of(config.getString("spark.app.caver.url"))
  }

  def of(caverUrl: String): CaverService = {
    val caver = new Caver(caverUrl)
    of(caverUrl,
       caver,
       HttpClient.createPooled(connectTimeout = 1000 * 1000,
                               socketTimeout = 1000 * 1000))
  }

  def of(caverUrl: String,
         caver: LazyEval[Caver],
         httpClient: LazyEval[HttpClient]): CaverService = {
    new CaverService(caverUrl, caver, httpClient)
  }
}

class CaverService(caverUrl: String,
                   caver: LazyEval[Caver],
                   httpClient: LazyEval[HttpClient])
    extends Serializable {
  def getCaver: Caver = caver

  def getTransactionCount(address: String): BigInt =
    Try(BigInt(caver.rpc.klay.getTransactionCount(address).send().getValue))
      .getOrElse(BigInt(0))

  def getBalance(address: String, blockNumber: Long): BigInt = {
    BigInt(caver.rpc.klay.getBalance(address).send().getValue)
  }

  def getNFT(contractAddress: String): Option[Contract] = {
    try {
      val kip17 = new KIP17MetadataReader(caver)
      val info = kip17.read(contractAddress)
      Some(
        Contract(contractAddress,
                 ContractType.KIP17,
                 info.name,
                 info.symbol,
                 None,
                 info.totalSupply))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }
  }

  def getTransactionReceipts(blockHash: String): List[TransactionReceipt] = {
    caver.rpc.klay
      .getBlockReceipts(blockHash)
      .send()
      .getResult
      .asScala
      .map { transactionReceipt =>
        val signatures = transactionReceipt.getSignatures.asScala.toList
          .map(s => SignatureData(s.getV, s.getR, s.getS))

        val feePayerSignatures =
          if (transactionReceipt.getFeePayerSignatures == null || transactionReceipt.getFeePayerSignatures.isEmpty) {
            None
          } else {
            Some(
              transactionReceipt.getFeePayerSignatures.asScala.toList.map(s =>
                SignatureData(s.getV, s.getR, s.getS)))
          }

        val eventLogs = transactionReceipt.getLogs.asScala.toList.map { l =>
          EventLog(
            l.getBlockHash,
            l.getBlockNumberRaw,
            l.getTransactionHash,
            l.getTransactionIndexRaw,
            l.getAddress,
            l.getTopics.asScala.toList,
            l.getData,
            l.getLogIndexRaw,
            Some(l.isRemoved)
          )
        }

        val accessList = if (transactionReceipt.getAccessList == null) {
          None
        } else {
          Some(transactionReceipt.getAccessList.asScala.map { x =>
            JsonUtil.asJson(
              TransactionAccessList(x.getAddress,
                                    x.getStorageKeys.asScala.toList))
          }.toList)
        }

        TransactionReceipt(
          accessList,
          transactionReceipt.getBlockHash,
          transactionReceipt.getBlockNumber,
          Option(transactionReceipt.getContractAddress),
          Option(transactionReceipt.getChainID),
          transactionReceipt.getFrom,
          transactionReceipt.getGas,
          Option(transactionReceipt.getGasPrice),
          transactionReceipt.getGasUsed,
          Option(transactionReceipt.getInput),
          eventLogs,
          transactionReceipt.getLogsBloom,
          Option(transactionReceipt.getMaxFeePerGas),
          Option(transactionReceipt.getMaxPriorityFeePerGas),
          transactionReceipt.getNonce,
          transactionReceipt.getSenderTxHash,
          signatures,
          transactionReceipt.getStatus,
          Option(transactionReceipt.getTo),
          transactionReceipt.getTransactionHash,
          transactionReceipt.getTransactionIndex,
          transactionReceipt.getType,
          transactionReceipt.getTypeInt.toInt,
          Option(transactionReceipt.getValue),
          Option(transactionReceipt.getCodeFormat),
          Option(transactionReceipt.getFeePayer),
          feePayerSignatures,
          Option(transactionReceipt.getFeeRatio),
          Option(transactionReceipt.isHumanReadable),
          Option(transactionReceipt.getKey),
          Option(transactionReceipt.getTxError),
          Option(transactionReceipt.getEffectiveGasPrice)
        )
      }
      .toList
  }

  def getBlockContent(blockNumber: Long): BlockContent = {
    val blockByNumber =
      caver.rpc.klay.getBlockByNumber(blockNumber).send().getResult
    val blockWithConsensusInfoByNumber =
      caver.rpc.klay
        .getBlockWithConsensusInfoByNumber(blockNumber)
        .send()
        .getResult
    val transactionReceipts = getTransactionReceipts(blockByNumber.getHash)

    BlockContent(
      Option(blockByNumber.getBaseFeePerGas),
      Option(blockByNumber.getBlockScore),
      Try { blockWithConsensusInfoByNumber.getCommittee.asScala.toList }
        .getOrElse(List.empty),
      blockByNumber.getExtraData,
      blockByNumber.getGasUsed,
      Option(blockByNumber.getGovernanceData),
      blockByNumber.getHash,
      blockByNumber.getLogsBloom,
      blockByNumber.getNumber,
      blockByNumber.getParentHash,
      Option(blockWithConsensusInfoByNumber.getProposer),
      blockByNumber.getReceiptsRoot,
      blockByNumber.getReward,
      blockByNumber.getSize,
      blockByNumber.getStateRoot,
      blockByNumber.getTimestamp,
      blockByNumber.getTimestampFoS,
      blockByNumber.getTotalBlockScore,
      blockByNumber.getTransactionsRoot,
      blockByNumber.getVoteData,
      transactionReceipts
    )
  }

  def getBlock(blockNumber: Long): Block =
    Block(blockNumber, getBlockContent(blockNumber))

  def getInternalTransaction(
      txHash: String): Option[InternalTransactionContent] = {
    val requestBody =
      s"""{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["$txHash", {"tracer": "callTracer"}],"id":1}"""
    val response = httpClient.post(
      caverUrl,
      requestBody,
      Map(HttpHeaders.CONTENT_TYPE -> "application/json"))
    if (!response.is2xx) {
      val msg =
        s"""status: ${response.status}
           |body: ${response.body}
           |""".stripMargin
      throw new IllegalStateException(msg)
    }

    JsonUtil.fromJson[InternalTransactionContentResp](
      response.body
        .replace(""""result":{"type":0""", """"result":{"type":"0"""")
        .replace(""","time":0}""", ""","time":"0"}""")
    ) match {
      case Some(r) => Some(r.result)
      case _       => None
    }
  }
}
