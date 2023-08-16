package io.klaytn.model

import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.{JsonUtil, Utils}

/**
  * FYI: https://ko.docs.klaytn.com/dapp/json-rpc/api-references/klay/block
  */
case class Block(blockNumber: Long, result: BlockContent) {
  val bc: BlockContent = this.result

  val blockNumberPartition: String = Utils.getBlockNumberPartition(blockNumber)

  val timestamp: Int = bc.timestamp.hexToInt()

  def toRefined
    : (RefinedBlock, List[RefinedTransactionReceipt], List[RefinedEventLog]) = {
    val blockScore = bc.blockscore match {
      case Some(blockScore) => Some(blockScore.hexToInt())
      case _                => None
    }
    val baseFeePerGas = bc.baseFeePerGas match {
      case Some(baseFeePerGas) => Some(baseFeePerGas.hexToBigInt().toString())
      case _                   => None
    }
    val refinedBlock =
      RefinedBlock(
        "blocks",
        blockNumberPartition,
        baseFeePerGas,
        blockScore,
        bc.committee,
        bc.extraData,
        bc.gasUsed.hexToInt(),
        bc.governanceData,
        bc.hash,
        bc.logsBloom,
        blockNumber,
        bc.parentHash,
        bc.proposer,
        bc.receiptsRoot,
        bc.reward,
        bc.size.hexToInt(),
        bc.stateRoot,
        timestamp,
        bc.timestampFoS.hexToInt(),
        bc.totalBlockScore.hexToInt(),
        bc.transactionsRoot,
        bc.voteData,
        bc.transactions.size
      )

    val refinedTransactions =
      bc.transactions.map(_.toRefined(blockNumberPartition, timestamp))
    val refinedLogs =
      bc.transactions.flatMap(
        tx =>
          tx.logs.map(
            _.toRefined(blockNumberPartition, tx.booleanStatus(), timestamp)))

    (refinedBlock, refinedTransactions, refinedLogs)
  }
}

case class BlockContent(baseFeePerGas: Option[String],
                        blockscore: Option[String],
                        committee: List[String],
                        extraData: String,
                        gasUsed: String,
                        governanceData: Option[String],
                        hash: String,
                        logsBloom: String,
                        number: String,
                        parentHash: String,
                        proposer: Option[String],
                        receiptsRoot: String,
                        reward: String,
                        size: String,
                        stateRoot: String,
                        timestamp: String,
                        timestampFoS: String,
                        totalBlockScore: String,
                        transactionsRoot: String,
                        voteData: String,
                        transactions: List[TransactionReceipt])

case class RefinedBlock(label: String,
                        bnp: String,
                        baseFeePerGas: Option[String],
                        blockScore: Option[Int],
                        committee: List[String],
                        extraData: String,
                        gasUsed: Int,
                        governanceData: Option[String],
                        hash: String,
                        logsBloom: String,
                        number: Long,
                        parentHash: String,
                        proposer: Option[String],
                        receiptsRoot: String,
                        reward: String,
                        size: Int,
                        stateRoot: String,
                        timestamp: Int,
                        timestampFoS: Int,
                        totalBlockScore: Int,
                        transactionsRoot: String,
                        voteData: String,
                        transactionCount: Int)

object Block {
  def parse(jsonBlock: String): Option[Block] = {
    JsonUtil.fromJson[Block](jsonBlock)
  }
}
