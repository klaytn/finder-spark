package io.klaytn.model

import io.klaytn.utils.config.Constants
import io.klaytn.utils.klaytn.NumberConverter._

abstract class TransactionReceiptTypeManager(input: Option[String]) {
  def getMethod: Option[String] = {
    input match {
      case Some(input) =>
        if (input.length >= 10) {
          input.substring(0, 10) match {
            case "0x60806040" => Some("CREATE")
            case "0x6150864c" => Some("CREATE2")
            case _            => None
          }
        } else {
          None
        }
      case _ =>
        None
    }
  }

  def isCreate: Boolean = {
    getMethod match {
      case Some(method) =>
        method == "CREATE" || method == "CREATE2"
      case _ => false
    }
  }
}

/**
  * FYI: https://ko.docs.klaytn.com/dapp/json-rpc/api-references/klay/transaction
  * fee ratio: The percentage of the transaction fee payer's share.
  *            If this value is 30, 30% of the transaction fee is paid by the transaction fee payer. The remaining 70% is paid by the transaction sender.
  */
case class TransactionReceipt(accessList: Option[List[String]],
                              blockHash: String,
                              blockNumber: String,
                              chainId: Option[String],
                              contractAddress: Option[String],
                              from: String,
                              gas: String,
                              gasPrice: Option[String],
                              gasUsed: String,
                              input: Option[String],
                              logs: List[EventLog],
                              logsBloom: String,
                              maxFeePerGas: Option[String],
                              maxPriorityFeePerGas: Option[String],
                              nonce: String,
                              senderTxHash: String,
                              signatures: List[SignatureData],
                              status: String,
                              to: Option[String],
                              transactionHash: String,
                              transactionIndex: String,
                              `type`: String,
                              typeInt: Int,
                              value: Option[String],
                              codeFormat: Option[String],
                              feePayer: Option[String],
                              feePayerSignatures: Option[List[SignatureData]],
                              feeRatio: Option[String],
                              humanReadable: Option[Boolean],
                              key: Option[String],
                              txError: Option[String],
                              effectiveGasPrice: Option[String])
    extends TransactionReceiptTypeManager(input) {

  def booleanStatus(): Boolean = this.status == "0x1"

  def getTransferCount: (Int, Int) = {
    val transferCount = this.logs.flatMap { log =>
      if (log.isTokenTransferEvent) {
        Some((1, 0))
      } else if (log.isNFTTransferEvent) {
        Some((0, 1))
      } else {
        None
      }
    }

    (transferCount.map(_._1).sum, transferCount.map(_._2).sum)
  }

  def toRefined(bnp: String, timestamp: Int): RefinedTransactionReceipt = {
    val (tokenTransferCount, nftTransferCount) = getTransferCount

    val gasPrice = this.gasPrice match {
      case Some(gasPrice) => gasPrice.hexToBigInt().toString()
      case _              => Constants.DefaultGasPrice(this.blockNumber.hexToLong())
    }
    val txError = this.txError match {
      case Some(txError) => Some(txError.hexToInt())
      case _             => None
    }

    val value = this.value match {
      case Some(value) => Some(value.hexToBigInt().toString())
      case _           => None
    }

    val maxFeePerGas = this.maxFeePerGas match {
      case Some(maxFeePerGas) => Some(maxFeePerGas.hexToBigInt().toString())
      case _                  => None
    }
    val maxPriorityFeePerGas = this.maxPriorityFeePerGas match {
      case Some(maxPriorityFeePerGas) =>
        Some(maxPriorityFeePerGas.hexToBigInt().toString)
      case _ => None
    }
    val effectiveGasPrice = this.effectiveGasPrice match {
      case Some(effectiveGasPrice) =>
        Some(effectiveGasPrice.hexToBigInt().toString())
      case _ => None
    }

    RefinedTransactionReceipt(
      "transaction_receipts",
      bnp,
      this.accessList,
      this.blockHash,
      this.blockNumber.hexToLong(),
      this.contractAddress,
      this.chainId,
      this.from,
      this.gas.hexToLong(),
      gasPrice,
      this.gasUsed.hexToInt(),
      this.input,
      this.logsBloom,
      maxFeePerGas,
      maxPriorityFeePerGas,
      this.nonce.hexToLong(),
      this.senderTxHash,
      this.signatures,
      this.booleanStatus(),
      this.to,
      this.transactionHash,
      this.transactionIndex.hexToInt(),
      this.`type`,
      this.typeInt,
      value,
      this.codeFormat,
      this.feePayer,
      this.feePayerSignatures,
      this.feeRatio,
      this.humanReadable,
      this.key,
      txError,
      tokenTransferCount,
      nftTransferCount,
      timestamp,
      effectiveGasPrice
    )
  }
}

case class SignatureData(V: String, R: String, S: String)

case class RefinedTransactionReceipt(
    label: String,
    bnp: String,
    accessList: Option[List[String]],
    blockHash: String,
    blockNumber: Long,
    contractAddress: Option[String],
    chainId: Option[String],
    from: String,
    gas: Long,
    gasPrice: String,
    gasUsed: Int,
    input: Option[String],
    logsBloom: String,
    maxFeePerGas: Option[String],
    maxPriorityFeePerGas: Option[String],
    nonce: Long,
    senderTxHash: String,
    signatures: List[SignatureData],
    status: Boolean,
    to: Option[String],
    transactionHash: String,
    transactionIndex: Int,
    `type`: String,
    typeInt: Int,
    value: Option[String],
    codeFormat: Option[String],
    feePayer: Option[String],
    feePayerSignatures: Option[List[SignatureData]],
    feeRatio: Option[String],
    humanReadable: Option[Boolean],
    key: Option[String],
    txError: Option[Int],
    tokenTransferCount: Int,
    nftTransferCount: Int,
    timestamp: Int,
    effectiveGasPrice: Option[String])
    extends TransactionReceiptTypeManager(input)
