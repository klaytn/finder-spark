package io.klaytn.model.finder.es

import io.klaytn.model.RefinedTransactionReceipt
import io.klaytn.utils.JsonUtil
import JsonUtil.Implicits._

case class ESTransaction(block_hash: String,
                         block_number: Long,
                         transaction_hash: String,
                         transaction_index: Int,
                         status: Boolean,
                         type_int: Int,
                         chain_id: Option[String],
                         contract_address: Option[String],
                         fee_payer: Option[String],
                         from: String,
                         to: Option[String],
                         sender_tx_hash: String,
                         timestamp: Long) {
  def asJson: String = JsonUtil.asJson(this)
}

object ESTransaction {
  def apply(in: RefinedTransactionReceipt): ESTransaction = {
    new ESTransaction(
      block_hash = in.blockHash,
      block_number = in.blockNumber,
      transaction_hash = in.transactionHash,
      transaction_index = in.transactionIndex,
      status = in.status,
      type_int = in.typeInt,
      chain_id = in.chainId,
      contract_address = in.contractAddress,
      fee_payer = in.feePayer,
      from = in.from,
      to = in.to,
      sender_tx_hash = in.senderTxHash,
      timestamp = in.timestamp,
    )
  }
}
