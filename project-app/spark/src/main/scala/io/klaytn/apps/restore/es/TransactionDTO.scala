package io.klaytn.apps.restore.es

private[es] case class TransactionDTO(
    block_hash: String,
    block_number: Long,
    transaction_hash: String,
    status: Boolean,
    type_int: Int,
    chain_id: Option[String],
    contract_address: Option[String],
    fee_payer: Option[String],
    from: String,
    to: Option[String],
    sender_tx_hash: String,
    timestamp: Long,
    transaction_index: Int
)
