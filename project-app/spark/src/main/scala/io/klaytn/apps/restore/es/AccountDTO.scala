package io.klaytn.apps.restore.es

private[es] case class AccountDTO(
    address: String,
    accountType: Int,
    balance: Option[BigDecimal],
    contract_type: Int,
    contract_creator_address: Option[String],
    contract_creator_tx_hash: Option[String],
    kns_domain: Option[String],
    address_label: Option[String],
    tags: Option[String],
    updated_at: Long,
)
