package io.klaytn.apps.restore.es

case class ContractDTO(
    contract_address: String,
    contract_type: Int,
    name: String,
    symbol: String,
    verified: Option[Boolean],
    created_at: Long,
    updated_at: Long,
    total_supply_order: String,
    total_transfer: Long,
)
