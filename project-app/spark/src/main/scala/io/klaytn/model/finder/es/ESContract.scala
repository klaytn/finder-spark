package io.klaytn.model.finder.es

import io.klaytn.utils.JsonUtil
import JsonUtil.Implicits._

case class ESContract(
    contract_address: String,
    contract_type: Int,
    name: String,
    symbol: String,
    verified: Option[Boolean],
    created_at: Long,
    updated_at: Long,
    total_supply_order: String,
    total_transfer: Long,
) {
  def asJson: String = JsonUtil.asJson(this)
}
